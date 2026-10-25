using System.Text.Json.Serialization;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The position of one job inside its plan, carried in every message of that job.
/// </summary>
/// <remarks>
/// The context travels inside the message and is the only state the workflow has, so it is
/// validated where it is set — in the accessors, which a <c>with</c> expression goes through as the
/// constructor does — rather than where it is read: a context that exists cannot throw from
/// <see cref="CurrentStep"/> on an index outside its plan. Equality is structural over the steps,
/// not over the list reference, so two contexts deserialized from the same message compare equal.
/// <para>
/// The context holds where a job is, never what it carries. It used to hold the query of the search the library
/// was taken out of, as an untyped string every job had to have; what a later step needs now reaches it through
/// the payloads of the steps before it, typed and checked as the plan declares them.
/// </para>
/// </remarks>
public sealed record WorkflowContext<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// Creates a context positioned before the first step, or at an explicit index into the plan.
    /// </summary>
    /// <exception cref="ArgumentException">The plan is empty, or the start is not in UTC.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The index falls outside the plan.</exception>
    public WorkflowContext(
        Guid jobId,
        IReadOnlyList<TStep> steps,
        int currentStepIndex = -1,
        DateTime startedAt = default)
    {
        JobId = jobId;
        Steps = steps;
        CurrentStepIndex = currentStepIndex;
        StartedAt = startedAt;
    }

    /// <summary>
    /// Identifies the job across every step and every process handling it.
    /// </summary>
    public Guid JobId { get; init; }

    /// <summary>
    /// When the job was started, in UTC; the default value when the message predates it.
    /// </summary>
    /// <exception cref="ArgumentException">The time is not in UTC.</exception>
    /// <remarks>
    /// The deadline is measured from here on every worker. It used to start when each worker first saw the job, so a
    /// job that moved from worker to worker, or outlived a restart, got a fresh lifetime at every move and none bound
    /// it. A <see cref="DateTime"/> rather than a <see cref="DateTimeOffset"/>: the context is copied on every step,
    /// and the eight bytes of a <see cref="DateTime"/> took a step from 144 to 152 B, the baseline (measured
    /// 2026-10-23); the sixteen of an offset would take it past the 5 % the benchmarks allow.
    /// </remarks>
    public DateTime StartedAt
    {
        get;
        init => field = value == default || value.Kind == DateTimeKind.Utc
            ? value
            : throw new ArgumentException("A job's start is kept in UTC, the one clock every worker reads alike.", nameof(StartedAt));
    }

    /// <summary>
    /// The ordered plan the job follows.
    /// </summary>
    /// <exception cref="ArgumentException">The plan is empty, or shorter than the position the job is at.</exception>
    public IReadOnlyList<TStep> Steps
    {
        get;
        init
        {
            ArgumentNullException.ThrowIfNull(value, nameof(Steps));

            if (value.Count == 0)
                throw new ArgumentException("A workflow plan needs at least one step.", nameof(Steps));

            if (CurrentStepIndex >= value.Count)
                throw new ArgumentException(
                    $"The plan has {value.Count} step(s), fewer than the position {CurrentStepIndex} the job is at.",
                    nameof(Steps));

            field = [.. value];
        }
    }

    /// <summary>
    /// Where the job stands in its plan; -1 before the first step.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The index falls outside the plan.</exception>
    public int CurrentStepIndex
    {
        get;
        init => field = value >= -1 && value < Steps.Count
            ? value
            : throw new ArgumentOutOfRangeException(
                nameof(CurrentStepIndex),
                value,
                $"The plan has {Steps.Count} step(s), so the index must be between -1 and {Steps.Count - 1}.");
    }

    /// <summary>
    /// The step being executed right now.
    /// </summary>
    /// <exception cref="InvalidOperationException">The job has not started yet.</exception>
    /// <remarks>
    /// Read off the position, so never written into a message: a writer reads every public property, and this one
    /// throws for a job not yet on a step — the context a job's deadline carries — so no deadline was ever published.
    /// </remarks>
    [JsonIgnore]
    public TStep CurrentStep => CurrentStepIndex >= 0
        ? Steps[CurrentStepIndex]
        : throw new InvalidOperationException(
            $"Job {JobId} has not started: there is no current step before the plan is entered.");

    /// <summary>
    /// The step executed before the current one, or null at the start of the plan.
    /// </summary>
    [JsonIgnore]
    public TStep? PreviousStep => CurrentStepIndex > 0 ? Steps[CurrentStepIndex - 1] : null;

    /// <summary>
    /// Whether the current step is the last one of the plan.
    /// </summary>
    [JsonIgnore]
    public bool IsFinished => CurrentStepIndex + 1 >= Steps.Count;

    /// <summary>
    /// The same job advanced to the next step.
    /// </summary>
    /// <exception cref="InvalidOperationException">The plan is already on its last step.</exception>
    public WorkflowContext<TStep> MoveNext() => IsFinished
        ? throw new InvalidOperationException(
            $"Job {JobId} is on the last step of its plan and cannot advance further.")
        : this with { CurrentStepIndex = CurrentStepIndex + 1 };

    /// <inheritdoc/>
    public bool Equals(WorkflowContext<TStep>? other) =>
        other is not null
        && JobId == other.JobId
        && CurrentStepIndex == other.CurrentStepIndex
        && StartedAt == other.StartedAt
        && Steps.SequenceEqual(other.Steps);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(JobId);
        hash.Add(CurrentStepIndex);
        hash.Add(StartedAt);

        foreach (TStep step in Steps)
        {
            hash.Add(step);
        }

        return hash.ToHashCode();
    }
}

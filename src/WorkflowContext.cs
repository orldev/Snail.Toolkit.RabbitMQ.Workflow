namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The position of one job inside its plan, carried in every message of that job.
/// </summary>
/// <remarks>
/// The context travels inside the message and is the only state the workflow has, so it is
/// validated on construction rather than where it is read: a context that survives the
/// constructor cannot throw from <see cref="CurrentStep"/> afterwards. Equality is structural
/// over the steps, not over the list reference, so two contexts deserialized from the same
/// message compare equal.
/// </remarks>
public sealed record WorkflowContext<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// Creates a context positioned before the first step, or at an explicit index into the plan.
    /// </summary>
    /// <exception cref="ArgumentException">The plan is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The index falls outside the plan.</exception>
    public WorkflowContext(Guid jobId, string originalQuery, IReadOnlyList<TStep> steps, int currentStepIndex = -1)
    {
        ArgumentNullException.ThrowIfNull(originalQuery);
        ArgumentNullException.ThrowIfNull(steps);

        if (steps.Count == 0)
            throw new ArgumentException("A workflow plan needs at least one step.", nameof(steps));

        if (currentStepIndex < -1 || currentStepIndex >= steps.Count)
            throw new ArgumentOutOfRangeException(nameof(currentStepIndex),
                currentStepIndex,
                $"The plan has {steps.Count} step(s), so the index must be between -1 and {steps.Count - 1}.");

        JobId = jobId;
        OriginalQuery = originalQuery;
        Steps = [.. steps];
        CurrentStepIndex = currentStepIndex;
    }

    /// <summary>
    /// Identifies the job across every step and every process handling it.
    /// </summary>
    public Guid JobId { get; init; }

    /// <summary>
    /// The request the job was started for.
    /// </summary>
    public string OriginalQuery { get; init; }

    /// <summary>
    /// The ordered plan the job follows.
    /// </summary>
    public IReadOnlyList<TStep> Steps { get; init; }

    /// <summary>
    /// Where the job stands in its plan; -1 before the first step.
    /// </summary>
    public int CurrentStepIndex { get; init; }

    /// <summary>
    /// The step being executed right now.
    /// </summary>
    /// <exception cref="InvalidOperationException">The job has not started yet.</exception>
    public TStep CurrentStep => CurrentStepIndex >= 0
        ? Steps[CurrentStepIndex]
        : throw new InvalidOperationException(
            $"Job {JobId} has not started: there is no current step before the plan is entered.");

    /// <summary>
    /// The step executed before the current one, or null at the start of the plan.
    /// </summary>
    public TStep? PreviousStep => CurrentStepIndex > 0 ? Steps[CurrentStepIndex - 1] : null;

    /// <summary>
    /// Whether the current step is the last one of the plan.
    /// </summary>
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
        && string.Equals(OriginalQuery, other.OriginalQuery, StringComparison.Ordinal)
        && Steps.SequenceEqual(other.Steps);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(JobId);
        hash.Add(CurrentStepIndex);
        hash.Add(OriginalQuery, StringComparer.Ordinal);

        foreach (var step in Steps)
        {
            hash.Add(step);
        }

        return hash.ToHashCode();
    }
}

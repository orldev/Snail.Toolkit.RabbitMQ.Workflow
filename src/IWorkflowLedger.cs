namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Remembers, for a while, the few facts about each job its workers must agree on: which step it handed on — its start
/// being the first — and that it ended.
/// </summary>
/// <remarks>
/// Not a journal. Nothing here holds a job's payload or its history, and nothing resumes a job from it: each mark is
/// a yes-or-no one worker sets once and the others read, kept for as long as asked and then forgotten. It exists
/// because the broker delivers at least once — a step redelivered after a crash, or a message published twice when a
/// connection dropped, used to run its step twice and publish the next one twice, and the job forked into two that
/// both ran to the end.
/// <para>
/// Every mark is set atomically: two workers asking at once get one true and one false. The ledger used when the
/// application registers none keeps its marks in this process, and catches what lands twice on one worker. A fleet
/// registers one every worker reads — a Redis instance, say, with one key set only if absent per mark — before
/// <c>AddWorkflow</c>.
/// </para>
/// </remarks>
public interface IWorkflowLedger
{
    /// <summary>
    /// Whether every worker of the fleet reads the same marks.
    /// </summary>
    /// <remarks>
    /// A job's deadline needs it: only a ledger the whole fleet reads can tell a job that ended on another worker from
    /// one still running.
    /// </remarks>
    bool IsShared { get; }

    /// <summary>
    /// Marks the result of a job's step handed on to the step after it; step −1 is the job's start, handed to its
    /// first step.
    /// </summary>
    /// <returns>False when that step was handed on already, or the job has ended.</returns>
    /// <remarks>
    /// The start is a hand-over like any other so that a start whose first message could not be published is taken
    /// back the same way, and the caller's retry under the same id is not refused as a second job.
    /// </remarks>
    ValueTask<bool> TryHandOverAsync(Guid jobId, int stepIndex, TimeSpan keepFor, CancellationToken cancellationToken = default);

    /// <summary>
    /// Takes back a hand-over whose message could not be published, so the step's next attempt can hand on.
    /// </summary>
    ValueTask ReleaseHandOverAsync(Guid jobId, int stepIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a job ended — finished, failed or cancelled.
    /// </summary>
    /// <returns>False when it had ended already.</returns>
    ValueTask<bool> TryEndAsync(Guid jobId, TimeSpan keepFor, CancellationToken cancellationToken = default);
}

using Snail.Toolkit.RabbitMQ.Consumers;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The message that comes back when a job's lifetime has passed, to end the job if nothing ended it first.
/// </summary>
/// <param name="Context">The job as it was started.</param>
/// <remarks>
/// A deadline that passed between two steps used to say nothing: the job was reported cancelled only when its next
/// message arrived, and never if none did. This message is published as the job starts, waits out the job's lifetime
/// in the broker, and then reaches one worker, which ends the job unless the ledger says it ended already.
/// </remarks>
public sealed record WorkflowDeadline<TStep>(WorkflowContext<TStep> Context) where TStep : struct, Enum;

/// <summary>
/// Marks a workflow whose jobs are given a deadline message as they start.
/// </summary>
internal sealed class WorkflowDeadlines<TStep> where TStep : struct, Enum;

/// <summary>
/// Ends a job whose deadline message came back before the job ended.
/// </summary>
/// <remarks>
/// The ledger decides: a job that finished or failed is marked ended, and its deadline tells nobody. One still running
/// is ended here, and its later steps find it ended and stop. The job is cancelled on this worker too, in case a step
/// of it runs here now.
/// </remarks>
internal sealed class WorkflowDeadlineConsumer<TStep>(IWorkflowDispatcher<TStep> dispatcher, IJobCancellation cancellation)
    : IConsumer<WorkflowDeadline<TStep>>
    where TStep : struct, Enum
{
    private static readonly WorkflowFailure OutOfTime = new("The job outlived its deadline.");

    public async ValueTask HandleAsync(WorkflowDeadline<TStep> message, CancellationToken cancellationToken = default)
    {
        cancellation.TryCancel(message.Context.JobId);

        await dispatcher.FailAsync(message.Context, OutOfTime, cancellationToken);
    }
}

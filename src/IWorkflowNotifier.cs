namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Tells the world outside the broker how a job is getting on.
/// </summary>
/// <remarks>
/// Progress usually has to reach a waiting caller over some other transport — a Redis channel
/// behind server-sent events, a websocket, a log. That transport is the application's business,
/// not the workflow's, so the workflow only says what happened and when.
/// </remarks>
public interface IWorkflowNotifier<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// A step's message has been published and the job has moved onto it.
    /// </summary>
    ValueTask StepStartedAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken = default);

    /// <summary>
    /// The last step of the plan completed.
    /// </summary>
    ValueTask FinishedAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken = default);

    /// <summary>
    /// The job stopped for good, and will not be retried.
    /// </summary>
    ValueTask FailedAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken = default);
}

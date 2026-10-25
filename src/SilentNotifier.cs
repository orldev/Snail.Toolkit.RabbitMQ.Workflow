namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The notifier used when an application registers none of its own.
/// </summary>
internal sealed class SilentNotifier<TStep> : IWorkflowNotifier<TStep> where TStep : struct, Enum
{
    public ValueTask StepStartedAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    public ValueTask FinishedAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    public ValueTask FailedAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
}

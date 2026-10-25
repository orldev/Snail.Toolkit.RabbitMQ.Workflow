using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging.Abstractions;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

/// <summary>
/// A producer that keeps every message instead of sending it, as the native probe's does.
/// </summary>
internal sealed class KeepingProducer : IRabbitProducer
{
    public ConcurrentQueue<object> Published { get; } = [];

    public ValueTask PublishAsync<T>(
        [DisallowNull] T payload,
        Action<IRabbitProducerBuilder<T>>? overrides = null,
        CancellationToken cancellationToken = default)
    {
        Published.Enqueue(payload);

        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// A step whose work is a function of the token it is handed.
/// </summary>
internal sealed class StepConsumer<TCommand, TResult>(
    IJobCancellation cancellation,
    IWorkflowDispatcher<Step> dispatcher,
    Func<CancellationToken, Task<TResult>> work)
    : WorkflowConsumer<TCommand, Step, TResult>(cancellation, dispatcher, NullLoggerFactory.Instance)
    where TCommand : IWorkflowCommand<Step>
{
    protected override Task<TResult> ProcessAsync(TCommand message, CancellationToken cancellationToken) =>
        work(cancellationToken);
}

internal static class Chaos
{
    public static WorkflowDispatcher<Step> Dispatcher(
        IRabbitProducer producer,
        IJobCancellation cancellation,
        IWorkflowNotifier<Step> notifier,
        IWorkflowLedger? ledger = null,
        bool hasDeadlines = false,
        TimeProvider? time = null) =>
        new(
            producer,
            TestWorkflow.Blueprint(),
            cancellation,
            ledger ?? new LocalLedger(TimeProvider.System),
            notifier,
            new WorkflowOptions(),
            time ?? TimeProvider.System,
            NullLogger<WorkflowDispatcher<Step>>.Instance,
            hasDeadlines ? new WorkflowDeadlines<Step>() : null);
}

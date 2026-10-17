using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow.NativeProbe;

/// <summary>The steps of the probe's plan.</summary>
public enum Step
{
    Search,
    Answer
}

/// <summary>The message of the first step.</summary>
public sealed record SearchCommand(WorkflowContext<Step> Context) : IWorkflowCommand<Step>;

/// <summary>The message of the last step, carrying what the first found.</summary>
public sealed record AnswerCommand(WorkflowContext<Step> Context, string Found) : IWorkflowCommand<Step>;

/// <summary>A producer that keeps every message instead of sending it; no broker.</summary>
public sealed class KeepingProducer : IRabbitProducer
{
    public List<object> Published { get; } = [];

    public ValueTask PublishAsync<T>([DisallowNull] T payload, Action<IRabbitProducerBuilder<T>>? overrides = null, CancellationToken cancellationToken = default)
    {
        Published.Add(payload);

        return ValueTask.CompletedTask;
    }
}

/// <summary>The first step: finds something and hands it on.</summary>
public sealed class SearchStep(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher, ILoggerFactory loggerFactory)
    : WorkflowConsumer<SearchCommand, Step, string>(cancellation, dispatcher, loggerFactory)
{
    protected override Task<string> ProcessAsync(SearchCommand message, CancellationToken cancellationToken) =>
        Task.FromResult("found Paris");
}

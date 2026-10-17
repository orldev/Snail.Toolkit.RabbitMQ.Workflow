using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>The steps of a search: find documents, rank them, answer.</summary>
public enum Step
{
    Search,
    Rank,
    Answer
}

/// <summary>The message of the first step.</summary>
public sealed record SearchCommand(WorkflowContext<Step> Context) : IWorkflowCommand<Step>;

/// <summary>The message of the second step, carrying what the first found.</summary>
public sealed record RankCommand(WorkflowContext<Step> Context, string Found) : IWorkflowCommand<Step>;

/// <summary>The message of the last step, carrying the ranking.</summary>
public sealed record AnswerCommand(WorkflowContext<Step> Context, string Ranked) : IWorkflowCommand<Step>;

/// <summary>A producer that accepts every message at once and counts them; no broker.</summary>
public sealed class CountingProducer : IRabbitProducer
{
    public long Published { get; private set; }

    public ValueTask PublishAsync<T>([DisallowNull] T payload, Action<IRabbitProducerBuilder<T>>? overrides = null, CancellationToken cancellationToken = default)
    {
        Published++;

        return ValueTask.CompletedTask;
    }
}

/// <summary>The search workflow registered as an application registers it, logging at Information as in production.</summary>
public static class Search
{
    public static ServiceProvider Services()
    {
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Information));
        services.AddSingleton<IRabbitProducer, CountingProducer>();
        services.AddWorkflow<Step>(workflow => workflow
            .Step<SearchCommand, None, string>(Step.Search, static (context, _) => new SearchCommand(context))
            .Step<RankCommand, string, string>(Step.Rank, static (context, found) => new RankCommand(context, found))
            .Step<AnswerCommand, string, string>(Step.Answer, static (context, ranked) => new AnswerCommand(context, ranked))
            .Plan("full", Step.Search, Step.Rank, Step.Answer));

        return services.BuildServiceProvider();
    }

    public static WorkflowContext<Step> Context(int index) =>
        new(Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"), "what is the capital of France", [Step.Search, Step.Rank, Step.Answer], index);
}

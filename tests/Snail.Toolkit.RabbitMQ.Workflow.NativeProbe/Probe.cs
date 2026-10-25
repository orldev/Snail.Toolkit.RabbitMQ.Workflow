using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.NativeProbe;

/// <summary>The steps of the probe's plan.</summary>
public enum Step
{
    Search,
    Answer
}

/// <summary>The message of the first step, carrying the question the job was started with.</summary>
public sealed record SearchCommand(WorkflowContext<Step> Context, string Question) : IWorkflowCommand<Step>;

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

/// <summary>A first step returning another type than the step is declared to produce.</summary>
public sealed class MisfitStep(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher, ILoggerFactory loggerFactory)
    : WorkflowConsumer<SearchCommand, Step, int>(cancellation, dispatcher, loggerFactory)
{
    protected override Task<int> ProcessAsync(SearchCommand message, CancellationToken cancellationToken) =>
        Task.FromResult(0);
}

/// <summary>
/// The startup check reads a consumer's result type off its generic base type; a native build has to keep that.
/// </summary>
public static class StartupCheck
{
    public static async Task<bool> PassesAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>()
        where TConsumer : class, IConsumer<SearchCommand>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbit(o => o.UseConnectionUrl("amqp://localhost"), rabbit => rabbit.AddConnection("probe", Topology<TConsumer>));
        services.AddWorkflow<Step>(Declared.Workflow);

        await using ServiceProvider provider = services.BuildServiceProvider();

        try
        {
            foreach (IHostedLifecycleService service in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
            {
                await service.StartingAsync(CancellationToken.None);
            }

            return true;
        }
        catch (WorkflowConfigurationException)
        {
            return false;
        }
    }

    private static void Topology<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(
        IRabbitConnectionBuilder connection)
        where TConsumer : class, IConsumer<SearchCommand> =>
        connection.AddConsumer(connection.AddQueue<SearchCommand>("probe.search").AsWorkflowStep(TimeSpan.FromSeconds(1)))
            .Subscribe<SearchCommand, TConsumer>();
}

/// <summary>The probe's workflow.</summary>
public static class Declared
{
    public static void Workflow(WorkflowBlueprint<Step> workflow) => workflow
        .Step<SearchCommand, string, string>(Step.Search, static (context, question) => new SearchCommand(context, question))
        .Step<AnswerCommand, string, string>(Step.Answer, static (context, found) => new AnswerCommand(context, found))
        .Plan("full", Step.Search, Step.Answer);
}

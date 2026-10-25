using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

/// <summary>
/// The topology a host declares, read before any of its consumers starts.
/// </summary>
public class WorkflowStartupCheckTests
{
    /// <summary>
    /// A step queue without retry rejected a failed message into nothing, and its job hung without a word.
    /// </summary>
    [Fact]
    public async Task Starting_StepQueueThatDoesNotRetry_FailsNamingTheQueue()
    {
        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(() => StartingAsync(PlainSearchQueue));

        Assert.Contains("q.search", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Starting_StepQueueDeclaredAsAWorkflowStep_Passes() =>
        await StartingAsync(RetryingSearchQueue);

    /// <summary>
    /// A consumer returning another type than its step produces used to fail its job as misconfigured, after the work.
    /// </summary>
    [Fact]
    public async Task Starting_ConsumerReturningWhatItsStepDoesNot_FailsNamingBoth()
    {
        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(() => StartingAsync(MisfitSearchQueue));

        Assert.Contains(nameof(MisfitConsumer), failure.Message, StringComparison.Ordinal);
        Assert.Contains("Search", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Starting_ConsumerOfACommandNoStepCarries_Fails()
    {
        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(() => StartingAsync(StrayQueue));

        Assert.Contains(nameof(CountedCommand), failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Starting_ProcessThatOnlyStartsJobs_Passes() =>
        await StartingAsync(NoQueues);

    /// <summary>
    /// A deadline read against a ledger kept in one process would end a job that had finished on another worker.
    /// </summary>
    [Fact]
    public async Task Starting_DeadlineWithALedgerKeptInOneProcess_FailsNamingTheLedger()
    {
        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(() => StartingAsync(Deadlined));

        Assert.Contains(nameof(IWorkflowLedger), failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Starting_DeadlineWithALedgerTheFleetShares_Passes() =>
        await StartingAsync(Deadlined, new SharedLedger());

    private static async Task StartingAsync(Action<IRabbitConnectionBuilder> connection, IWorkflowLedger? ledger = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        if (ledger is not null)
            services.AddSingleton(ledger);

        services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"), cs => cs.AddConnection("bus", connection));
        services.AddWorkflow<Step>(Declare);

        await using ServiceProvider provider = services.BuildServiceProvider();
        IHostedLifecycleService check = provider.GetServices<IHostedService>().OfType<WorkflowStartupCheck<Step>>().Single();

        await check.StartingAsync(CancellationToken.None);
    }

    private static void Declare(WorkflowBlueprint<Step> workflow) => workflow
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
        .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(Step.Search, (context, terms) => new SearchCommand(context, terms))
        .Plan("expand-search", Step.Expand, Step.Search);

    private static void Deadlined(IRabbitConnectionBuilder connection) => connection.AddWorkflowDeadline<Step>("q.deadline");

    private static void PlainSearchQueue(IRabbitConnectionBuilder connection) =>
        connection.AddConsumer(connection.AddQueue<SearchCommand>("q.search")).Subscribe<SearchCommand, SearchConsumer>();

    private static void RetryingSearchQueue(IRabbitConnectionBuilder connection) =>
        connection.AddConsumer(connection.AddQueue<SearchCommand>("q.search").AsWorkflowStep(TimeSpan.FromSeconds(1)))
            .Subscribe<SearchCommand, SearchConsumer>();

    private static void MisfitSearchQueue(IRabbitConnectionBuilder connection) =>
        connection.AddConsumer(connection.AddQueue<SearchCommand>("q.search").AsWorkflowStep(TimeSpan.FromSeconds(1)))
            .Subscribe<SearchCommand, MisfitConsumer>();

    private static void StrayQueue(IRabbitConnectionBuilder connection) =>
        connection.AddConsumer(connection.AddQueue<CountedCommand>("q.count").AsWorkflowStep(TimeSpan.FromSeconds(1)))
            .Subscribe<CountedCommand, StrayConsumer>();

    private static void NoQueues(IRabbitConnectionBuilder connection)
    {
    }

    private sealed class SearchConsumer(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher)
        : WorkflowConsumer<SearchCommand, Step, IReadOnlyList<string>>(cancellation, dispatcher, NullLoggerFactory.Instance)
    {
        protected override Task<IReadOnlyList<string>> ProcessAsync(SearchCommand message, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>([]);
    }

    private sealed class MisfitConsumer(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher)
        : WorkflowConsumer<SearchCommand, Step, string>(cancellation, dispatcher, NullLoggerFactory.Instance)
    {
        protected override Task<string> ProcessAsync(SearchCommand message, CancellationToken cancellationToken) =>
            Task.FromResult(string.Empty);
    }

    private sealed class StrayConsumer(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher)
        : WorkflowConsumer<CountedCommand, Step, int>(cancellation, dispatcher, NullLoggerFactory.Instance)
    {
        protected override Task<int> ProcessAsync(CountedCommand message, CancellationToken cancellationToken) =>
            Task.FromResult(0);
    }
}

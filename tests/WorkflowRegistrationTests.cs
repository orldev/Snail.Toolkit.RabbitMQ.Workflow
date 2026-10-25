using Microsoft.Extensions.DependencyInjection;
using Moq;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Queues;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowRegistrationTests
{
    /// <summary>
    /// A cancellation registry per scope hands every message a map of its own, and every cancellation is lost
    /// without a word: the reason the library registers it rather than leaving it to each application.
    /// </summary>
    [Fact]
    public void AddWorkflow_AnyApplication_HasOneJobCancellationForTheWholeProcess()
    {
        using ServiceProvider provider = Provider(new ServiceCollection());
        using IServiceScope one = provider.CreateScope();
        using IServiceScope other = provider.CreateScope();

        IJobCancellation first = one.ServiceProvider.GetRequiredService<IJobCancellation>();
        IJobCancellation second = other.ServiceProvider.GetRequiredService<IJobCancellation>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddWorkflow_AfterTheApplicationsOwnNotifier_KeepsThatNotifier()
    {
        var services = new ServiceCollection();
        IWorkflowNotifier<Step> notifier = Mock.Of<IWorkflowNotifier<Step>>();
        services.AddSingleton(notifier);

        using ServiceProvider provider = Provider(services);

        Assert.Same(notifier, provider.GetRequiredService<IWorkflowNotifier<Step>>());
    }

    [Fact]
    public void AddWorkflow_AnyApplication_ResolvesADispatcher()
    {
        using ServiceProvider provider = Provider(new ServiceCollection());

        Assert.NotNull(provider.GetRequiredService<IWorkflowDispatcher<Step>>());
    }

    /// <summary>
    /// The options are a singleton every worker reads on each new job, and the deadline was checked once, at
    /// registration: a lifetime set to zero afterwards cancelled every job on arrival.
    /// </summary>
    [Fact]
    public void WorkflowOptions_ChangedAfterRegistration_IsRefused()
    {
        using ServiceProvider provider = Provider(new ServiceCollection());
        WorkflowOptions options = provider.GetRequiredService<WorkflowOptions>();

        Assert.Throws<WorkflowConfigurationException>(() => options.JobLifetime = TimeSpan.Zero);
    }

    [Fact]
    public void AddWorkflow_PlanThatDoesNotFit_FailsTheRegistration()
    {
        var services = new ServiceCollection();

        Assert.Throws<WorkflowConfigurationException>(() => services.AddWorkflow<Step>(Misfit));
    }

    /// <summary>
    /// A workflow consumer rethrows a transient failure on the assumption that the broker brings the message back,
    /// and a step whose attempts ran out is the message that could still finish its job.
    /// </summary>
    [Fact]
    public void AsWorkflowStep_AnyQueue_IsDurableRetryingAndParked()
    {
        var queue = new QueueBuilder<SearchCommand>(new RabbitQueueDeclaration(new RabbitConnectionDeclaration("bus"), "q.search"));

        queue.AsWorkflowStep(TimeSpan.FromSeconds(30), maxAttempts: 5);

        Assert.True(queue.Declaration.Durable);
        Assert.Equal(5, queue.Declaration.MaxAttempts);
        Assert.Equal("q.search.parked", queue.Declaration.ParkingQueue?.Name);
    }

    private static ServiceProvider Provider(IServiceCollection services)
    {
        services.AddLogging();
        services.AddSingleton(Mock.Of<IRabbitProducer>());
        services.AddWorkflow<Step>(Declare);

        return services.BuildServiceProvider();
    }

    private static void Declare(WorkflowBlueprint<Step> workflow) => workflow
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
        .Step<SearchCommand, IReadOnlyList<string>, string>(Step.Search, (context, terms) => new SearchCommand(context, terms))
        .Plan("expand-search", Step.Expand, Step.Search);

    private static void Misfit(WorkflowBlueprint<Step> workflow) => workflow
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
        .Step<RankCommand, string, string>(Step.Rank, (context, _) => new RankCommand(context, []))
        .Plan("misfit", Step.Expand, Step.Rank);

    private sealed class QueueBuilder<T>(RabbitQueueDeclaration declaration) : IRabbitQueueBuilder<T>
    {
        public RabbitQueueDeclaration Declaration { get; } = declaration;
    }
}

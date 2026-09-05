using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Workflow;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowDispatcherTests
{
    private readonly Mock<IRabbitProducer> _producer = new(MockBehavior.Strict);
    private readonly Mock<IWorkflowNotifier<Step>> _notifier = new();
    private readonly JobCancellation _cancellation =
        new(new WorkflowOptions(), TimeProvider.System);

    [Fact]
    public async Task Dispatch_MidPlan_PublishesTheNextStepsCommand()
    {
        var published = Publishes<SearchCommand>();
        var context = TestWorkflow.Context(currentStepIndex: 0);

        await Dispatcher().DispatchAsync(context, (IReadOnlyList<string>)["blue sky", "rayleigh"]);

        Assert.Equal(Step.Search, published.Value!.Context.CurrentStep);
        Assert.Equal(["blue sky", "rayleigh"], published.Value.Terms);
    }

    [Fact]
    public async Task Start_ANamedPlan_PublishesItsFirstStep()
    {
        var published = Publishes<ExpandCommand>();

        await Dispatcher().StartAsync(TestWorkflow.FullSearch, Guid.NewGuid(), "why is the sky blue");

        Assert.Equal(Step.Expand, published.Value!.Context.CurrentStep);
    }

    [Fact]
    public async Task Dispatch_AfterTheLastStep_FinishesTheJobAndPublishesNothing()
    {
        var context = TestWorkflow.Context(currentStepIndex: 2);
        var job = context.JobId;
        _cancellation.Token(job);

        await Dispatcher().DispatchAsync(context, "the sky scatters blue light");

        _notifier.Verify(n => n.FinishedAsync(context, It.IsAny<CancellationToken>()), Times.Once);
        Assert.False(_cancellation.Complete(job));
    }

    /// <summary>
    /// A context can be built by hand from a step list no declared plan ever covered, so the pairing
    /// startup validation could not see is checked again here rather than published blindly.
    /// </summary>
    [Fact]
    public async Task Dispatch_IntoAStepThatCannotAcceptTheResult_NamesTheStepAndTheType()
    {
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
            .Step<RankCommand, string, string>(Step.Rank, (context, _) => new RankCommand(context, []));
        var adHoc = new WorkflowContext<Step>(
            Guid.NewGuid(), "query", [Step.Expand, Step.Rank], currentStepIndex: 0);

        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(async () =>
            await Dispatcher(blueprint).DispatchAsync(adHoc, (IReadOnlyList<string>)["term"]));

        Assert.Contains("Rank", failure.Message, StringComparison.Ordinal);
        Assert.Contains("String", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dispatch_AResultTheStepIsNotDeclaredToProduce_IsRejected()
    {
        var context = TestWorkflow.Context(currentStepIndex: 0);

        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(async () =>
            await Dispatcher().DispatchAsync(context, 42));

        Assert.Contains("Expand", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Fail_AnyJob_ReportsItAndReleasesTheJob()
    {
        var context = TestWorkflow.Context(currentStepIndex: 1);
        var failure = new WorkflowFailure("No documents came back.", nameof(Step.Search));
        _cancellation.Token(context.JobId);

        await Dispatcher().FailAsync(context, failure);

        _notifier.Verify(n => n.FailedAsync(context, failure, It.IsAny<CancellationToken>()), Times.Once);
        Assert.False(_cancellation.Complete(context.JobId));
    }

    private StrongBox<TCommand> Publishes<TCommand>() where TCommand : class
    {
        var captured = new StrongBox<TCommand>();

        _producer
            .Setup(p => p.PublishAsync(
                It.IsAny<TCommand>(),
                It.IsAny<Action<IRabbitProducerBuilder<TCommand>>?>(),
                It.IsAny<CancellationToken>()))
            .Callback<TCommand, Action<IRabbitProducerBuilder<TCommand>>?, CancellationToken>(
                (command, _, _) => captured.Value = command)
            .Returns(ValueTask.CompletedTask);

        return captured;
    }

    private WorkflowDispatcher<Step> Dispatcher(WorkflowBlueprint<Step>? blueprint = null) =>
        new(_producer.Object,
            blueprint ?? TestWorkflow.Blueprint(),
            _cancellation,
            _notifier.Object,
            NullLogger<WorkflowDispatcher<Step>>.Instance);

    private sealed class StrongBox<T> where T : class
    {
        public T? Value { get; set; }
    }
}

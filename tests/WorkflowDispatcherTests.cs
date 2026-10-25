using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowDispatcherTests
{
    private readonly Mock<IRabbitProducer> _producer = new(MockBehavior.Strict);
    private readonly Mock<IWorkflowNotifier<Step>> _notifier = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 23, 9, 0, 0, TimeSpan.Zero));
    private readonly LocalLedger _ledger;

    public WorkflowDispatcherTests() => _ledger = new LocalLedger(_time);
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
    public async Task Dispatch_AValueTypeResult_PublishesItToTheStepThatAcceptsIt()
    {
        var published = Publishes<CountedCommand>();
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, int>(Step.Expand, (context, _) => new ExpandCommand(context))
            .Step<CountedCommand, int, string>(Step.Search, (context, count) => new CountedCommand(context, count));
        var context = new WorkflowContext<Step>(Guid.NewGuid(), [Step.Expand, Step.Search], currentStepIndex: 0);

        await Dispatcher(blueprint).DispatchAsync(context, 12);

        Assert.Equal(12, published.Value!.Count);
        Assert.Equal(Step.Search, published.Value.Context.CurrentStep);
    }

    [Fact]
    public async Task Dispatch_AResultOfADerivedType_PublishesItAsTheAcceptedType()
    {
        var published = Publishes<SearchCommand>();
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, string[]>(Step.Expand, (context, _) => new ExpandCommand(context))
            .Step<SearchCommand, IReadOnlyList<string>, string>(Step.Search, (context, terms) => new SearchCommand(context, terms));
        var context = new WorkflowContext<Step>(Guid.NewGuid(), [Step.Expand, Step.Search], currentStepIndex: 0);

        await Dispatcher(blueprint).DispatchAsync(context, new[] { "blue sky" });

        Assert.Equal(["blue sky"], published.Value!.Terms);
    }

    [Fact]
    public async Task Start_ANamedPlan_PublishesItsFirstStep()
    {
        var published = Publishes<ExpandCommand>();

        await Dispatcher().StartAsync(TestWorkflow.FullSearch, Guid.NewGuid(), default(None));

        Assert.Equal(Step.Expand, published.Value!.Context.CurrentStep);
    }

    /// <summary>
    /// A job started without an id gets one no other job can have, and the caller learns it.
    /// </summary>
    [Fact]
    public async Task Start_WithoutAnId_GivesTheJobANewOneAndAnswersWithIt()
    {
        var published = Publishes<ExpandCommand>();

        Guid jobId = await Dispatcher().StartAsync(TestWorkflow.FullSearch, default(None), CancellationToken.None);

        Assert.Equal(jobId, published.Value!.Context.JobId);
        Assert.Equal(7, jobId.Version);
    }

    [Fact]
    public async Task Start_AnyJob_StampsItsStartInUtc()
    {
        var published = Publishes<ExpandCommand>();

        await Dispatcher().StartAsync(TestWorkflow.FullSearch, Guid.NewGuid(), default(None));

        Assert.Equal(_time.GetUtcNow().UtcDateTime, published.Value!.Context.StartedAt);
    }

    [Fact]
    public async Task Start_WithThePayloadTheFirstStepAccepts_HandsItToThatStep()
    {
        var published = Publishes<SearchCommand>();
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<SearchCommand, IReadOnlyList<string>, string>(Step.Search, (context, terms) => new SearchCommand(context, terms))
            .Plan("search", Step.Search);

        await Dispatcher(blueprint).StartAsync("search", Guid.NewGuid(), (IReadOnlyList<string>)["blue sky"]);

        Assert.Equal(["blue sky"], published.Value!.Terms);
    }

    /// <summary>
    /// The payload a job starts with is the one pairing startup cannot see, so it is checked by the call that chose
    /// it, before anything is published.
    /// </summary>
    [Fact]
    public async Task Start_WithAPayloadTheFirstStepDoesNotAccept_IsRefusedBeforeAnythingIsPublished()
    {
        var failure = await Assert.ThrowsAsync<WorkflowConfigurationException>(async () =>
            await Dispatcher().StartAsync(TestWorkflow.FullSearch, Guid.NewGuid(), "why is the sky blue"));

        Assert.Contains("Expand", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every job started with an empty id would share one cancellation entry, and the first of them to end would
    /// release it for all the others.
    /// </summary>
    [Fact]
    public async Task Start_WithAnEmptyJobId_IsRefused() =>
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await Dispatcher().StartAsync(TestWorkflow.FullSearch, Guid.Empty, default(None)));

    [Fact]
    public async Task Dispatch_AfterTheLastStep_FinishesTheJobAndPublishesNothing()
    {
        var context = TestWorkflow.Context(currentStepIndex: 2);
        var job = context.JobId;
        _cancellation.Token(job);

        await Dispatcher().DispatchAsync(context, "the sky scatters blue light");

        _notifier.Verify(n => n.FinishedAsync(context, It.IsAny<CancellationToken>()), Times.Once);
        Assert.False(_cancellation.TryComplete(job));
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
            Guid.NewGuid(), [Step.Expand, Step.Rank], currentStepIndex: 0);

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
        Assert.False(_cancellation.TryComplete(context.JobId));
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
            _ledger,
            _notifier.Object,
            new WorkflowOptions(),
            _time,
            NullLogger<WorkflowDispatcher<Step>>.Instance);

    private sealed class StrongBox<T> where T : class
    {
        public T? Value { get; set; }
    }
}

public sealed record CountedCommand(WorkflowContext<Step> Context, int Count) : IWorkflowCommand<Step>;

using Moq;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

/// <summary>
/// A deadline that passed between two steps used to say nothing: the job was reported cancelled when its next message
/// arrived, and never if none did. Its deadline message now comes back and ends it — unless it ended first.
/// </summary>
public class DeadlineChaosTests
{
    private readonly KeepingProducer _producer = new();
    private readonly Mock<IWorkflowNotifier<Step>> _notifier = new();
    private readonly JobCancellation _cancellation = new(new WorkflowOptions(), TimeProvider.System);
    private readonly SharedLedger _ledger = new();
    private readonly WorkflowContext<Step> _started = TestWorkflow.Context(currentStepIndex: -1);

    [Fact]
    public async Task Deadline_OfAJobStillRunning_EndsItOnceHoweverOftenItComes()
    {
        WorkflowDeadlineConsumer<Step> deadline = Deadline();

        await deadline.HandleAsync(new WorkflowDeadline<Step>(_started));
        await deadline.HandleAsync(new WorkflowDeadline<Step>(_started));

        _notifier.Verify(n => n.FailedAsync(
            _started,
            It.Is<WorkflowFailure>(failure => failure.Reason == "The job outlived its deadline."),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deadline_OfAJobThatFinished_TellsNobody()
    {
        WorkflowDispatcher<Step> dispatcher = Dispatcher();
        await dispatcher.DispatchAsync(_started with { CurrentStepIndex = 2 }, "the answer");

        await Deadline(dispatcher).HandleAsync(new WorkflowDeadline<Step>(_started));

        _notifier.Verify(n => n.FailedAsync(
            It.IsAny<WorkflowContext<Step>>(),
            It.IsAny<WorkflowFailure>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Step_OfAJobPastItsDeadline_IsNotHandedOn()
    {
        WorkflowDispatcher<Step> dispatcher = Dispatcher();
        await Deadline(dispatcher).HandleAsync(new WorkflowDeadline<Step>(_started));

        await dispatcher.DispatchAsync(_started with { CurrentStepIndex = 1 }, (IReadOnlyList<string>)["a document"]);

        Assert.Empty(_producer.Published.OfType<RankCommand>());
    }

    [Fact]
    public async Task Start_WithDeadlines_PublishesTheDeadlineAfterTheFirstStep()
    {
        WorkflowDispatcher<Step> dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object, _ledger, hasDeadlines: true);

        Guid jobId = await dispatcher.StartAsync(TestWorkflow.FullSearch, default(None), CancellationToken.None);

        Assert.Collection(
            _producer.Published,
            first => Assert.IsType<ExpandCommand>(first),
            deadline => Assert.Equal(jobId, Assert.IsType<WorkflowDeadline<Step>>(deadline).Context.JobId));
    }

    private WorkflowDispatcher<Step> Dispatcher() => Chaos.Dispatcher(_producer, _cancellation, _notifier.Object, _ledger);

    private WorkflowDeadlineConsumer<Step> Deadline(WorkflowDispatcher<Step>? dispatcher = null) =>
        new(dispatcher ?? Dispatcher(), _cancellation);
}

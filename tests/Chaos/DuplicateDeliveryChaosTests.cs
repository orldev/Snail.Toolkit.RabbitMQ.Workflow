using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

/// <summary>
/// The broker delivers at least once: a step redelivered after a crash, a message published twice when a connection
/// dropped. Each of these used to publish the next step again, and the job forked into two that both ran to the end.
/// </summary>
public class DuplicateDeliveryChaosTests
{
    private readonly KeepingProducer _producer = new();
    private readonly Mock<IWorkflowNotifier<Step>> _notifier = new();
    private readonly JobCancellation _cancellation = new(new WorkflowOptions(), TimeProvider.System);

    [Fact]
    public async Task Step_DeliveredTwice_IsHandedOnOnce()
    {
        var dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1);

        await dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]);
        await dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]);

        Assert.Single(_producer.Published.OfType<RankCommand>());
    }

    /// <summary>
    /// A hand-over is marked before its message is published. When the publish fails, the mark is taken back — or the
    /// step's retry would be dropped as its own duplicate and the job would hang.
    /// </summary>
    [Fact]
    public async Task Step_WhosePublishFailed_IsHandedOnByItsRetry()
    {
        var flaky = new FlakyProducer(_producer);
        var dispatcher = Chaos.Dispatcher(flaky, _cancellation, _notifier.Object);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1);

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
            await dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]));
        await dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]);

        Assert.Single(_producer.Published.OfType<RankCommand>());
    }

    /// <summary>
    /// Taking a mark back used to wait on the ledger with no token: a shared ledger that stopped answering held the
    /// failed step, and the host's shutdown, for as long as it stayed silent. It is given five seconds now, and the
    /// publish failure goes on to the broker's retry.
    /// </summary>
    [Fact]
    public async Task PublishFailure_OnALedgerThatNeverAnswers_GoesOnAfterFiveSeconds()
    {
        var clock = new FakeTimeProvider();
        WorkflowDispatcher<Step> dispatcher = Chaos.Dispatcher(new FlakyProducer(_producer), _cancellation, _notifier.Object, new SilentLedger(), time: clock);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1);

        Task handing = dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]).AsTask();
        clock.Advance(TimeSpan.FromSeconds(4));
        bool isHeldAtFourSeconds = !handing.IsCompleted;
        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.True(isHeldAtFourSeconds);
        await Assert.ThrowsAsync<HttpRequestException>(() => handing.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task LastStep_DeliveredTwice_FinishesTheJobOnce()
    {
        var dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object);
        WorkflowContext<Step> ranking = TestWorkflow.Context(currentStepIndex: 2);

        await dispatcher.DispatchAsync(ranking, "the answer");
        await dispatcher.DispatchAsync(ranking, "the answer");

        _notifier.Verify(n => n.FinishedAsync(ranking, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Failure_ReportedTwice_IsToldOnce()
    {
        var dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1);
        var failure = new WorkflowFailure("The step failed on every attempt.", nameof(Step.Search));

        await dispatcher.FailAsync(searching, failure);
        await dispatcher.FailAsync(searching, failure);

        _notifier.Verify(n => n.FailedAsync(searching, failure, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Step_OfAJobThatHasEnded_IsNotHandedOn()
    {
        var dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1);

        await dispatcher.FailAsync(searching, new WorkflowFailure("The job was cancelled.", nameof(Step.Search)));
        await dispatcher.DispatchAsync(searching, (IReadOnlyList<string>)["a document"]);

        Assert.Empty(_producer.Published.OfType<RankCommand>());
    }

    /// <summary>
    /// Two jobs started under one id used to share their cancellation and deadline on every worker.
    /// </summary>
    [Fact]
    public async Task Job_StartedTwiceUnderOneId_IsRefusedTheSecondTime()
    {
        var dispatcher = Chaos.Dispatcher(_producer, _cancellation, _notifier.Object);
        var job = Guid.NewGuid();

        await dispatcher.StartAsync(TestWorkflow.FullSearch, job, default(None));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await dispatcher.StartAsync(TestWorkflow.FullSearch, job, default(None)));
        Assert.Single(_producer.Published.OfType<ExpandCommand>());
    }

    /// <summary>
    /// A start whose first message could not be published is taken back, so the caller can retry it under the same id.
    /// </summary>
    [Fact]
    public async Task Job_WhoseStartFailedToPublish_StartsWhenRetriedUnderTheSameId()
    {
        var dispatcher = Chaos.Dispatcher(new FlakyProducer(_producer), _cancellation, _notifier.Object);
        var job = Guid.NewGuid();

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
            await dispatcher.StartAsync(TestWorkflow.FullSearch, job, default(None)));
        await dispatcher.StartAsync(TestWorkflow.FullSearch, job, default(None));

        Assert.Single(_producer.Published.OfType<ExpandCommand>());
    }

    /// <summary>
    /// A shared ledger that takes marks, and never answers when one is given back, whatever its token says.
    /// </summary>
    private sealed class SilentLedger : IWorkflowLedger
    {
        private readonly LocalLedger _marks = new(TimeProvider.System);

        public bool IsShared => true;

        public ValueTask<bool> TryHandOverAsync(Guid jobId, int stepIndex, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
            _marks.TryHandOverAsync(jobId, stepIndex, keepFor, cancellationToken);

        public ValueTask ReleaseHandOverAsync(Guid jobId, int stepIndex, CancellationToken cancellationToken = default) =>
            new(new TaskCompletionSource().Task);

        public ValueTask<bool> TryEndAsync(Guid jobId, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
            _marks.TryEndAsync(jobId, keepFor, cancellationToken);
    }

    private sealed class FlakyProducer(IRabbitProducer producer) : IRabbitProducer
    {
        private int _publishes;

        public ValueTask PublishAsync<T>(
            [DisallowNull] T payload,
            Action<IRabbitProducerBuilder<T>>? overrides = null,
            CancellationToken cancellationToken = default) =>
            Interlocked.Increment(ref _publishes) == 1
                ? ValueTask.FromException(new HttpRequestException("the broker is unreachable"))
                : producer.PublishAsync(payload, overrides, cancellationToken);
    }
}

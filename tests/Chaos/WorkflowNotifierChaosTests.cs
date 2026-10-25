using Moq;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

public class WorkflowNotifierChaosTests
{
    private readonly KeepingProducer _producer = new();
    private readonly Mock<IWorkflowNotifier<Step>> _notifier = new();
    private readonly JobCancellation _cancellation = new(new WorkflowOptions(), TimeProvider.System);

    /// <summary>
    /// The notifier is a side channel — a Redis stream, a websocket — yet its failure on the last step used to escape
    /// the handler as a transient error. The broker then ran the whole last step again for a job whose work was done,
    /// and with a fresh deadline, since the job had been released before the notifier was called. A Redis outage
    /// turned every finishing job into a repeated final step.
    /// </summary>
    [Fact]
    public async Task Notifier_FailsOnFinished_TheFinishedStepIsNotHandedBackToTheBroker()
    {
        _notifier
            .Setup(n => n.FinishedAsync(It.IsAny<WorkflowContext<Step>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new HttpRequestException("the progress stream is down"));
        var message = new RankCommand(TestWorkflow.Context(currentStepIndex: 2), ["a document"]);
        var consumer = Consumer<RankCommand, string>(_ => Task.FromResult("the answer"));

        var escaped = await Record.ExceptionAsync(async () => await consumer.HandleAsync(message, CancellationToken.None));

        Assert.Null(escaped);
    }

    /// <summary>
    /// The notifier used to be told a step started before its message was published. When the notifier failed, the
    /// next step was never published and the current one was retried: the work was redone because a progress line
    /// could not be written.
    /// </summary>
    [Fact]
    public async Task Notifier_FailsOnStepStarted_TheNextStepIsStillPublished()
    {
        _notifier
            .Setup(n => n.StepStartedAsync(It.IsAny<WorkflowContext<Step>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new HttpRequestException("the progress stream is down"));
        var message = new SearchCommand(TestWorkflow.Context(currentStepIndex: 1), ["blue sky"]);
        var consumer = Consumer<SearchCommand, IReadOnlyList<string>>(_ => Task.FromResult<IReadOnlyList<string>>(["a document"]));

        await Record.ExceptionAsync(async () => await consumer.HandleAsync(message, CancellationToken.None));

        Assert.Single(_producer.Published.OfType<RankCommand>());
    }

    /// <summary>
    /// A final failure used to be reported with <see cref="CancellationToken.None"/>, so a notifier that waits on an
    /// unreachable transport held the handler — and with Prefetch(1) the whole consumer — for as long as the transport
    /// stayed down, past the host's own shutdown.
    /// </summary>
    [Fact]
    public async Task Notifier_HangsOnFailed_HostShutdownStillEndsTheHandler()
    {
        _notifier
            .Setup(n => n.FailedAsync(
                It.IsAny<WorkflowContext<Step>>(),
                It.IsAny<WorkflowFailure>(),
                It.IsAny<CancellationToken>()))
            .Returns((WorkflowContext<Step> _, WorkflowFailure _, CancellationToken token) =>
                new ValueTask(Task.Delay(Timeout.Infinite, token)));
        var message = new SearchCommand(TestWorkflow.Context(currentStepIndex: 1), ["blue sky"]);
        var consumer = Consumer<SearchCommand, IReadOnlyList<string>>(_ =>
            throw new WorkflowFailureException("The query matched nothing."));
        using var host = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var handled = consumer.HandleAsync(message, host.Token).AsTask();
        var finished = await Task.WhenAny(handled, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.Same(handled, finished);
    }

    /// <summary>
    /// The host's token only helps a notifier that listens to it. One that waits on its transport without a token
    /// held the handler past shutdown all the same.
    /// </summary>
    [Fact]
    public async Task Notifier_DeafToItsTokenHangsOnFailed_HostShutdownStillEndsTheHandler()
    {
        _notifier
            .Setup(n => n.FailedAsync(
                It.IsAny<WorkflowContext<Step>>(),
                It.IsAny<WorkflowFailure>(),
                It.IsAny<CancellationToken>()))
            .Returns(new ValueTask(Task.Delay(Timeout.Infinite, CancellationToken.None)));
        var message = new SearchCommand(TestWorkflow.Context(currentStepIndex: 1), ["blue sky"]);
        var consumer = Consumer<SearchCommand, IReadOnlyList<string>>(_ =>
            throw new WorkflowFailureException("The query matched nothing."));
        using var host = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var handled = consumer.HandleAsync(message, host.Token).AsTask();
        var finished = await Task.WhenAny(handled, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.Same(handled, finished);
    }

    private StepConsumer<TCommand, TResult> Consumer<TCommand, TResult>(Func<CancellationToken, Task<TResult>> work)
        where TCommand : IWorkflowCommand<Step> =>
        new(_cancellation, Chaos.Dispatcher(_producer, _cancellation, _notifier.Object), work);
}

using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Snail.Toolkit.RabbitMQ.Workflow;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowConsumerTests
{
    private readonly Mock<IWorkflowDispatcher<Step>> _dispatcher = new();
    private readonly JobCancellation _cancellation = new(new WorkflowOptions(), TimeProvider.System);
    private readonly SearchCommand _message = new(TestWorkflow.Context(currentStepIndex: 1), ["blue sky"]);

    [Fact]
    public async Task Step_Succeeds_HandsItsResultToTheDispatcher()
    {
        var consumer = Consumer(_ => Task.FromResult<IReadOnlyList<string>>(["a document"]));

        await consumer.HandleAsync(_message, CancellationToken.None);

        _dispatcher.Verify(d => d.DispatchAsync(
            _message.Context,
            It.Is<IReadOnlyList<string>>(documents => documents.Single() == "a document"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// The job token and the host token used to be linked into one and only the linked token was
    /// examined, so a shutdown looked exactly like a cancelled job: the message was acknowledged
    /// and lost. Every deployment cost one in-flight message per consumer.
    /// </summary>
    [Fact]
    public async Task Host_IsShuttingDown_ReturnsTheMessageToTheBrokerAndLeavesTheJobAlone()
    {
        using var host = new CancellationTokenSource();
        await host.CancelAsync();
        var consumer = Consumer(token => throw new OperationCanceledException(token));

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await consumer.HandleAsync(_message, host.Token));

        VerifyJobWasNotEnded();
    }

    [Fact]
    public async Task Job_WasCancelled_EndsTheJobAndKeepsTheMessageAcknowledged()
    {
        _cancellation.Token(_message.Context.JobId);
        _cancellation.Cancel(_message.Context.JobId);
        var consumer = Consumer(token =>
        {
            token.ThrowIfCancellationRequested();
            return Task.FromResult<IReadOnlyList<string>>([]);
        });

        await consumer.HandleAsync(_message, CancellationToken.None);

        _dispatcher.Verify(d => d.FailAsync(
            _message.Context,
            It.Is<WorkflowFailure>(failure => failure.Step == nameof(Step.Search)),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// The old handler reported the job dead and rethrew in the same breath, so the broker ran the
    /// step again for a job that had already been finalized and removed.
    /// </summary>
    [Fact]
    public async Task Step_FailsTransiently_IsRetriedByTheBrokerWithoutEndingTheJob()
    {
        var consumer = Consumer(_ => throw new HttpRequestException("the search engine is down"));

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
            await consumer.HandleAsync(_message, CancellationToken.None));

        VerifyJobWasNotEnded();
    }

    [Fact]
    public async Task Step_FailsFinally_EndsTheJobWithoutARetry()
    {
        var consumer = Consumer(_ => throw new WorkflowFailureException("The query matched nothing."));

        await consumer.HandleAsync(_message, CancellationToken.None);

        _dispatcher.Verify(d => d.FailAsync(
            _message.Context,
            It.Is<WorkflowFailure>(failure => failure.Reason == "The query matched nothing."),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Step_DecidesForItselfWhatIsWorthRetrying_IsObeyed()
    {
        var consumer = new StubbornConsumer(_cancellation, _dispatcher.Object);

        await consumer.HandleAsync(_message, CancellationToken.None);

        _dispatcher.Verify(d => d.FailAsync(
            It.IsAny<WorkflowContext<Step>>(),
            It.IsAny<WorkflowFailure>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// Reporting a failure goes over a transport of its own, and when that transport was down the
    /// exception from it replaced the step's, hiding what actually went wrong.
    /// </summary>
    [Fact]
    public async Task Reporting_TheFailureAlsoFails_DoesNotEscapeTheHandler()
    {
        _dispatcher
            .Setup(d => d.FailAsync(
                It.IsAny<WorkflowContext<Step>>(),
                It.IsAny<WorkflowFailure>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("the notification channel is down"));
        var consumer = Consumer(_ => throw new WorkflowFailureException("The query matched nothing."));

        await consumer.HandleAsync(_message, CancellationToken.None);
    }

    private void VerifyJobWasNotEnded() =>
        _dispatcher.Verify(d => d.FailAsync(
            It.IsAny<WorkflowContext<Step>>(),
            It.IsAny<WorkflowFailure>(),
            It.IsAny<CancellationToken>()), Times.Never);

    private TestConsumer Consumer(Func<CancellationToken, Task<IReadOnlyList<string>>> work) =>
        new(_cancellation, _dispatcher.Object, work);

    private sealed class TestConsumer(
        IJobCancellation cancellation,
        IWorkflowDispatcher<Step> dispatcher,
        Func<CancellationToken, Task<IReadOnlyList<string>>> work)
        : WorkflowConsumer<SearchCommand, Step, IReadOnlyList<string>>(
            cancellation, dispatcher, NullLoggerFactory.Instance)
    {
        protected override Task<IReadOnlyList<string>> ProcessAsync(
            SearchCommand message,
            CancellationToken cancellationToken) => work(cancellationToken);
    }

    private sealed class StubbornConsumer(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher)
        : WorkflowConsumer<SearchCommand, Step, IReadOnlyList<string>>(
            cancellation, dispatcher, NullLoggerFactory.Instance)
    {
        protected override bool CanRetry(Exception exception) => false;

        protected override Task<IReadOnlyList<string>> ProcessAsync(
            SearchCommand message,
            CancellationToken cancellationToken) => throw new HttpRequestException("the search engine is down");
    }
}

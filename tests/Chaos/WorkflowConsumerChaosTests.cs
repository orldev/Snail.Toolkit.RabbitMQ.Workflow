using Microsoft.Extensions.Time.Testing;
using Moq;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

public class WorkflowConsumerChaosTests
{
    /// <summary>
    /// A job cancelled while its next message waited in the queue used to be handed an already-cancelled token and
    /// nothing else: the handler never looked at the token itself. A step that does not poll it — a synchronous
    /// computation, a client without cancellation — did its whole work and handed the result on, advancing the job.
    /// </summary>
    [Fact]
    public async Task Job_CancelledBeforeItsMessageArrives_IsNotAdvanced()
    {
        var dispatcher = new Mock<IWorkflowDispatcher<Step>>();
        var cancellation = new JobCancellation(new WorkflowOptions(), TimeProvider.System);
        var message = new SearchCommand(TestWorkflow.Context(currentStepIndex: 1), ["blue sky"]);
        cancellation.Token(message.Context.JobId);
        cancellation.TryCancel(message.Context.JobId);
        var consumer = new StepConsumer<SearchCommand, IReadOnlyList<string>>(
            cancellation, dispatcher.Object, _ => Task.FromResult<IReadOnlyList<string>>(["a document"]));

        await consumer.HandleAsync(message, CancellationToken.None);

        dispatcher.Verify(d => d.DispatchAsync(
            It.IsAny<WorkflowContext<Step>>(),
            It.IsAny<IReadOnlyList<string>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// A worker meeting a job whose lifetime ran out on other workers used to give it a fresh one and run its step.
    /// </summary>
    [Fact]
    public async Task Job_OlderThanItsLifetimeArrivingAtANewWorker_IsReportedCancelled()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new Mock<IWorkflowDispatcher<Step>>();
        var cancellation = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        WorkflowContext<Step> context = TestWorkflow.Context(currentStepIndex: 1) with
        {
            StartedAt = time.GetUtcNow().UtcDateTime.AddMinutes(-11)
        };
        var consumer = new StepConsumer<SearchCommand, IReadOnlyList<string>>(
            cancellation, dispatcher.Object, _ => Task.FromResult<IReadOnlyList<string>>(["a document"]));

        await consumer.HandleAsync(new SearchCommand(context, ["blue sky"]), CancellationToken.None);

        dispatcher.Verify(d => d.FailAsync(
            context,
            It.Is<WorkflowFailure>(failure => failure.Reason == "The job was cancelled."),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// The default json options let a message whose context went missing deserialize with a null one. The handler
    /// then threw a null reference from inside its own catch, while naming the step for the log, so nothing said what
    /// was wrong with the message.
    /// </summary>
    [Fact]
    public async Task Message_WithoutAContext_IsRefusedAsMisconfigured()
    {
        var cancellation = new JobCancellation(new WorkflowOptions(), TimeProvider.System);
        var consumer = new StepConsumer<SearchCommand, IReadOnlyList<string>>(
            cancellation, Mock.Of<IWorkflowDispatcher<Step>>(), _ => Task.FromResult<IReadOnlyList<string>>([]));

        await Assert.ThrowsAsync<WorkflowConfigurationException>(async () =>
            await consumer.HandleAsync(new SearchCommand(null!, ["blue sky"]), CancellationToken.None));
    }
}

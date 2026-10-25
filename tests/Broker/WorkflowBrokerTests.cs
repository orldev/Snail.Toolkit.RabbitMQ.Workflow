using RabbitMQ.Client;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Broker;

/// <summary>
/// The workflow over a real RabbitMQ: what no fake producer can show — retry, parking, and a shutdown that hands a
/// message back.
/// </summary>
[Trait("Category", "Container")]
public sealed class WorkflowBrokerTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private readonly string _fleet = $"wf-{Guid.NewGuid():N}"[..15];

    [BrokerFact]
    public async Task Job_RunOverABroker_FinishesWithEveryStepStarted()
    {
        var notifier = new RecordingNotifier();
        await using Worker worker = await Worker.StartAsync(await broker.ConnectionStringAsync(), _fleet, 3, Searching.Finds("a document"), notifier);

        await worker.Dispatcher.StartAsync(TestWorkflow.FullSearch, Guid.CreateVersion7(), default(None));
        WorkflowContext<Step> finished = await notifier.Finished.Task.WaitAsync(Patience);

        Assert.Equal(Step.Rank, finished.CurrentStep);
        Assert.Equal([Step.Expand, Step.Search, Step.Rank], notifier.Started);
    }

    /// <summary>
    /// A step whose attempts ran out used to be dropped, and its job hung with nobody told. It is parked now, and the
    /// job reported failed.
    /// </summary>
    [BrokerFact]
    public async Task Step_FailingOnEveryAttempt_IsParkedAndItsJobReportedFailed()
    {
        var notifier = new RecordingNotifier();
        var searching = new Searching(_ => throw new HttpRequestException("the search engine is down"));
        string url = await broker.ConnectionStringAsync();
        await using Worker worker = await Worker.StartAsync(url, _fleet, 2, searching, notifier);

        await worker.Dispatcher.StartAsync(TestWorkflow.FullSearch, Guid.CreateVersion7(), default(None));
        WorkflowFailure failure = await notifier.Failed.Task.WaitAsync(Patience);

        Assert.Equal(new WorkflowFailure("The step failed on every attempt.", nameof(Step.Search)), failure);
        Assert.Equal(1u, await ParkedAsync(url, $"{_fleet}.search.parked"));
    }

    /// <summary>
    /// A host stopping in the middle of a step must hand its message back whole, so another worker finishes the job.
    /// One attempt only: a shutdown counted as an attempt would give the step up and fail the job.
    /// </summary>
    [BrokerFact]
    public async Task Worker_StoppingMidStep_HandsTheStepToAnotherWorkerWithoutSpendingAnAttempt()
    {
        string url = await broker.ConnectionStringAsync();
        var searchBegun = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopping = new RecordingNotifier();
        var hanging = new Searching(async token =>
        {
            searchBegun.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);

            return [];
        });
        var finishing = new RecordingNotifier();

        await using Worker first = await Worker.StartAsync(url, _fleet, 1, hanging, stopping);
        await first.Dispatcher.StartAsync(TestWorkflow.FullSearch, Guid.CreateVersion7(), default(None));
        await searchBegun.Task.WaitAsync(Patience);
        await first.StopAsync();
        await using Worker second = await Worker.StartAsync(url, _fleet, 1, Searching.Finds("a document"), finishing);
        await finishing.Finished.Task.WaitAsync(Patience);

        Assert.False(stopping.Failed.Task.IsCompleted);
        Assert.False(finishing.Failed.Task.IsCompleted);
    }

    /// <summary>
    /// The same step published twice — as a producer does when its connection drops after the broker took the message
    /// — used to fork the job: both copies handed on, and the job finished twice. With a ledger the fleet shares, two
    /// workers race for the hand-over and one wins.
    /// </summary>
    [BrokerFact]
    public async Task Step_PublishedTwice_IsHandedOnOnceAcrossTwoWorkers()
    {
        string url = await broker.ConnectionStringAsync();
        var ledger = new SharedLedger();
        var one = new RecordingNotifier();
        var other = new RecordingNotifier();
        await using Worker first = await Worker.StartAsync(url, _fleet, 3, Searching.Finds("a document"), one, ledger);
        await using Worker second = await Worker.StartAsync(url, _fleet, 3, Searching.Finds("a document"), other, ledger);
        WorkflowContext<Step> searching = TestWorkflow.Context(currentStepIndex: 1) with { StartedAt = DateTime.UtcNow };
        var duplicate = new SearchCommand(searching, ["blue sky"]);

        await first.Producer.PublishAsync(duplicate);
        await first.Producer.PublishAsync(duplicate);
        await Task.WhenAny(one.Finished.Task, other.Finished.Task).WaitAsync(Patience);
        await Task.Delay(TimeSpan.FromSeconds(2));
        int ranked = one.Started.Count(step => step == Step.Rank) + other.Started.Count(step => step == Step.Rank);

        Assert.Equal(1, ranked);
        Assert.Equal(1, one.FinishedCount + other.FinishedCount);
    }

    /// <summary>
    /// A job whose step hangs — deaf to its token — used to say nothing until its next message, and here there is
    /// none. Its deadline comes back, the job is reported failed once, and the hung step's late result is dropped.
    /// </summary>
    [BrokerFact]
    public async Task Job_StuckPastItsDeadline_IsReportedFailedOnce()
    {
        string url = await broker.ConnectionStringAsync();
        var notifier = new RecordingNotifier();
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deaf = new Searching(async _ =>
        {
            await released.Task;

            return ["a late document"];
        });
        var log = new KeptLog();
        await using Worker worker = await Worker.StartAsync(
            url, _fleet, 3, deaf, notifier, new SharedLedger(), lifetime: TimeSpan.FromSeconds(2), hasDeadline: true, log: log);

        WorkflowFailure? failure;
        try
        {
            await worker.Dispatcher.StartAsync(TestWorkflow.FullSearch, default(None), CancellationToken.None);
            failure = await notifier.Failed.Task.WaitAsync(TimeSpan.FromSeconds(30)).ContinueWith(
                told => told.IsCompletedSuccessfully ? told.Result : null, TaskScheduler.Default);
        }
        finally
        {
            released.TrySetResult();
        }

        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.True(failure is not null, string.Join(Environment.NewLine, log.Lines));
        Assert.Equal("The job outlived its deadline.", failure.Reason);
        Assert.Equal(1, notifier.FailedCount);
        Assert.Equal(0, notifier.FinishedCount);
    }

    private static async Task<uint> ParkedAsync(string url, string queue)
    {
        var factory = new ConnectionFactory { Uri = new Uri(url) };
        await using IConnection connection = await factory.CreateConnectionAsync();
        await using IChannel channel = await connection.CreateChannelAsync();

        DateTime giveUp = DateTime.UtcNow + Patience;
        uint parked = await channel.MessageCountAsync(queue);
        while (parked == 0 && DateTime.UtcNow < giveUp)
        {
            await Task.Delay(100);
            parked = await channel.MessageCountAsync(queue);
        }

        return parked;
    }
}

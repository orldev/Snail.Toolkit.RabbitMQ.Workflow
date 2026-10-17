using Microsoft.Extensions.Time.Testing;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class JobCancellationTests
{
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void Token_AskedTwiceForOneJob_IsTheSameToken()
    {
        var cancellation = Cancellation();
        var job = Guid.NewGuid();

        Assert.Equal(cancellation.Token(job), cancellation.Token(job));
    }

    [Fact]
    public void Token_AskedConcurrentlyForANewJob_IsOneTokenThatCancels()
    {
        var cancellation = Cancellation();
        var job = Guid.NewGuid();
        var tokens = new CancellationToken[64];

        Parallel.For(0, tokens.Length, i => tokens[i] = cancellation.Token(job));
        cancellation.TryCancel(job);

        Assert.All(tokens, token => Assert.Equal(tokens[0], token));
        Assert.True(tokens[0].IsCancellationRequested);
    }

    [Fact]
    public void Token_ForDifferentJobs_AreIndependent()
    {
        var cancellation = Cancellation();
        var one = Guid.NewGuid();
        cancellation.Token(one);
        var other = cancellation.Token(Guid.NewGuid());

        cancellation.TryCancel(one);

        Assert.True(cancellation.Token(one).IsCancellationRequested);
        Assert.False(other.IsCancellationRequested);
    }

    [Fact]
    public void Token_PastTheJobLifetime_IsCancelled()
    {
        var cancellation = Cancellation(TimeSpan.FromMinutes(10));
        var token = cancellation.Token(Guid.NewGuid());

        _time.Advance(TimeSpan.FromMinutes(10));

        Assert.True(token.IsCancellationRequested);
    }

    [Fact]
    public void Token_WithoutALifetime_NeverExpires()
    {
        var cancellation = Cancellation(Timeout.InfiniteTimeSpan);
        var token = cancellation.Token(Guid.NewGuid());

        _time.Advance(TimeSpan.FromDays(1));

        Assert.False(token.IsCancellationRequested);
    }

    /// <summary>
    /// Cancelling used to drop the job from the registry, so the next message of that job was
    /// handed a fresh token and ran as though nothing had been cancelled.
    /// </summary>
    [Fact]
    public void Token_TakenAfterTheJobWasCancelled_IsAlreadyCancelled()
    {
        var cancellation = Cancellation();
        var job = Guid.NewGuid();
        cancellation.Token(job);

        cancellation.TryCancel(job);

        Assert.True(cancellation.Token(job).IsCancellationRequested);
    }

    [Fact]
    public void Cancel_JobThisProcessNeverSaw_SaysSo() =>
        Assert.False(Cancellation().TryCancel(Guid.NewGuid()));

    [Fact]
    public void Complete_RunningJob_ReleasesItOnce()
    {
        var cancellation = Cancellation();
        var job = Guid.NewGuid();
        cancellation.Token(job);

        Assert.True(cancellation.TryComplete(job));
        Assert.False(cancellation.TryComplete(job));
    }

    /// <summary>
    /// Completion used to dispose the source, and a message of the same job still being picked up
    /// got ObjectDisposedException out of the registry — thrown before the handler could classify it.
    /// </summary>
    [Fact]
    public void Token_TakenWhileTheJobIsBeingCompleted_DoesNotThrow()
    {
        var cancellation = Cancellation();
        var job = Guid.NewGuid();
        var token = cancellation.Token(job);

        cancellation.TryComplete(job);

        Assert.False(token.IsCancellationRequested);
        Assert.False(cancellation.Token(job).IsCancellationRequested);
    }

    private JobCancellation Cancellation(TimeSpan? lifetime = null) =>
        new(new WorkflowOptions { JobLifetime = lifetime ?? TimeSpan.FromMinutes(10) }, _time);
}

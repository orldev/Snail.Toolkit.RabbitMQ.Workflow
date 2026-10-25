using Microsoft.Extensions.Time.Testing;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

public class JobCancellationChaosTests
{
    /// <summary>
    /// The map was said to stay "bounded by the jobs actually in flight", and was not. Worker A takes the first step of
    /// every job, worker B the rest, and B releases the job when it ends. A never hears of the end, and its entry used
    /// to stay forever — the deadline only cancelled the source — at 222 B a job. A keeps receiving new jobs meanwhile,
    /// as a live worker does, since that is where it sweeps.
    /// </summary>
    [Fact]
    public void Token_JobFinishedOnAnotherWorker_IsReleasedByItsDeadline()
    {
        var time = new FakeTimeProvider();
        var options = new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) };
        var workerA = new JobCancellation(options, time);
        var workerB = new JobCancellation(options, time);
        Guid[] jobs = [.. Enumerable.Range(0, 10_000).Select(_ => Guid.NewGuid())];

        Parallel.ForEach(jobs, job =>
        {
            workerA.Token(job);
            workerB.Token(job);
            workerB.TryComplete(job);
        });
        time.Advance(TimeSpan.FromDays(1));
        workerA.Token(Guid.NewGuid());
        time.Advance(TimeSpan.FromDays(1));
        workerA.Token(Guid.NewGuid());

        int held = jobs.Count(workerA.TryCancel);

        Assert.Equal(0, held);
    }

    /// <summary>
    /// The deadline used to start when each worker first saw the job, so a step that moved to a new worker got a whole
    /// new lifetime and nothing bound the job. Measured from the job's start, a worker meeting it nine minutes in
    /// gives it the one minute left.
    /// </summary>
    [Fact]
    public void Token_JobMetLateByANewWorker_KeepsTheJobsOwnDeadline()
    {
        var time = new FakeTimeProvider();
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        DateTime startedAt = time.GetUtcNow().UtcDateTime;
        time.Advance(TimeSpan.FromMinutes(9));

        CancellationToken token = worker.Token(Guid.NewGuid(), startedAt);
        time.Advance(TimeSpan.FromMinutes(1));

        Assert.True(token.IsCancellationRequested);
    }

    [Fact]
    public void Token_JobOlderThanItsLifetime_IsCancelledOnArrival()
    {
        var time = new FakeTimeProvider();
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        DateTime startedAt = time.GetUtcNow().UtcDateTime;
        time.Advance(TimeSpan.FromMinutes(11));

        Assert.True(worker.Token(Guid.NewGuid(), startedAt).IsCancellationRequested);
    }

    /// <summary>
    /// A start ahead of this machine's clock counts as nothing elapsed; it never stretches the lifetime past the one
    /// the startup check proved a timer can hold.
    /// </summary>
    [Fact]
    public void Token_JobStartedOnAClockAheadOfThisOne_GetsNoMoreThanItsLifetime()
    {
        var time = new FakeTimeProvider();
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        DateTime startedAt = time.GetUtcNow().UtcDateTime.AddMinutes(5);

        CancellationToken token = worker.Token(Guid.NewGuid(), startedAt);
        time.Advance(TimeSpan.FromMinutes(10));

        Assert.True(token.IsCancellationRequested);
    }

    /// <summary>
    /// A cancellation broadcast to the fleet used to be forgotten by every worker that had not seen the job yet, and the
    /// job's next step, landing on one of them, ran as if nothing had been cancelled.
    /// </summary>
    [Fact]
    public void Token_ForAJobCancelledBeforeItArrived_IsCancelled()
    {
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, new FakeTimeProvider());
        var job = Guid.NewGuid();

        worker.TryCancel(job);

        Assert.True(worker.Token(job).IsCancellationRequested);
    }

    /// <summary>
    /// Remembering cancellations of jobs never seen here must not grow without end.
    /// </summary>
    [Fact]
    public void TryCancel_OfJobsNeverSeenHere_IsForgottenOnceTheSweepPassesTwice()
    {
        var time = new FakeTimeProvider();
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        Guid[] jobs = [.. Enumerable.Range(0, 1_000).Select(_ => Guid.NewGuid())];

        foreach (Guid job in jobs)
        {
            worker.TryCancel(job);
        }

        time.Advance(TimeSpan.FromHours(1));
        worker.Token(Guid.NewGuid());
        time.Advance(TimeSpan.FromHours(1));
        worker.Token(Guid.NewGuid());

        Assert.DoesNotContain(jobs, job => worker.Token(job).IsCancellationRequested);
    }

    /// <summary>
    /// The sweep must not undo the earlier fix: a job cancelled here stays cancelled for a later message of it.
    /// </summary>
    [Fact]
    public void Token_TakenWithinALifetimeOfTheCancellation_IsStillCancelled()
    {
        var time = new FakeTimeProvider();
        var worker = new JobCancellation(new WorkflowOptions { JobLifetime = TimeSpan.FromMinutes(10) }, time);
        var job = Guid.NewGuid();
        worker.Token(job);
        worker.TryCancel(job);

        time.Advance(TimeSpan.FromMinutes(10));
        worker.Token(Guid.NewGuid());

        Assert.True(worker.Token(job).IsCancellationRequested);
    }
}

using System.Collections.Concurrent;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Keeps one cancellation source per job for as long as the job is running in this process.
/// </summary>
/// <remarks>
/// Two things here are deliberate and were both bugs before.
/// <para>
/// The sources are never disposed. Disposing on completion raced with a message of the same job
/// still being picked up, and the loser of that race got an <see cref="ObjectDisposedException"/>
/// out of <see cref="Token"/> — thrown before the handler's own error handling could see it. A
/// cancelled-but-undisposed source answers that same call with an already-cancelled token, which
/// is exactly what the caller should act on. The cost is one timer per finished job until its
/// deadline elapses; the entry itself is removed immediately, so the map stays bounded by the
/// jobs actually in flight.
/// </para>
/// <para>
/// Two messages of a job this process has not seen yet may both build a source for it, and only
/// one of them enters the map. The other was never handed out, so it is disposed on the spot:
/// left alone, it would arm a timer nobody would ever release.
/// </para>
/// </remarks>
public sealed class JobCancellation(WorkflowOptions options, TimeProvider time) : IJobCancellation
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _jobs = new();

    /// <inheritdoc/>
    public CancellationToken Token(Guid jobId)
    {
        if (_jobs.TryGetValue(jobId, out var running))
            return running.Token;

        var created = Deadline();
        running = _jobs.GetOrAdd(jobId, created);

        if (!ReferenceEquals(running, created))
            created.Dispose();

        return running.Token;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The entry stays behind, cancelled, so that another message of the same job picked up after
    /// the cancellation sees a cancelled token instead of a fresh one. Only <see cref="TryComplete"/>
    /// releases it. A cancellation that arrives before this process has seen the job at all is
    /// answered with false and forgotten, because remembering it would mean holding an entry for
    /// every job cancelled anywhere in the fleet.
    /// </remarks>
    public bool TryCancel(Guid jobId)
    {
        if (!_jobs.TryGetValue(jobId, out var source))
            return false;

        source.Cancel();
        return true;
    }

    /// <inheritdoc/>
    public bool TryComplete(Guid jobId) => _jobs.TryRemove(jobId, out _);

    private CancellationTokenSource Deadline() =>
        options.JobLifetime == Timeout.InfiniteTimeSpan
            ? new CancellationTokenSource()
            : new CancellationTokenSource(options.JobLifetime, time);
}

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
/// The source is built through <see cref="Lazy{T}"/> because the factory of a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> may run more than once under contention, and
/// every extra run would arm a timer nobody would ever release.
/// </para>
/// </remarks>
public sealed class JobCancellation(WorkflowOptions options, TimeProvider time) : IJobCancellation
{
    private readonly ConcurrentDictionary<Guid, Lazy<CancellationTokenSource>> _jobs = new();

    /// <inheritdoc/>
    public CancellationToken Token(Guid jobId) =>
        _jobs.GetOrAdd(jobId, _ => new Lazy<CancellationTokenSource>(Deadline)).Value.Token;

    /// <inheritdoc/>
    /// <remarks>
    /// The entry stays behind, cancelled, so that another message of the same job picked up after
    /// the cancellation sees a cancelled token instead of a fresh one. Only <see cref="Complete"/>
    /// releases it. A cancellation that arrives before this process has seen the job at all is
    /// answered with false and forgotten, because remembering it would mean holding an entry for
    /// every job cancelled anywhere in the fleet.
    /// </remarks>
    public bool Cancel(Guid jobId)
    {
        if (!_jobs.TryGetValue(jobId, out var source))
            return false;

        source.Value.Cancel();
        return true;
    }

    /// <inheritdoc/>
    public bool Complete(Guid jobId) => _jobs.TryRemove(jobId, out _);

    private CancellationTokenSource Deadline() =>
        options.JobLifetime == Timeout.InfiniteTimeSpan
            ? new CancellationTokenSource()
            : new CancellationTokenSource(options.JobLifetime, time);
}

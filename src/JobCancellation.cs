using System.Collections.Concurrent;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Keeps one cancellation source per job for as long as the job is running in this process.
/// </summary>
/// <remarks>
/// Three things here are deliberate, and each was a bug before.
/// <para>
/// The sources are never disposed. Disposing on completion raced with a message of the same job
/// still being picked up, and the loser of that race got an <see cref="ObjectDisposedException"/>
/// out of <see cref="Token"/> — thrown before the handler's own error handling could see it.
/// </para>
/// <para>
/// Two messages of a job this process has not seen yet may both build a source for it, and only
/// one of them enters the map. The other was never handed out, so it is disposed on the spot.
/// </para>
/// <para>
/// <see cref="TryComplete"/> releases a job only on the worker that ended it; every other worker it
/// passed through never hears of the end, and held its entry forever (222 B each, measured
/// 2026-10-21). So an entry whose source is cancelled — by its deadline or by <see cref="TryCancel"/> —
/// is dropped once two sweeps, a job lifetime apart, have both seen it cancelled: long enough for a
/// late message of the job to still find it cancelled. The sweep runs on the first message of a new
/// job, never on a known one, and costs nothing per job. Every job has a deadline, so every entry goes.
/// </para>
/// </remarks>
public sealed class JobCancellation(WorkflowOptions options, TimeProvider time) : IJobCancellation
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _jobs = new();
    private HashSet<Guid> _cancelledAtLastSweep = [];
    private long _sweptAt = time.GetTimestamp();

    /// <inheritdoc/>
    public CancellationToken Token(Guid jobId, DateTime startedAt = default)
    {
        if (_jobs.TryGetValue(jobId, out CancellationTokenSource? running))
            return running.Token;

        SweepWhenDue();

        CancellationTokenSource created = Deadline(startedAt);
        running = _jobs.GetOrAdd(jobId, created);

        if (!ReferenceEquals(running, created))
            created.Dispose();

        return running.Token;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The entry stays behind, cancelled, for at least one sweep period, so that another message of
    /// the same job picked up after the cancellation sees a cancelled token instead of a fresh one.
    /// A cancellation of a job this process has not seen yet is answered with false and remembered
    /// the same way: it used to be forgotten, and the job's next step, landing here, ran as if nothing
    /// had been cancelled. The sweep forgets it again, so only the cancellations of the last period or
    /// two are held — a handful, where the jobs themselves run by the thousand.
    /// </remarks>
    public bool TryCancel(Guid jobId)
    {
        if (_jobs.TryGetValue(jobId, out CancellationTokenSource? source))
        {
            source.Cancel();
            return true;
        }

        Remember(jobId);

        return false;
    }

    /// <inheritdoc/>
    public bool TryComplete(Guid jobId) => _jobs.TryRemove(jobId, out _);

    private CancellationTokenSource Deadline(DateTime startedAt)
    {
        TimeSpan remaining = options.JobLifetime - Elapsed(startedAt);
        if (remaining > TimeSpan.Zero)
            return new CancellationTokenSource(remaining, time);

        var expired = new CancellationTokenSource();
        expired.Cancel();

        return expired;
    }

    /// <remarks>
    /// A start in the future — the starting machine's clock ahead of this one — counts as no time elapsed, so the
    /// remaining lifetime never exceeds the lifetime the startup check proved a timer can hold.
    /// </remarks>
    private TimeSpan Elapsed(DateTime startedAt)
    {
        if (startedAt == default)
            return TimeSpan.Zero;

        TimeSpan elapsed = time.GetUtcNow().UtcDateTime - startedAt;

        return elapsed > TimeSpan.Zero ? elapsed : TimeSpan.Zero;
    }

    private void Remember(Guid jobId)
    {
        SweepWhenDue();

        var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        CancellationTokenSource kept = _jobs.GetOrAdd(jobId, cancelled);
        if (ReferenceEquals(kept, cancelled))
            return;

        cancelled.Dispose();
        kept.Cancel();
    }

    private void SweepWhenDue()
    {
        long sweptAt = Volatile.Read(ref _sweptAt);
        if (time.GetElapsedTime(sweptAt) < options.JobLifetime)
            return;

        if (Interlocked.CompareExchange(ref _sweptAt, time.GetTimestamp(), sweptAt) != sweptAt)
            return;

        Sweep();
    }

    private void Sweep()
    {
        HashSet<Guid> cancelled = [];

        foreach ((Guid jobId, CancellationTokenSource source) in _jobs)
        {
            if (!source.IsCancellationRequested)
                continue;

            if (_cancelledAtLastSweep.Contains(jobId))
                _jobs.TryRemove(KeyValuePair.Create(jobId, source));
            else
                cancelled.Add(jobId);
        }

        _cancelledAtLastSweep = cancelled;
    }
}

using System.Collections.Concurrent;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The ledger used when an application registers none: marks kept in this process.
/// </summary>
/// <remarks>
/// One record per job, and a compare-and-swap per mark, so a step handed on costs no allocation once its job is known
/// here. A plan is a line, so the hand-overs of a job are one number — the last step handed on, −1 for its start, −2
/// for nothing yet — and a step at or before it is a duplicate. Records outlive the time they were kept for until a
/// sweep, run at most once a minute on the first mark of a new job, removes them.
/// </remarks>
internal sealed class LocalLedger(TimeProvider time) : IWorkflowLedger
{
    private static readonly TimeSpan SweepPeriod = TimeSpan.FromMinutes(1);

    private readonly ConcurrentDictionary<Guid, Marks> _jobs = new();
    private long _sweptAt = time.GetTimestamp();

    public bool IsShared => false;

    public ValueTask<bool> TryHandOverAsync(
        Guid jobId,
        int stepIndex,
        TimeSpan keepFor,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(Marked(jobId, keepFor).TryHandOver(stepIndex));

    public ValueTask ReleaseHandOverAsync(Guid jobId, int stepIndex, CancellationToken cancellationToken = default)
    {
        if (_jobs.TryGetValue(jobId, out Marks? marks))
            marks.Release(stepIndex);

        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TryEndAsync(Guid jobId, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(Marked(jobId, keepFor).TryEnd());

    private Marks Marked(Guid jobId, TimeSpan keepFor)
    {
        if (_jobs.TryGetValue(jobId, out Marks? known))
            return known;

        SweepWhenDue();

        long expiresAt = time.GetTimestamp() + (long)(keepFor.TotalSeconds * time.TimestampFrequency);

        return _jobs.GetOrAdd(jobId, new Marks(expiresAt));
    }

    private void SweepWhenDue()
    {
        long sweptAt = Volatile.Read(ref _sweptAt);
        if (time.GetElapsedTime(sweptAt) < SweepPeriod)
            return;

        long now = time.GetTimestamp();
        if (Interlocked.CompareExchange(ref _sweptAt, now, sweptAt) != sweptAt)
            return;

        foreach ((Guid jobId, Marks marks) in _jobs)
        {
            if (marks.ExpiresAt <= now)
                _jobs.TryRemove(KeyValuePair.Create(jobId, marks));
        }
    }

    private sealed class Marks(long expiresAt)
    {
        private int _handedOver = -2;
        private int _isEnded;

        public long ExpiresAt { get; } = expiresAt;

        public bool TryEnd() => Interlocked.Exchange(ref _isEnded, 1) == 0;

        public bool TryHandOver(int stepIndex)
        {
            while (Volatile.Read(ref _isEnded) == 0)
            {
                int handedOver = Volatile.Read(ref _handedOver);
                if (handedOver >= stepIndex)
                    return false;

                if (Interlocked.CompareExchange(ref _handedOver, stepIndex, handedOver) == handedOver)
                    return true;
            }

            return false;
        }

        public void Release(int stepIndex) => Interlocked.CompareExchange(ref _handedOver, stepIndex - 1, stepIndex);
    }
}

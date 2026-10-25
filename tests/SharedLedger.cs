namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

/// <summary>
/// A ledger the tests' workers share by holding one instance, as a fleet shares one in Redis.
/// </summary>
internal sealed class SharedLedger(TimeProvider time) : IWorkflowLedger
{
    private readonly LocalLedger _marks = new(time);

    public SharedLedger()
        : this(TimeProvider.System)
    {
    }

    public bool IsShared => true;

    public ValueTask<bool> TryHandOverAsync(Guid jobId, int stepIndex, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
        _marks.TryHandOverAsync(jobId, stepIndex, keepFor, cancellationToken);

    public ValueTask ReleaseHandOverAsync(Guid jobId, int stepIndex, CancellationToken cancellationToken = default) =>
        _marks.ReleaseHandOverAsync(jobId, stepIndex, cancellationToken);

    public ValueTask<bool> TryEndAsync(Guid jobId, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
        _marks.TryEndAsync(jobId, keepFor, cancellationToken);
}

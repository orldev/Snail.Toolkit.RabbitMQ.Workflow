using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>
/// A job's cancellation: the token of a job this process meets for the first time and its release, as every step that
/// lands on another worker of the fleet pays it, and the token of a job it already runs.
/// </summary>
/// <remarks>
/// Every job has a deadline. The timers here are ones that never fire and cost nothing, so no timer outlives an
/// operation and the numbers are the registry's own, not the system timer queue's.
/// </remarks>
[MemoryDiagnoser]
public class Cancelling
{
    private static readonly Guid Running = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

    private readonly JobCancellation _cancellation = new(new WorkflowOptions(), new StillTime());

    private int _met;

    [GlobalSetup]
    public void Setup() => _cancellation.Token(Running);

    [Benchmark]
    public CancellationToken NewJob()
    {
        var job = new Guid(++_met, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var token = _cancellation.Token(job);
        _cancellation.TryComplete(job);

        return token;
    }

    [Benchmark]
    public CancellationToken KnownJob() => _cancellation.Token(Running);
}

/// <summary>A clock whose timers never fire: one shared timer that does nothing.</summary>
internal sealed class StillTime : TimeProvider
{
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) => NoTimer.Instance;

    private sealed class NoTimer : ITimer
    {
        public static readonly NoTimer Instance = new();

        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

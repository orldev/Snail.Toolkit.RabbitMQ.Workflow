using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>
/// A job's cancellation: the token of a job this process meets for the first time and its release, as every step that
/// lands on another worker of the fleet pays it, and the token of a job it already runs. Jobs here have no deadline, so
/// no timer outlives an operation.
/// </summary>
[MemoryDiagnoser]
public class Cancelling
{
    private static readonly Guid Running = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

    private readonly JobCancellation _cancellation = new(new WorkflowOptions { JobLifetime = Timeout.InfiniteTimeSpan }, TimeProvider.System);

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

using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>
/// One step hand-over: a step's result checked against the blueprint and published to the next step, and one whole
/// step as a consumer runs it — the job's token, the linked token, the work, the hand-over.
/// </summary>
[MemoryDiagnoser]
public class Dispatching
{
    private ServiceProvider _services = null!;

    private IWorkflowDispatcher<Step> _dispatcher = null!;

    private RankStep _rank = null!;

    private WorkflowContext<Step> _searched = null!;

    private RankCommand _command = null!;

    [GlobalSetup]
    public void Setup()
    {
        _services = Search.Services();
        _dispatcher = _services.GetRequiredService<IWorkflowDispatcher<Step>>();
        _rank = new RankStep(_services.GetRequiredService<IJobCancellation>(), _dispatcher, _services.GetRequiredService<ILoggerFactory>());
        _searched = Search.Context(0);
        _command = new RankCommand(Search.Context(1), "found 12 documents");
    }

    [GlobalCleanup]
    public void Cleanup() => _services.Dispose();

    [Benchmark(Baseline = true)]
    public ValueTask HandOver() => _dispatcher.DispatchAsync(_searched, "found 12 documents");

    [Benchmark]
    public ValueTask WholeStep() => _rank.HandleAsync(_command, CancellationToken.None);

    /// <summary>The ranking step: its work is a completed task, so the timing is the workflow's own.</summary>
    private sealed class RankStep(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher, ILoggerFactory loggerFactory)
        : WorkflowConsumer<RankCommand, Step, string>(cancellation, dispatcher, loggerFactory)
    {
        private static readonly Task<string> Ranked = Task.FromResult("ranked 12 documents");

        protected override Task<string> ProcessAsync(RankCommand message, CancellationToken cancellationToken) => Ranked;
    }
}

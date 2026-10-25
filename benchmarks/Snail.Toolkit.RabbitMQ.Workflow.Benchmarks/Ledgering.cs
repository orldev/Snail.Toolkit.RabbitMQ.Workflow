using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>
/// The ledger kept in the process, as an application without one of its own gets it: the first mark of a job this
/// process has not seen — what every step landing on another worker of the fleet pays — and the next hand-over of a
/// job it knows.
/// </summary>
[MemoryDiagnoser]
public class Ledgering
{
    private static readonly Guid Known = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

    private static readonly TimeSpan KeepFor = TimeSpan.FromMinutes(20);

    private ServiceProvider _services = null!;

    private IWorkflowLedger _ledger = null!;

    private int _met;

    private int _handed;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Information));
        services.AddSingleton<IRabbitProducer, CountingProducer>();
        services.AddWorkflow<Step>(workflow => workflow
            .Step<SearchCommand, None, string>(Step.Search, static (context, _) => new SearchCommand(context))
            .Plan("search", Step.Search));

        _services = services.BuildServiceProvider();
        _ledger = _services.GetRequiredService<IWorkflowLedger>();
    }

    [GlobalCleanup]
    public void Cleanup() => _services.Dispose();

    [Benchmark]
    public ValueTask<bool> IsNewJobHandedOver() => _ledger.TryHandOverAsync(new Guid(++_met, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 0, KeepFor);

    [Benchmark]
    public ValueTask<bool> IsKnownJobHandedOver() => _ledger.TryHandOverAsync(Known, ++_handed, KeepFor);
}

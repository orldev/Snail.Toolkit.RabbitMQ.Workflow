using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>The steps of a count: tally the documents, report the number.</summary>
public enum Tally
{
    Count,
    Report
}

/// <summary>The message of the step that reports, carrying the number counted.</summary>
public sealed record ReportCommand(WorkflowContext<Tally> Context, int Documents) : IWorkflowCommand<Tally>;

/// <summary>A step hand-over whose result is a value type: the number a counting step returns.</summary>
[MemoryDiagnoser]
public class Counting
{
    private ServiceProvider _services = null!;

    private IWorkflowDispatcher<Tally> _dispatcher = null!;

    private WorkflowContext<Tally> _counted = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Information));
        services.AddSingleton<IRabbitProducer, CountingProducer>();
        services.AddSingleton<IWorkflowLedger, AcceptingLedger>();
        services.AddWorkflow<Tally>(workflow => workflow
            .Step<CountCommand, None, int>(Tally.Count, static (context, _) => new CountCommand(context))
            .Step<ReportCommand, int, string>(Tally.Report, static (context, documents) => new ReportCommand(context, documents))
            .Plan("count", Tally.Count, Tally.Report));

        _services = services.BuildServiceProvider();
        _dispatcher = _services.GetRequiredService<IWorkflowDispatcher<Tally>>();
        _counted = new WorkflowContext<Tally>(Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"), [Tally.Count, Tally.Report], 0);
    }

    [GlobalCleanup]
    public void Cleanup() => _services.Dispose();

    [Benchmark]
    public ValueTask HandOverNumber() => _dispatcher.DispatchAsync(_counted, 12);
}

/// <summary>The message of the step that counts.</summary>
public sealed record CountCommand(WorkflowContext<Tally> Context) : IWorkflowCommand<Tally>;

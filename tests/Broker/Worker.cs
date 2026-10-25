using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Broker;

/// <summary>
/// One worker of a fleet: the test workflow's three steps over a real broker, started the way a host starts them.
/// </summary>
internal sealed class Worker : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly IReadOnlyList<IHostedService> _hosted;
    private bool _isStopped;

    private Worker(ServiceProvider provider)
    {
        _provider = provider;
        _hosted = [.. provider.GetServices<IHostedService>()];
    }

    public IWorkflowDispatcher<Step> Dispatcher => _provider.GetRequiredService<IWorkflowDispatcher<Step>>();

    public IRabbitProducer Producer => _provider.GetRequiredService<IRabbitProducer>();

    public static async Task<Worker> StartAsync(
        string url,
        string fleet,
        int maxAttempts,
        Searching searching,
        RecordingNotifier notifier,
        IWorkflowLedger? ledger = null,
        TimeSpan? lifetime = null,
        bool hasDeadline = false,
        KeptLog? log = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(logging =>
        {
            if (log is not null)
                logging.AddProvider(log);
        });
        services.AddSingleton(searching);
        services.AddSingleton<IWorkflowNotifier<Step>>(notifier);
        if (ledger is not null)
            services.AddSingleton(ledger);

        services.AddRabbit(o => o.UseConnectionUrl(url), rabbit => rabbit.AddConnection(fleet, c => Topology(c, fleet, maxAttempts, hasDeadline)));
        services.AddWorkflow<Step>(workflow => Declare(workflow, lifetime ?? TimeSpan.FromMinutes(10)));

        var worker = new Worker(services.BuildServiceProvider());
        await worker.StartAsync();

        return worker;
    }

    public async Task StopAsync()
    {
        if (_isStopped)
            return;

        _isStopped = true;

        foreach (IHostedService service in _hosted.Reverse())
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _provider.DisposeAsync();
    }

    private async Task StartAsync()
    {
        foreach (IHostedLifecycleService service in _hosted.OfType<IHostedLifecycleService>())
        {
            await service.StartingAsync(CancellationToken.None);
        }

        foreach (IHostedService service in _hosted)
        {
            await service.StartAsync(CancellationToken.None);
        }
    }

    private static void Declare(WorkflowBlueprint<Step> workflow, TimeSpan lifetime)
    {
        workflow.Options.JobLifetime = lifetime;
        DeclareSteps(workflow);
    }

    private static void DeclareSteps(WorkflowBlueprint<Step> workflow) => workflow
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
        .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(Step.Search, (context, terms) => new SearchCommand(context, terms))
        .Step<RankCommand, IReadOnlyList<string>, string>(Step.Rank, (context, documents) => new RankCommand(context, documents))
        .Plan(TestWorkflow.FullSearch, Step.Expand, Step.Search, Step.Rank);

    private static void Topology(IRabbitConnectionBuilder connection, string fleet, int maxAttempts, bool hasDeadline)
    {
        Carry<ExpandCommand, ExpandStep>(connection, $"{fleet}.expand", maxAttempts);
        Carry<SearchCommand, SearchStep>(connection, $"{fleet}.search", maxAttempts);
        Carry<RankCommand, RankStep>(connection, $"{fleet}.rank", maxAttempts);

        if (hasDeadline)
            connection.AddWorkflowDeadline<Step>($"{fleet}.deadline");
    }

    private static void Carry<TCommand, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(
        IRabbitConnectionBuilder connection,
        string queue,
        int maxAttempts)
        where TConsumer : class, IConsumer<TCommand>
    {
        connection.AddConsumer(connection.AddQueue<TCommand>(queue).AsWorkflowStep(TimeSpan.FromMilliseconds(200), maxAttempts))
            .Prefetch(1)
            .Subscribe<TCommand, TConsumer>();
        connection.AddProducer<TCommand>().RoutedTo(queue);
    }
}

/// <summary>
/// What the search step does, set by each test.
/// </summary>
internal sealed class Searching(Func<CancellationToken, Task<IReadOnlyList<string>>> work)
{
    public Func<CancellationToken, Task<IReadOnlyList<string>>> Work { get; } = work;

    public static Searching Finds(params string[] documents) => new(_ => Task.FromResult<IReadOnlyList<string>>(documents));
}

/// <summary>
/// Remembers what the workflow said about its jobs.
/// </summary>
internal sealed class RecordingNotifier : IWorkflowNotifier<Step>
{
    private int _finished;
    private int _failed;

    public ConcurrentQueue<Step> Started { get; } = [];

    public TaskCompletionSource<WorkflowContext<Step>> Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource<WorkflowFailure> Failed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int FinishedCount => _finished;

    public int FailedCount => _failed;

    public ValueTask StepStartedAsync(WorkflowContext<Step> context, CancellationToken cancellationToken = default)
    {
        Started.Enqueue(context.CurrentStep);

        return ValueTask.CompletedTask;
    }

    public ValueTask FinishedAsync(WorkflowContext<Step> context, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _finished);
        Finished.TrySetResult(context);

        return ValueTask.CompletedTask;
    }

    public ValueTask FailedAsync(WorkflowContext<Step> context, WorkflowFailure failure, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _failed);
        Failed.TrySetResult(failure);

        return ValueTask.CompletedTask;
    }
}

internal sealed class ExpandStep(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher, ILoggerFactory loggerFactory)
    : WorkflowConsumer<ExpandCommand, Step, IReadOnlyList<string>>(cancellation, dispatcher, loggerFactory)
{
    protected override Task<IReadOnlyList<string>> ProcessAsync(ExpandCommand message, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>(["blue sky"]);
}

internal sealed class SearchStep(
    Searching searching,
    IJobCancellation cancellation,
    IWorkflowDispatcher<Step> dispatcher,
    ILoggerFactory loggerFactory)
    : WorkflowConsumer<SearchCommand, Step, IReadOnlyList<string>>(cancellation, dispatcher, loggerFactory)
{
    protected override Task<IReadOnlyList<string>> ProcessAsync(SearchCommand message, CancellationToken cancellationToken) =>
        searching.Work(cancellationToken);
}

internal sealed class RankStep(IJobCancellation cancellation, IWorkflowDispatcher<Step> dispatcher, ILoggerFactory loggerFactory)
    : WorkflowConsumer<RankCommand, Step, string>(cancellation, dispatcher, loggerFactory)
{
    protected override Task<string> ProcessAsync(RankCommand message, CancellationToken cancellationToken) =>
        Task.FromResult($"ranked {message.Documents.Count}");
}

/// <summary>
/// Keeps what a worker logs at warning and above, so a broker test that fails can say why.
/// </summary>
internal sealed class KeptLog : ILoggerProvider
{
    public ConcurrentQueue<string> Lines { get; } = [];

    public ILogger CreateLogger(string categoryName) => new Writer(this, categoryName);

    public void Dispose()
    {
    }

    private sealed class Writer(KeptLog log, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                log.Lines.Enqueue($"{logLevel} {category}: {formatter(state, exception)} {exception?.GetType().Name} {exception?.Message}");
        }
    }
}

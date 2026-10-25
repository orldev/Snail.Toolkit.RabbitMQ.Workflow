# Snail.Toolkit.RabbitMQ.Workflow

Step-by-step workflows over RabbitMQ queues, on top of
[Snail.Toolkit.RabbitMQ](https://github.com/orldev/Snail.Toolkit.RabbitMQ).

A job follows a plan of steps. Each step is one message on one queue, handled by one consumer,
which returns a result that becomes the next step's input. Every step declares what it accepts
and what it produces, and the application refuses to start if a plan pipes a step into one that
cannot accept what the previous one makes.

## What it gives you

- **A typed chain.** `Step<TCommand, TIn, TOut>` states, in one place, the message a step runs on,
  the payload it takes and the payload it returns. The compiler binds the payload to the factory
  that consumes it; startup walks every declared plan and proves the chain fits.
- **A dispatcher that ships.** Publishing the next step is the library's job, not a hundred lines
  of pattern matching in each application.
- **Failure with one meaning.** A failing step is either transient, and the broker retries it, or
  final, and the job ends. Never both. A step that fails on every attempt is parked and its job
  reported failed, not left hanging.
- **Cancellation that is not shutdown.** A cancelled job is acknowledged and ended, even when its
  step never looks at the token; a host stopping hands its message back to the broker.
- **Progress that cannot break a job.** The notifier is told after the fact, and a notifier that
  fails is logged, never a reason to run a step again.
- **One job, whatever the broker repeats.** A step delivered twice is handed on once, and a job
  ends — and is reported — once: the job ledger is asked first.
- **A deadline per job**, measured from its start on every worker and reported even when it passes
  between two steps, and one registration entry point that cannot be wired up wrongly.
- **A host that will not start wrong.** A step queue that does not retry, or a consumer returning
  another type than its step produces, stops the host before any consumer runs.

## Declaring a workflow

```csharp
public enum Step { Expand, Search, Rank }

public sealed record ExpandCommand(WorkflowContext<Step> Context, string Query) : IWorkflowCommand<Step>;
public sealed record SearchCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Terms) : IWorkflowCommand<Step>;
public sealed record RankCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Documents) : IWorkflowCommand<Step>;

services.AddWorkflow<Step>(workflow =>
{
    workflow.Options.JobLifetime = TimeSpan.FromMinutes(10);

    workflow
        .Step<ExpandCommand, string, IReadOnlyList<string>>(Step.Expand, (context, query) => new ExpandCommand(context, query))
        .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(Step.Search, (context, terms) => new SearchCommand(context, terms))
        .Step<RankCommand, IReadOnlyList<string>, string>(Step.Rank, (context, documents) => new RankCommand(context, documents))
        .Plan("full-search", Step.Expand, Step.Search, Step.Rank);
});
```

Swap the last two `Step` declarations around and the application will not start:

```
Plan 'full-search' pipes step 'Search' into 'Rank', but 'Search' produces
'IReadOnlyList`1' and 'Rank' accepts 'String'.
```

## Writing a step

A consumer returns its result. It does not publish anything, so it can no longer leave a job
hanging by forgetting to.

```csharp
public sealed class SearchConsumer(
    SearchEngine engine,
    IJobCancellation cancellation,
    IWorkflowDispatcher<Step> dispatcher,
    ILoggerFactory loggerFactory)
    : WorkflowConsumer<SearchCommand, Step, IReadOnlyList<string>>(cancellation, dispatcher, loggerFactory)
{
    protected override async Task<IReadOnlyList<string>> ProcessAsync(
        SearchCommand message, CancellationToken cancellationToken)
    {
        var documents = await engine.SearchAsync(message.Terms, cancellationToken);

        return documents.Count > 0
            ? documents
            : throw new WorkflowFailureException("Nothing matched the query.");
    }
}
```

Anything else the step throws is treated as transient and handed back to the broker. Override
`CanRetry` to say otherwise.

## Topology

A workflow step's queue **must** be able to bring a failed message back, because that is what the
consumer assumes when it rethrows:

```csharp
connection.AddQueue<SearchCommand>("q.search").AsWorkflowStep(TimeSpan.FromSeconds(30), maxAttempts: 5);
connection.AddConsumer(queue).Prefetch(1).Subscribe<SearchCommand, SearchConsumer>();
connection.AddProducer<SearchCommand>(exchange).RoutedTo(queue).Persistent();
```

On a queue with neither retry nor a dead letter exchange, a rejected message is discarded
silently and its job hangs forever, so a host whose step queue does not retry refuses to start —
`AsWorkflowStep` is the one way to get it right. A step whose attempts run out is parked in
`<queue>.parked` (or the queue named by `parkTo`), kept for inspection and replay, and its job is
reported failed: the toolkit tells the consumer when it gives a message up.

## Starting and cancelling a job

```csharp
Guid jobId = await dispatcher.StartAsync("full-search", query, cancellationToken);
```

The job gets an id of its own, a `Guid.CreateVersion7()`, and the call answers with it. To tie a job
to something that already has an id — a request, a record — pass that id instead:
`StartAsync("full-search", requestId, query, cancellationToken)`; a job id names one job, and an id
the ledger has seen started is refused.

The payload is what the plan's first step accepts — `default(None)` when it accepts nothing — and
this call checks it before anything is published. The context carries where a job is, never what
it carries: data a later step needs reaches it through the payloads of the steps before it.

Cancellation is local to a process. To cancel a job running anywhere in the fleet, broadcast over
a fanout exchange with one queue per instance and call `IJobCancellation.TryCancel` in that consumer:

```csharp
public sealed class CancelConsumer(IJobCancellation cancellation) : IConsumer<CancelJob>
{
    public ValueTask HandleAsync(CancelJob message, CancellationToken cancellationToken)
    {
        cancellation.TryCancel(message.JobId);
        return ValueTask.CompletedTask;
    }
}
```

Report progress to whoever is waiting by registering an `IWorkflowNotifier<TStep>` before
`AddWorkflow`; without one, the workflow says nothing.

A cancellation may reach a worker before the job does; the worker remembers it, and the job's
first step there finds it cancelled.

Every job has a deadline, `JobLifetime`, ten minutes unless set — at most
`WorkflowOptions.LongestJobLifetime`, about 49.7 days. It is measured from the job's start on every
worker, and a worker that ran one step of a job, never hearing that it ended on another one,
forgets the job once its deadline has passed.

## A fleet: the job ledger

The broker delivers at least once. A step redelivered after a crash, or published twice when a
connection dropped, would run its step twice and hand the job on twice. The job ledger,
`IWorkflowLedger`, keeps two kinds of mark — a step handed on (the start is step −1), and a job
ended — each set once, atomically, and forgotten after two lifetimes. It holds no payload and no
history, and resumes nothing.

Without one of your own, the ledger is kept in the process, which catches what lands twice on one
worker. A fleet registers one every worker reads, before `AddWorkflow` — Redis, for instance:

```csharp
public sealed class RedisLedger(IConnectionMultiplexer redis) : IWorkflowLedger
{
    public bool IsShared => true;

    public async ValueTask<bool> TryHandOverAsync(Guid jobId, int stepIndex, TimeSpan keepFor, CancellationToken cancellationToken = default)
    {
        IDatabase marks = redis.GetDatabase();

        return !await marks.KeyExistsAsync($"workflow:{jobId:N}:ended")
            && await marks.StringSetAsync($"workflow:{jobId:N}:step:{stepIndex}", 1, keepFor, When.NotExists);
    }

    public async ValueTask ReleaseHandOverAsync(Guid jobId, int stepIndex, CancellationToken cancellationToken = default) =>
        await redis.GetDatabase().KeyDeleteAsync($"workflow:{jobId:N}:step:{stepIndex}");

    public async ValueTask<bool> TryEndAsync(Guid jobId, TimeSpan keepFor, CancellationToken cancellationToken = default) =>
        await redis.GetDatabase().StringSetAsync($"workflow:{jobId:N}:ended", 1, keepFor, When.NotExists);
}

services.AddSingleton<IWorkflowLedger, RedisLedger>();
services.AddWorkflow<Step>(workflow => ...);
```

The ledger makes a job's *hand-overs* happen once, not its *work*: two copies of a step delivered
to two workers at once both do the step's work, and only one hands it on.

## A deadline between steps

A step stops at its job's deadline wherever it runs, but a deadline that passes while no step runs
— the next message lost, or a step that never returns — has no one to notice it. Ask for a deadline
message, and every job publishes one as it starts; it waits out the lifetime in the broker and comes
back to end the job, unless the ledger says it ended already:

```csharp
connection.AddWorkflowDeadline<Step>("q.deadline");
```

It declares `q.deadline.waiting`, where deadlines wait, and `q.deadline`, where one worker ends the
job and reports it failed with "The job outlived its deadline." A deadline needs a ledger the fleet
shares — only that one can tell a job that ended on another worker from one still running — and a
host that asks for one without it refuses to start.

## Known limits

State lives in the message and nowhere else. That has consequences worth knowing before you rely
on it:

- **A job cannot be resumed or inspected.** There is no journal; lose the message and the job is
  gone with no record of it.
- **A step's work may run twice.** A retry, a redelivery after a crash, or two copies on two
  workers run the step again; the ledger keeps the job from forking, not the work from repeating.
  Make the work repeatable, or deduplicate it yourself.
- **Without a shared ledger, a fleet keeps only half the promise.** The ledger kept in one process
  catches duplicates that land on that process, and no deadline can be asked for.
- **The deadline is read off the fleet's clocks.** It is measured from the job's start, stamped by
  the process that started it, so clocks that disagree by more than a small part of `JobLifetime`
  move it by as much.

Resuming and inspecting jobs means moving job state into durable storage, which is a different
library.

## Requirements

.NET 10, Snail.Toolkit.RabbitMQ 2.1.0 or later: from 2.0 a delivery cancelled by a stopping host
goes back to its queue whole, without spending a retry attempt, which is what makes shutdown free;
2.1 tells a consumer when its message is given up, and lets the host's topology be read at startup.

## Licence

MIT.

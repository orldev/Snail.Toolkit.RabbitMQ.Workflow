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
  final, and the job ends. Never both.
- **Cancellation that is not shutdown.** A cancelled job is acknowledged and ended; a host stopping
  hands its message back to the broker.
- **A deadline per job**, and one registration entry point that cannot be wired up wrongly.

## Declaring a workflow

```csharp
public enum Step { Expand, Search, Rank }

public sealed record ExpandCommand(WorkflowContext<Step> Context) : IWorkflowCommand<Step>;
public sealed record SearchCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Terms) : IWorkflowCommand<Step>;
public sealed record RankCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Documents) : IWorkflowCommand<Step>;

services.AddWorkflow<Step>(workflow =>
{
    workflow.Options.JobLifetime = TimeSpan.FromMinutes(10);

    workflow
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
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
silently and its job hangs forever. The library cannot check this for you — the toolkit keeps its
queue declarations internal — so `AsWorkflowStep` is the one obvious way to get it right.

## Starting and cancelling a job

```csharp
await dispatcher.StartAsync("full-search", jobId, query, cancellationToken);
```

Cancellation is local to a process. To cancel a job running anywhere in the fleet, broadcast over
a fanout exchange with one queue per instance and call `IJobCancellation.Cancel` in that consumer:

```csharp
public sealed class CancelConsumer(IJobCancellation cancellation) : IConsumer<CancelJob>
{
    public ValueTask HandleAsync(CancelJob message, CancellationToken cancellationToken)
    {
        cancellation.Cancel(message.JobId);
        return ValueTask.CompletedTask;
    }
}
```

Report progress to whoever is waiting by registering an `IWorkflowNotifier<TStep>` before
`AddWorkflow`; without one, the workflow says nothing.

## Known limits

State lives in the message and nowhere else. That has consequences worth knowing before you rely
on it:

- **A job cannot be resumed or inspected.** There is no journal; lose the message and the job is
  gone with no record of it.
- **A step whose retries run out leaves its job hanging.** The broker drops the message with a
  logged error, and no terminal event is produced, because nothing else knows the job existed.
- **A cancellation that arrives before the job does is forgotten.** Only jobs this process has
  already seen can be cancelled here.
- **Steps are not idempotent for you.** A retry, or a redelivery after a crash, runs the step
  again; make the work repeatable or deduplicate it yourself.
- **The deadline restarts with the worker.** It is measured from when this process first saw the
  job, not from when the job began.

Fixing these means moving job state into durable storage, which is a different library.

## Requirements

.NET 10, Snail.Toolkit.RabbitMQ 1.1.0 or later.

## Licence

MIT.

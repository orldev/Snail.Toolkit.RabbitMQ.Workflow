using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Publishes the next step of a job from the blueprint, and releases the job when it ends.
/// </summary>
/// <remarks>
/// The library used to ship this contract without an implementation, so every application wrote
/// its own hundred lines of it and each one guessed the next message from an untyped payload. The
/// guessing now lives in <see cref="WorkflowBlueprint{TStep}"/>, checked at startup, and this class
/// only walks it.
/// <para>
/// The notifier is a side channel, and nothing it does decides the job. A notifier that threw used
/// to escape as a transient failure: a finished job ran its last step again, and a step whose
/// progress line could not be written was never handed on. It is now told after the fact — a step
/// once its message is published, as its contract says — and its failure is logged and goes no further.
/// </para>
/// <para>
/// A step's result is handed on once, and a job ends once, whatever the broker delivers twice: the ledger is asked
/// before the next step is published and before an end is told. A duplicate used to publish the next step again,
/// and the job forked into two that both ran to the end and both reported it.
/// </para>
/// </remarks>
internal sealed class WorkflowDispatcher<TStep>(
    IRabbitProducer producer,
    WorkflowBlueprint<TStep> blueprint,
    IJobCancellation cancellation,
    IWorkflowLedger ledger,
    IWorkflowNotifier<TStep> notifier,
    WorkflowOptions options,
    TimeProvider time,
    ILogger<WorkflowDispatcher<TStep>> logger,
    WorkflowDeadlines<TStep>? deadlines = null) : IWorkflowDispatcher<TStep>
    where TStep : struct, Enum
{
    /// <summary>
    /// How long the ledger keeps a job's marks: two lifetimes, so a mark outlasts the job and anything of it the
    /// broker still holds.
    /// </summary>
    private TimeSpan KeepFor => options.JobLifetime * 2;

    /// <summary>
    /// How long taking a mark back may wait on the ledger: five seconds, as long as a Redis client waits for a reply by
    /// default.
    /// </summary>
    private static readonly TimeSpan ReleasePatience = TimeSpan.FromSeconds(5);

    /// <inheritdoc/>
    public async ValueTask StartAsync<TPayload>(
        string plan,
        Guid jobId,
        TPayload payload,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfEqual(jobId, Guid.Empty);

        var context = new WorkflowContext<TStep>(jobId, blueprint.Steps(plan), startedAt: time.GetUtcNow().UtcDateTime);
        if (!await ledger.TryHandOverAsync(jobId, context.CurrentStepIndex, KeepFor, cancellationToken))
            throw new ArgumentException($"Job {jobId} was started already, and a job id names one job.", nameof(jobId));

        await HandOnAsync(context, payload, cancellationToken);

        if (deadlines is not null)
            await PublishDeadlineAsync(context, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<Guid> StartAsync<TPayload>(
        string plan,
        TPayload payload,
        CancellationToken cancellationToken)
    {
        Guid jobId = Guid.CreateVersion7(time.GetUtcNow());

        await StartAsync(plan, jobId, payload, cancellationToken);

        return jobId;
    }

    /// <inheritdoc/>
    public async ValueTask DispatchAsync<TResult>(
        WorkflowContext<TStep> context,
        TResult result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        GuardProducedType(context, typeof(TResult));

        if (context.IsFinished)
        {
            await FinishAsync(context, cancellationToken);
            return;
        }

        if (!await IsHandedOverHereAsync(context, cancellationToken))
            return;

        await HandOnAsync(context, result, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask FailAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(failure);

        cancellation.TryComplete(context.JobId);
        if (!await ledger.TryEndAsync(context.JobId, KeepFor, cancellationToken))
        {
            Log.EndedAlready(logger, context.JobId);
            return;
        }

        await NotifyAsync(
            static (notifier, ended, token) => notifier.FailedAsync(ended.Context, ended.Failure, token),
            (Context: context, Failure: failure),
            context.JobId,
            cancellationToken);

        Log.Ended(logger, context.JobId, failure.Step, failure.Reason);
    }

    /// <remarks>
    /// The deadline is published after the first step, so a start that failed to publish leaves no deadline behind to
    /// report a job that never ran. A deadline that cannot be published is logged, not thrown: the job is running
    /// already, and every worker still stops its steps at the job's deadline.
    /// </remarks>
    private async ValueTask PublishDeadlineAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken)
    {
        var deadline = new WorkflowDeadline<TStep>(context);
        TimeSpan lifetime = options.JobLifetime;

        try
        {
            await producer.PublishAsync(deadline, waiting => waiting.MessageTimeToLive(lifetime), cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            Log.DeadlineNotPublished(logger, exception, context.JobId);
        }
    }

    private async ValueTask FinishAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken)
    {
        cancellation.TryComplete(context.JobId);
        if (!await ledger.TryEndAsync(context.JobId, KeepFor, cancellationToken))
        {
            Log.EndedAlready(logger, context.JobId);
            return;
        }

        await NotifyAsync(
            static (notifier, finished, token) => notifier.FinishedAsync(finished, token),
            context,
            context.JobId,
            cancellationToken);

        Log.Finished(logger, context.JobId, context.Steps.Count);
    }

    /// <remarks>
    /// A delivery that loses here — a duplicate of a step already handed on, or a step of a job that has ended —
    /// drops its result.
    /// </remarks>
    private async ValueTask<bool> IsHandedOverHereAsync(WorkflowContext<TStep> context, CancellationToken cancellationToken)
    {
        if (await ledger.TryHandOverAsync(context.JobId, context.CurrentStepIndex, KeepFor, cancellationToken))
            return true;

        Log.NotHandedOver(logger, context);

        return false;
    }

    private async ValueTask HandOnAsync<TResult>(WorkflowContext<TStep> context, TResult result, CancellationToken cancellationToken)
    {
        WorkflowContext<TStep> next = context.MoveNext();
        WorkflowStep<TStep> step = blueprint.Step(next.CurrentStep);

        await PublishOnceAsync(step, context, next, result, cancellationToken);
        await NotifyAsync(
            static (notifier, started, token) => notifier.StepStartedAsync(started, token),
            next,
            next.JobId,
            cancellationToken);

        Log.MovedTo(logger, context.JobId, next.CurrentStep);
    }

    /// <remarks>
    /// A publish that fails takes its mark back, or the step's own retry would be dropped as its duplicate and the job
    /// would hang. Taking it back must not hide why the publish failed, so its own failure is only logged.
    /// </remarks>
    private async ValueTask PublishOnceAsync<TResult>(
        WorkflowStep<TStep> step,
        WorkflowContext<TStep> handing,
        WorkflowContext<TStep> next,
        TResult result,
        CancellationToken cancellationToken)
    {
        try
        {
            await PublishAsync(step, next, result, cancellationToken);
        }
        catch (Exception)
        {
            await ReleaseAsync(handing);
            throw;
        }
    }

    /// <remarks>
    /// Taken back under a patience of its own, not under the step's token: a publish cancelled by a stopping host still
    /// takes its mark back. It used to wait with no token at all, and a shared ledger that stopped answering held the
    /// handler, and the host's shutdown, for as long as it stayed silent. A ledger deaf to the token is abandoned, not
    /// stopped, as a notifier is.
    /// </remarks>
    private async ValueTask ReleaseAsync(WorkflowContext<TStep> handing)
    {
        using var patience = new CancellationTokenSource(ReleasePatience, time);
        try
        {
            ValueTask released = ledger.ReleaseHandOverAsync(handing.JobId, handing.CurrentStepIndex, patience.Token);

            if (released.IsCompleted)
                await released;
            else
                await released.AsTask().WaitAsync(patience.Token);
        }
        catch (Exception exception)
        {
            Log.NotReleased(logger, exception, handing.JobId);
        }
    }

    private ValueTask PublishAsync<TResult>(
        WorkflowStep<TStep> step,
        WorkflowContext<TStep> next,
        TResult result,
        CancellationToken cancellationToken)
    {
        if (step.PublishAccepted is Func<WorkflowContext<TStep>, TResult, IRabbitProducer, CancellationToken, ValueTask> accepted)
            return accepted(next, result, producer, cancellationToken);

        object? handed = result;
        GuardAcceptedType(step, next.CurrentStep, handed);

        return step.Publish(next, handed, producer, cancellationToken);
    }

    /// <remarks>
    /// The notice is a static lambda over explicit state, so telling a notifier that completes at once
    /// allocates nothing, and the hand-over benchmarks keep their bytes. A notice still pending is awaited under the
    /// token, not merely handed it: a notifier deaf to its token held the handler past the host's shutdown. Such a
    /// notice is abandoned, not stopped, and logged as lost.
    /// </remarks>
    private async ValueTask NotifyAsync<TState>(
        Func<IWorkflowNotifier<TStep>, TState, CancellationToken, ValueTask> notice,
        TState state,
        Guid jobId,
        CancellationToken cancellationToken)
    {
        try
        {
            ValueTask told = notice(notifier, state, cancellationToken);

            if (told.IsCompleted)
                await told;
            else
                await told.AsTask().WaitAsync(cancellationToken);
        }
        catch (Exception exception)
        {
            Log.NotNotified(logger, exception, jobId);
        }
    }

    private void GuardProducedType(WorkflowContext<TStep> context, Type produced)
    {
        if (context.CurrentStepIndex < 0)
            return;

        WorkflowStep<TStep> completed = blueprint.Step(context.CurrentStep);
        if (completed.Output.IsAssignableFrom(produced))
            return;

        throw new WorkflowConfigurationException(
            $"Step '{context.CurrentStep}' is declared to produce '{completed.Output.Name}', " +
            $"but its consumer returned '{produced.Name}'.");
    }

    /// <remarks>
    /// Only a result of another type than the step accepts comes here; one of exactly that type, or a reference the
    /// accepted type is assigned from, is published by the step's typed delegate and needs no check. The result
    /// arrives boxed once, so a value-type result is not boxed again here and for the publish.
    /// </remarks>
    private static void GuardAcceptedType(WorkflowStep<TStep> step, TStep next, object? result)
    {
        if (result is null
                ? !step.Input.IsValueType || Nullable.GetUnderlyingType(step.Input) is not null
                : step.Input.IsInstanceOfType(result))
            return;

        throw new WorkflowConfigurationException(
            $"Step '{next}' accepts '{step.Input.Name}', but it was handed " +
            $"'{(result is null ? "null" : result.GetType().Name)}'.");
    }
}

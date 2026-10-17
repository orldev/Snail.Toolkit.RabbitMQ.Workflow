using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Publishes the next step of a job from the blueprint, and releases the job when it ends.
/// </summary>
/// <remarks>
/// The library used to ship this contract without an implementation, so every application wrote
/// its own hundred lines of it and each one guessed the next message from an untyped payload. The
/// guessing now lives in <see cref="WorkflowBlueprint{TStep}"/>, checked at startup, and this class
/// only walks it.
/// </remarks>
internal sealed class WorkflowDispatcher<TStep>(
    IRabbitProducer producer,
    WorkflowBlueprint<TStep> blueprint,
    IJobCancellation cancellation,
    IWorkflowNotifier<TStep> notifier,
    ILogger<WorkflowDispatcher<TStep>> logger) : IWorkflowDispatcher<TStep>
    where TStep : struct, Enum
{
    /// <inheritdoc/>
    public ValueTask StartAsync(
        string plan,
        Guid jobId,
        string query,
        CancellationToken cancellationToken = default) =>
        DispatchAsync(new WorkflowContext<TStep>(jobId, query, blueprint.Steps(plan)), default(None), cancellationToken);

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
            cancellation.TryComplete(context.JobId);
            await notifier.FinishedAsync(context, cancellationToken);

            Log.Finished(logger, context.JobId, context.Steps.Count);
            return;
        }

        var next = context.MoveNext();
        var step = blueprint.Step(next.CurrentStep);

        if (step.PublishAccepted is Func<WorkflowContext<TStep>, TResult, IRabbitProducer, CancellationToken, ValueTask> accepted)
        {
            await notifier.StepStartedAsync(next, cancellationToken);
            await accepted(next, result, producer, cancellationToken);
        }
        else
        {
            object? handed = result;
            GuardAcceptedType(step, next.CurrentStep, handed);

            await notifier.StepStartedAsync(next, cancellationToken);
            await step.Publish(next, handed, producer, cancellationToken);
        }

        Log.MovedTo(logger, context.JobId, next.CurrentStep);
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
        await notifier.FailedAsync(context, failure, cancellationToken);

        Log.Ended(logger, context.JobId, failure.Step, failure.Reason);
    }

    private void GuardProducedType(WorkflowContext<TStep> context, Type produced)
    {
        if (context.CurrentStepIndex < 0)
            return;

        var completed = blueprint.Step(context.CurrentStep);
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

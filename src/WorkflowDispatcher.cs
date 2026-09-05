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
            cancellation.Complete(context.JobId);
            await notifier.FinishedAsync(context, cancellationToken);

            logger.LogInformation("Job {JobId} finished the plan after {StepCount} step(s)",
                context.JobId, context.Steps.Count);
            return;
        }

        var next = context.MoveNext();
        var step = blueprint.Step(next.CurrentStep);

        GuardAcceptedType(step, next.CurrentStep, result);

        await notifier.StepStartedAsync(next, cancellationToken);
        await step.Publish(next, result, producer, cancellationToken);

        logger.LogDebug("Job {JobId} moved to step {Step}", context.JobId, next.CurrentStep);
    }

    /// <inheritdoc/>
    public async ValueTask FailAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(failure);

        cancellation.Complete(context.JobId);
        await notifier.FailedAsync(context, failure, cancellationToken);

        logger.LogWarning("Job {JobId} ended on step {Step}: {Reason}",
            context.JobId, failure.Step, failure.Reason);
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

    private static void GuardAcceptedType<TResult>(WorkflowStep<TStep> step, TStep next, TResult result)
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

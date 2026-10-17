using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Consumers;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Runs one step of a job and hands its result to the next step.
/// </summary>
/// <typeparam name="TCommand">The message that carries this step.</typeparam>
/// <typeparam name="TStep">The step enumeration of the workflow.</typeparam>
/// <typeparam name="TResult">What this step produces for the next one; <see cref="None"/> when it produces nothing.</typeparam>
/// <remarks>
/// Three outcomes are kept apart on purpose, because conflating them cost messages.
/// <list type="bullet">
/// <item>
/// The job was cancelled or outlived its deadline: the message is acknowledged and the job is
/// ended, because nothing is waiting for it any more.
/// </item>
/// <item>
/// The host is shutting down: the exception is rethrown so the broker keeps the message. The
/// previous version could not tell this apart from a cancelled job — both tokens were linked
/// into one and only the linked token was examined — so every deployment silently ate one
/// in-flight message per consumer.
/// </item>
/// <item>
/// The step threw: either it is transient and the broker retries it, or it is final and the job
/// is ended here. Doing both, as the previous version did, announced the job dead and then let
/// the broker run it again against a job nobody was listening to.
/// </item>
/// </list>
/// Dispatching the next step is not the derived class's job any more: returning a result is
/// enough, so a step can no longer leave a job hanging by forgetting to publish.
/// </remarks>
public abstract class WorkflowConsumer<TCommand, TStep, TResult> : IConsumer<TCommand>
    where TStep : struct, Enum
    where TCommand : IWorkflowCommand<TStep>
{
    private readonly IJobCancellation _cancellation;
    private readonly IWorkflowDispatcher<TStep> _dispatcher;
    private readonly ILogger _logger;

    /// <summary>
    /// Creates a consumer that logs under the name of the derived type.
    /// </summary>
    protected WorkflowConsumer(
        IJobCancellation cancellation,
        IWorkflowDispatcher<TStep> dispatcher,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(cancellation);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        _cancellation = cancellation;
        _dispatcher = dispatcher;
        _logger = loggerFactory.CreateLogger(GetType());
    }

    /// <inheritdoc/>
    public async ValueTask HandleAsync(TCommand message, CancellationToken cancellationToken)
    {
        var context = message.Context;
        var job = CancellationToken.None;

        try
        {
            job = _cancellation.Token(context.JobId);
            using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, job);

            var result = await ProcessAsync(message, work.Token);

            await _dispatcher.DispatchAsync(context, result, work.Token);
        }
        catch (OperationCanceledException) when (job.IsCancellationRequested)
        {
            Log.Cancelled(_logger, context.JobId, context.CurrentStep);

            await ReportAsync(context, new WorkflowFailure("The job was cancelled.", Named(context)));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.Interrupted(_logger, context.CurrentStep, context.JobId);

            throw;
        }
        catch (Exception exception) when (CanRetry(exception))
        {
            Log.Retried(_logger, exception, context.CurrentStep, context.JobId);

            throw;
        }
        catch (Exception exception)
        {
            Log.Failed(_logger, exception, context.CurrentStep, context.JobId);

            await ReportAsync(context, Failure(exception, context));
        }
    }

    /// <summary>
    /// Does the step's work and returns what the next step needs.
    /// </summary>
    /// <remarks>
    /// Throw <see cref="WorkflowFailureException"/> to end the job without a retry; anything else
    /// thrown is treated as transient and handed back to the broker.
    /// </remarks>
    protected abstract Task<TResult> ProcessAsync(TCommand message, CancellationToken cancellationToken);

    /// <summary>
    /// Whether a failure is worth another attempt.
    /// </summary>
    /// <remarks>
    /// Override to recognise the transient failures of a particular step — a rate limit, a
    /// timeout — and to refuse retries for the failures that will repeat identically.
    /// </remarks>
    protected virtual bool CanRetry(Exception exception) =>
        exception is not (WorkflowFailureException or WorkflowConfigurationException);

    private static WorkflowFailure Failure(Exception exception, WorkflowContext<TStep> context) => exception switch
    {
        WorkflowFailureException failure => failure.Failure with { Step = Named(context) },
        WorkflowConfigurationException => new WorkflowFailure("The workflow is misconfigured.", Named(context)),
        _ => WorkflowFailure.Unknown with { Step = Named(context) }
    };

    private static string Named(WorkflowContext<TStep> context) => context.CurrentStep.ToString();

    private async Task ReportAsync(WorkflowContext<TStep> context, WorkflowFailure failure)
    {
        try
        {
            await _dispatcher.FailAsync(context, failure, CancellationToken.None);
        }
        catch (Exception exception)
        {
            Log.NotReported(_logger, exception, context.JobId, context.CurrentStep);
        }
    }
}

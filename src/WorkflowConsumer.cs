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
/// in-flight message per consumer. Snail.Toolkit.RabbitMQ 2.0 returns such a message to its
/// queue whole, without spending a retry attempt.
/// </item>
/// <item>
/// The step threw: either it is transient and the broker retries it, or it is final and the job
/// is ended here. Doing both, as the previous version did, announced the job dead and then let
/// the broker run it again against a job nobody was listening to.
/// </item>
/// </list>
/// Dispatching the next step is not the derived class's job any more: returning a result is
/// enough, so a step can no longer leave a job hanging by forgetting to publish.
/// <para>
/// The job's token is checked by the handler itself, before the step's work and before its result
/// is handed on: a step that never polls the token — a computation, a client without cancellation —
/// used to advance a job that had been cancelled while its message waited in the queue.
/// </para>
/// <para>
/// A transient failure is handed back to the broker, and its last attempt looks like every other one from here. When
/// the broker gives the step up, it says so through <see cref="ExhaustedAsync"/>, and the job is reported failed: it
/// used to hang, with its message dropped and nobody upstream told.
/// </para>
/// </remarks>
public abstract class WorkflowConsumer<TCommand, TStep, TResult> : IExhaustibleConsumer<TCommand>
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
        WorkflowContext<TStep> context = message.Context;
        if (context is null || context.CurrentStepIndex < 0)
            throw NotEntered();

        CancellationToken job = CancellationToken.None;

        try
        {
            job = _cancellation.Token(context.JobId, context.StartedAt);
            using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, job);

            work.Token.ThrowIfCancellationRequested();
            TResult result = await ProcessAsync(message, work.Token);

            work.Token.ThrowIfCancellationRequested();
            await _dispatcher.DispatchAsync(context, result, work.Token);
        }
        catch (OperationCanceledException) when (job.IsCancellationRequested)
        {
            Log.Cancelled(_logger, context.JobId, context.CurrentStep);

            await ReportAsync(context, new WorkflowFailure("The job was cancelled.", Named(context)), cancellationToken);
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

            await ReportAsync(context, Failure(exception, context), cancellationToken);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Only a failure handed back to the broker comes here: a final one was reported by the handler itself, which then
    /// let the message go, so a job is never reported failed twice. A message without a context names no job to report.
    /// </remarks>
    public async ValueTask ExhaustedAsync(TCommand message, Exception failure, CancellationToken cancellationToken = default)
    {
        WorkflowContext<TStep> context = message.Context;
        if (context is null || context.CurrentStepIndex < 0)
            return;

        Log.GivenUp(_logger, failure, context.CurrentStep, context.JobId);

        await ReportAsync(context, new WorkflowFailure("The step failed on every attempt.", Named(context)), cancellationToken);
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

    /// <remarks>
    /// A message whose json lost its context, or one built before its job entered the plan, names no step. Every
    /// branch of the handler threw again from that step's name inside its own catch, so the failure reached the log
    /// as nothing but a null reference. It is refused up front instead, and the broker retries it, then parks it.
    /// </remarks>
    private WorkflowConfigurationException NotEntered()
    {
        Log.NotEntered(_logger, typeof(TCommand).Name);

        return new WorkflowConfigurationException(
            $"A '{typeof(TCommand).Name}' message carries no workflow context positioned on a step.");
    }

    /// <remarks>
    /// The report runs under the host's token, not under none: a notifier waiting on a transport that is down
    /// held the handler, and with a prefetch of one the whole consumer, past the host's own shutdown.
    /// </remarks>
    private async Task ReportAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken)
    {
        try
        {
            await _dispatcher.FailAsync(context, failure, cancellationToken);
        }
        catch (Exception exception)
        {
            Log.NotReported(_logger, exception, context.JobId, context.CurrentStep);
        }
    }
}

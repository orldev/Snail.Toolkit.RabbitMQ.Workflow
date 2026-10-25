using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// What this library says about the jobs it runs.
/// </summary>
/// <remarks>
/// A step is an enumeration of the application's, and a generated logging method cannot be generic: each line takes
/// the step's name, and the generic entry points below name the step only once the level is enabled, so a line below
/// the configured level costs a level check and nothing else, on every step of every job.
/// </remarks>
internal static partial class Log
{
    internal static void Finished(ILogger logger, Guid jobId, int stepCount)
    {
        if (logger.IsEnabled(LogLevel.Information))
            FinishedPlan(logger, jobId, stepCount);
    }

    internal static void MovedTo<TStep>(ILogger logger, Guid jobId, TStep step) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Debug))
            MovedToStep(logger, jobId, step.ToString());
    }

    internal static void Ended(ILogger logger, Guid jobId, string? step, string reason)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            EndedOnStep(logger, jobId, step, reason);
    }

    internal static void Cancelled<TStep>(ILogger logger, Guid jobId, TStep step) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Warning))
            CancelledOnStep(logger, jobId, step.ToString());
    }

    internal static void Interrupted<TStep>(ILogger logger, TStep step, Guid jobId) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Information))
            InterruptedByShutdown(logger, step.ToString(), jobId);
    }

    internal static void Retried<TStep>(ILogger logger, Exception exception, TStep step, Guid jobId) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Warning))
            StepRetried(logger, exception, step.ToString(), jobId);
    }

    internal static void Failed<TStep>(ILogger logger, Exception exception, TStep step, Guid jobId) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Error))
            StepFailed(logger, exception, step.ToString(), jobId);
    }

    internal static void NotReported<TStep>(ILogger logger, Exception exception, Guid jobId, TStep step) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Error))
            FailureNotReported(logger, exception, jobId, step.ToString());
    }

    internal static void GivenUp<TStep>(ILogger logger, Exception exception, TStep step, Guid jobId) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Error))
            StepGivenUp(logger, exception, step.ToString(), jobId);
    }

    internal static void NotHandedOver<TStep>(ILogger logger, WorkflowContext<TStep> context) where TStep : struct, Enum
    {
        if (logger.IsEnabled(LogLevel.Information))
            StepNotHandedOver(logger, context.CurrentStepIndex < 0 ? "start" : context.CurrentStep.ToString(), context.JobId);
    }

    [LoggerMessage(EventId = 12, Level = LogLevel.Information, SkipEnabledCheck = true,
        Message = "Step {Step} of job {JobId} was handed on already, or its job has ended; this delivery's result is dropped")]
    private static partial void StepNotHandedOver(ILogger logger, string step, Guid jobId);

    [LoggerMessage(EventId = 13, Level = LogLevel.Information,
        Message = "Job {JobId} had ended already; it is not reported again")]
    internal static partial void EndedAlready(ILogger logger, Guid jobId);

    [LoggerMessage(EventId = 15, Level = LogLevel.Error,
        Message = "The deadline of job {JobId} could not be published; its workers still stop it at its deadline, but nothing reports a deadline passed between steps")]
    internal static partial void DeadlineNotPublished(ILogger logger, Exception exception, Guid jobId);

    [LoggerMessage(EventId = 14, Level = LogLevel.Error,
        Message = "The hand-over of job {JobId} could not be taken back after its publish failed; the step's retry may be dropped as a duplicate")]
    internal static partial void NotReleased(ILogger logger, Exception exception, Guid jobId);

    [LoggerMessage(EventId = 11, Level = LogLevel.Error, SkipEnabledCheck = true,
        Message = "Step {Step} of job {JobId} failed on every attempt and was given up; the job is reported failed")]
    private static partial void StepGivenUp(ILogger logger, Exception exception, string step, Guid jobId);

    [LoggerMessage(EventId = 10, Level = LogLevel.Error,
        Message = "A {Command} message carries no workflow context positioned on a step; the broker retries it, then parks it")]
    internal static partial void NotEntered(ILogger logger, string command);

    [LoggerMessage(EventId = 9, Level = LogLevel.Error,
        Message = "The workflow notifier failed for job {JobId}; the job goes on without that notice")]
    internal static partial void NotNotified(ILogger logger, Exception exception, Guid jobId);

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, SkipEnabledCheck = true,
        Message = "Job {JobId} finished the plan after {StepCount} step(s)")]
    private static partial void FinishedPlan(ILogger logger, Guid jobId, int stepCount);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug, SkipEnabledCheck = true,
        Message = "Job {JobId} moved to step {Step}")]
    private static partial void MovedToStep(ILogger logger, Guid jobId, string step);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, SkipEnabledCheck = true,
        Message = "Job {JobId} ended on step {Step}: {Reason}")]
    private static partial void EndedOnStep(ILogger logger, Guid jobId, string? step, string reason);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning, SkipEnabledCheck = true,
        Message = "Job {JobId} was cancelled on step {Step}")]
    private static partial void CancelledOnStep(ILogger logger, Guid jobId, string step);

    [LoggerMessage(EventId = 5, Level = LogLevel.Information, SkipEnabledCheck = true,
        Message = "Step {Step} of job {JobId} was interrupted by shutdown; the message goes back to the broker")]
    private static partial void InterruptedByShutdown(ILogger logger, string step, Guid jobId);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning, SkipEnabledCheck = true,
        Message = "Step {Step} of job {JobId} failed; the broker will retry it")]
    private static partial void StepRetried(ILogger logger, Exception exception, string step, Guid jobId);

    [LoggerMessage(EventId = 7, Level = LogLevel.Error, SkipEnabledCheck = true,
        Message = "Step {Step} of job {JobId} failed for good")]
    private static partial void StepFailed(ILogger logger, Exception exception, string step, Guid jobId);

    [LoggerMessage(EventId = 8, Level = LogLevel.Error, SkipEnabledCheck = true,
        Message = "Job {JobId} ended on step {Step}, and reporting that failure failed as well")]
    private static partial void FailureNotReported(ILogger logger, Exception exception, Guid jobId, string step);
}

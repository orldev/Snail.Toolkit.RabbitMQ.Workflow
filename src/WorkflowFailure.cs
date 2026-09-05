namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The terminal reason a job stopped, in words its caller can be shown.
/// </summary>
/// <param name="Reason">What went wrong, phrased for whoever waits on the job.</param>
/// <param name="Step">The step the job died on, or null when it died before entering one.</param>
/// <remarks>
/// The failing exception deliberately does not travel in this record. It used to, and the
/// message of an arbitrary exception ended up in the caller's stream verbatim, carrying
/// connection strings and internal paths with it. Log the exception, report the reason.
/// </remarks>
public sealed record WorkflowFailure(string Reason, string? Step = null)
{
    /// <summary>
    /// The reason shown when a step gives no better one.
    /// </summary>
    public static WorkflowFailure Unknown { get; } = new("The step failed.");
}

/// <summary>
/// Thrown by step code to say a failure is final and must not be retried.
/// </summary>
/// <remarks>
/// Everything else a step throws is assumed transient and handed back to the broker, because
/// retrying a permanent failure only wastes attempts while failing to retry a transient one
/// loses the job. A step that knows better says so by throwing this.
/// </remarks>
public sealed class WorkflowFailureException(string reason, Exception? innerException = null)
    : Exception(reason, innerException)
{
    /// <summary>
    /// The terminal failure to report to whoever waits on the job.
    /// </summary>
    public WorkflowFailure Failure { get; } = new(reason);
}

/// <summary>
/// Thrown at startup when the registered steps and plans cannot form a working workflow.
/// </summary>
/// <remarks>
/// Every check that raises this could only have been made at dispatch time before, which meant
/// a mistyped plan surfaced as a failed job in production instead of a failed boot.
/// </remarks>
public sealed class WorkflowConfigurationException(string message) : Exception(message);

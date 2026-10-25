namespace Snail.Toolkit.RabbitMQ.Workflow;

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

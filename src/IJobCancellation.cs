namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The cancellation and deadline of every job this process is running.
/// </summary>
/// <remarks>
/// Cancellation registered here reaches only the process that registered it. Cancelling a job
/// that runs across several workers means broadcasting the cancellation to all of them —
/// typically a fanout exchange with one queue per instance, whose consumer calls
/// <see cref="TryCancel"/> locally. This type is the local half of that, not the whole of it.
/// </remarks>
public interface IJobCancellation
{
    /// <summary>
    /// The token that is cancelled when the job is cancelled or outlives its deadline.
    /// </summary>
    CancellationToken Token(Guid jobId);

    /// <summary>
    /// Cancels the job if this process is running it.
    /// </summary>
    /// <returns>True when a job was found and cancelled here.</returns>
    bool TryCancel(Guid jobId);

    /// <summary>
    /// Releases a job that reached a terminal outcome.
    /// </summary>
    /// <returns>True when a job was found and released here.</returns>
    bool TryComplete(Guid jobId);
}

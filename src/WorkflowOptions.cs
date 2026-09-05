namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Settings that apply to every job of one workflow.
/// </summary>
public sealed class WorkflowOptions
{
    /// <summary>
    /// How long a job may run before it is cancelled; <see cref="Timeout.InfiniteTimeSpan"/> for no deadline.
    /// </summary>
    /// <remarks>
    /// The deadline covers the whole job, not one step, and it starts when this process first sees
    /// the job — a worker restart therefore restarts the clock. Keep it comfortably under the
    /// broker's consumer timeout, which drops the channel of a consumer that holds a message longer
    /// than that (30 minutes on a stock RabbitMQ).
    /// </remarks>
    public TimeSpan JobLifetime { get; set; } = TimeSpan.FromMinutes(10);
}

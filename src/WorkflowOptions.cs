namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Settings that apply to every job of one workflow.
/// </summary>
/// <remarks>
/// The settings are fixed once the workflow is validated at registration. They are one singleton every worker reads
/// on each new job, and a deadline changed after the check that proved it could be held — set to zero, it cancelled
/// every job on arrival — slipped past that check.
/// </remarks>
public sealed class WorkflowOptions
{
    private bool _isFixed;

    /// <summary>
    /// The longest deadline a job can have: what a timer holds, about 49.7 days.
    /// </summary>
    public static TimeSpan LongestJobLifetime { get; } = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// How long a job may run before it is cancelled.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">
    /// The workflow is already registered, or the deadline is not longer than zero and at most
    /// <see cref="LongestJobLifetime"/>.
    /// </exception>
    /// <remarks>
    /// The deadline covers the whole job, not one step, and is measured from its start on every worker. Keep it
    /// comfortably under the broker's consumer timeout, which drops the channel of a consumer that holds a message
    /// longer than that (30 minutes on a stock RabbitMQ). A job has a deadline, always: without one, a worker that
    /// ran a step of a job ended elsewhere never learns it may forget the job, and kept its entry forever. Its value
    /// is checked where it is set, because a span no timer can hold used to start the application and then fail
    /// every job — zero cancelled each one on arrival, and a negative span or one past the timer's limit threw from
    /// every <see cref="IJobCancellation.Token"/>, which the consumer took for a transient failure.
    /// </remarks>
    public TimeSpan JobLifetime
    {
        get;
        set
        {
            if (_isFixed)
                throw new WorkflowConfigurationException(
                    "The workflow's settings were fixed when it was registered, and a deadline changed afterwards would never be checked.");

            if (value <= TimeSpan.Zero || value > LongestJobLifetime)
                throw new WorkflowConfigurationException(
                    $"JobLifetime is {value}, but a job's deadline must be longer than zero and at most " +
                    $"{LongestJobLifetime.TotalDays:F1} days ({nameof(LongestJobLifetime)}).");

            field = value;
        }
    } = TimeSpan.FromMinutes(10);

    internal void Fix() => _isFixed = true;
}

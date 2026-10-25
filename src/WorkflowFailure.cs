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

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Thrown when the declared steps and plans cannot form a working workflow, or a job is handed something they do not fit.
/// </summary>
/// <remarks>
/// Every check that raises this could only have been made at dispatch time before, which meant
/// a mistyped plan surfaced as a failed job in production instead of a failed boot. What startup can
/// prove it proves at startup; the payload a job starts with is checked by the call that starts it, and
/// a context built by hand is checked where it is dispatched.
/// </remarks>
public sealed class WorkflowConfigurationException(string message) : Exception(message);

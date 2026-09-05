namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// A message that runs one step of a job and carries the job's position with it.
/// </summary>
public interface IWorkflowCommand<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// Where the job stands, as of this step.
    /// </summary>
    WorkflowContext<TStep> Context { get; init; }
}

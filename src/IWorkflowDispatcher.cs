namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Moves a job through its plan by publishing the next step's message.
/// </summary>
public interface IWorkflowDispatcher<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// Starts a job on a named plan and publishes its first step.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">No plan goes by that name.</exception>
    ValueTask StartAsync(string plan, Guid jobId, string query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Hands a completed step's result to the next step, or finishes the job when the plan is done.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">
    /// The result does not match what the next step was declared to accept.
    /// </exception>
    ValueTask DispatchAsync<TResult>(
        WorkflowContext<TStep> context,
        TResult result,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Ends the job for good and reports why.
    /// </summary>
    ValueTask FailAsync(
        WorkflowContext<TStep> context,
        WorkflowFailure failure,
        CancellationToken cancellationToken = default);
}

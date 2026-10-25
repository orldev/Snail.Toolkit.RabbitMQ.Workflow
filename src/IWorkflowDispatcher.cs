namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Moves a job through its plan by publishing the next step's message.
/// </summary>
public interface IWorkflowDispatcher<TStep> where TStep : struct, Enum
{
    /// <summary>
    /// Starts a job on a named plan and publishes its first step with the payload given.
    /// </summary>
    /// <typeparam name="TPayload">
    /// What the plan's first step accepts; <see cref="None"/> when it accepts nothing.
    /// </typeparam>
    /// <exception cref="WorkflowConfigurationException">
    /// No plan goes by that name, or its first step does not accept the payload.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The job id is <see cref="Guid.Empty"/>, which every job started without an id of its own would share — and
    /// with it one cancellation, released by whichever of them ends first.
    /// </exception>
    /// <exception cref="ArgumentException">A job was started under this id already.</exception>
    /// <remarks>
    /// A job id names one job: two jobs under one id would share their cancellation and deadline on every worker. The
    /// ledger refuses an id it has seen started — on this process with the ledger kept here, across the fleet with one
    /// it shares. A start whose first message could not be published is taken back, so a retry under the same id is
    /// not refused.
    /// <para>
    /// The first step's payload is the one pairing startup cannot prove, because it is chosen here, by the caller. It
    /// is checked here instead, before anything is published, so a mismatch fails the call that made it rather than a
    /// consumer later.
    /// </para>
    /// </remarks>
    ValueTask StartAsync<TPayload>(
        string plan,
        Guid jobId,
        TPayload payload,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a job on a named plan under an id of its own, and answers with that id.
    /// </summary>
    /// <typeparam name="TPayload">
    /// What the plan's first step accepts; <see cref="None"/> when it accepts nothing.
    /// </typeparam>
    /// <exception cref="WorkflowConfigurationException">
    /// No plan goes by that name, or its first step does not accept the payload.
    /// </exception>
    /// <remarks>
    /// The id is a <see cref="Guid.CreateVersion7()"/>: new for every job, so two jobs can never share one, and
    /// ordered by time, so the ids of a log read in the order the jobs began. Give an id of your own only to tie the
    /// job to something that already has one — a request, a record.
    /// </remarks>
    ValueTask<Guid> StartAsync<TPayload>(
        string plan,
        TPayload payload,
        CancellationToken cancellationToken);

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

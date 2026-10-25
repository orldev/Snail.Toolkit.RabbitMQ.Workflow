using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Refuses to start a host whose consumers do not fit the workflow they run.
/// </summary>
/// <remarks>
/// Two mistakes used to surface only as failed jobs. A step queue declared without retry rejected a failed message
/// into nothing, so its job hung without a word; and a consumer returning another type than its step was declared to
/// produce failed its job as misconfigured, after the step's work was done. Both are written in the topology the
/// application declares, so both are read here, before any consumer starts; and so is a deadline asked for without a
/// ledger the fleet shares. The check runs as the host is starting, ahead of every hosted service's start. A process
/// that consumes no step, one that only starts jobs, has nothing to check but its deadline.
/// </remarks>
internal sealed class WorkflowStartupCheck<TStep>(
    WorkflowBlueprint<TStep> blueprint,
    IOptions<RabbitOptions> rabbit,
    IWorkflowLedger ledger,
    WorkflowDeadlines<TStep>? deadlines = null)
    : IHostedLifecycleService
    where TStep : struct, Enum
{
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        List<string> problems = [];
        CheckDeadline(problems);

        foreach (RabbitConsumerDeclaration consumer in rabbit.Value.Consumers)
        {
            CheckQueues(consumer, problems);
            CheckConsumerTypes(consumer, problems);
        }

        if (problems.Count == 0)
            return Task.CompletedTask;

        throw new WorkflowConfigurationException(string.Join(Environment.NewLine, problems));
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <remarks>
    /// A deadline read against a ledger kept in one process would end a job that had finished on another worker.
    /// </remarks>
    private void CheckDeadline(List<string> problems)
    {
        if (deadlines is null || ledger.IsShared)
            return;

        problems.Add(
            "Jobs are given a deadline, but the ledger is kept in this process alone, so a deadline coming back here " +
            "cannot tell a job that finished on another worker from one still running. Register an IWorkflowLedger " +
            "the whole fleet shares before AddWorkflow.");
    }

    private void CheckQueues(RabbitConsumerDeclaration consumer, List<string> problems)
    {
        if (StepCarriedBy(consumer.PayloadType) is not { } step)
            return;

        foreach (RabbitQueueDeclaration queue in consumer.QueueDeclarations)
        {
            if (queue.RetryQueue is null)
                problems.Add(
                    $"Queue '{queue.Name}' carries step '{step}' but does not retry, so a failed step there is lost " +
                    "with its job. Declare it with AsWorkflowStep.");
        }
    }

    private void CheckConsumerTypes(RabbitConsumerDeclaration consumer, List<string> problems)
    {
        foreach (Type consumerType in consumer.ConsumerTypes)
        {
            if (Handled(consumerType) is not var (command, produced))
                continue;

            if (StepCarriedBy(command) is not { } step)
            {
                problems.Add(
                    $"Consumer '{consumerType.Name}' handles '{command.Name}', which no step of this workflow is declared with.");
                continue;
            }

            Type declared = blueprint.Step(step).Output;
            if (!declared.IsAssignableFrom(produced))
                problems.Add(
                    $"Consumer '{consumerType.Name}' returns '{produced.Name}', " +
                    $"but step '{step}' is declared to produce '{declared.Name}'.");
        }
    }

    private TStep? StepCarriedBy(Type command)
    {
        foreach ((TStep step, WorkflowStep<TStep> declared) in blueprint.DeclaredSteps)
        {
            if (declared.Command == command)
                return step;
        }

        return null;
    }

    /// <remarks>
    /// The command and the result are read off the consumer's <see cref="WorkflowConsumer{TCommand, TStep, TResult}"/>
    /// base; a consumer of another workflow's steps, or of no workflow at all, is none of this check's business.
    /// </remarks>
    private static (Type Command, Type Produced)? Handled(Type consumerType)
    {
        for (Type? type = consumerType; type is not null; type = type.BaseType)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(WorkflowConsumer<,,>))
                continue;

            Type[] arguments = type.GetGenericArguments();

            return arguments[1] == typeof(TStep) ? (arguments[0], arguments[2]) : null;
        }

        return null;
    }
}

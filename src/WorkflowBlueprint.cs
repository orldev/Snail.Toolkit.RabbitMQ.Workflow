using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// Declares what every step of a workflow receives, what it produces, and which plans are allowed.
/// </summary>
/// <remarks>
/// This is the contract the workflow used to lack. Before, a dispatcher was handed a step and an
/// untyped payload and guessed the message to publish by pattern-matching the pair at run time, so
/// a step wired to the wrong payload surfaced as a dead job in production. Here the pairing is
/// stated once per step and checked twice: the compiler binds the payload type to the factory that
/// consumes it, and <see cref="Validate"/> walks every declared plan at startup to prove each step
/// produces what the next one accepts.
/// </remarks>
public sealed class WorkflowBlueprint<TStep> where TStep : struct, Enum
{
    private readonly Dictionary<TStep, WorkflowStep<TStep>> _steps = [];
    private readonly Dictionary<string, IReadOnlyList<TStep>> _plans = [];
    private bool _isValidated;

    /// <summary>
    /// Settings shared by every job of this workflow.
    /// </summary>
    public WorkflowOptions Options { get; } = new();

    /// <summary>
    /// Declares a step: the message that carries it, the payload it accepts, and the payload it produces.
    /// </summary>
    /// <typeparam name="TCommand">The message published to run this step.</typeparam>
    /// <typeparam name="TIn">
    /// What the previous step must produce, or, for a plan's first step, what the job is started with; <see cref="None"/>
    /// for nothing.
    /// </typeparam>
    /// <typeparam name="TOut">What this step's consumer returns.</typeparam>
    /// <exception cref="WorkflowConfigurationException">The step is already declared.</exception>
    /// <remarks>
    /// The factory is handed the already-advanced context and the previous step's result, so the
    /// message it builds carries the position the receiving consumer will read back.
    /// </remarks>
    public WorkflowBlueprint<TStep> Step<TCommand, TIn, TOut>(
        TStep step,
        Func<WorkflowContext<TStep>, TIn, TCommand> command)
        where TCommand : class, IWorkflowCommand<TStep>
    {
        ArgumentNullException.ThrowIfNull(command);
        ThrowIfValidated();

        if (_steps.ContainsKey(step))
            throw new WorkflowConfigurationException($"Step '{step}' is declared twice.");

        Func<WorkflowContext<TStep>, TIn, IRabbitProducer, CancellationToken, ValueTask> accepted =
            (context, payload, producer, cancellationToken) =>
                producer.PublishAsync(command(context, payload), cancellationToken: cancellationToken);

        _steps[step] = new WorkflowStep<TStep>(
            typeof(TCommand),
            typeof(TIn),
            typeof(TOut),
            (context, payload, producer, cancellationToken) =>
                producer.PublishAsync(command(context, (TIn)payload!), cancellationToken: cancellationToken),
            accepted);

        return this;
    }

    /// <summary>
    /// Names an ordered run of steps a job may be started with.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">The name is already taken or the plan is empty.</exception>
    public WorkflowBlueprint<TStep> Plan(string name, params TStep[] steps)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(steps);
        ThrowIfValidated();

        if (steps.Length == 0)
            throw new WorkflowConfigurationException($"Plan '{name}' has no steps.");

        if (!_plans.TryAdd(name, [.. steps]))
            throw new WorkflowConfigurationException($"Plan '{name}' is declared twice.");

        return this;
    }

    /// <summary>
    /// The steps of a named plan.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">No plan goes by that name.</exception>
    public IReadOnlyList<TStep> Steps(string name) =>
        _plans.TryGetValue(name, out IReadOnlyList<TStep>? steps)
            ? steps
            : throw new WorkflowConfigurationException(
                $"No plan named '{name}'. Declared plans: {Names(_plans.Keys)}.");

    /// <summary>
    /// Every declared step, by its name.
    /// </summary>
    internal IReadOnlyDictionary<TStep, WorkflowStep<TStep>> DeclaredSteps => _steps;

    internal WorkflowStep<TStep> Step(TStep step) =>
        _steps.TryGetValue(step, out WorkflowStep<TStep>? declared)
            ? declared
            : throw new WorkflowConfigurationException(
                $"Step '{step}' is not declared, so no message can be published for it.");

    /// <summary>
    /// Proves every declared plan is runnable, and throws naming the first pairing that is not.
    /// </summary>
    /// <exception cref="WorkflowConfigurationException">A plan uses an undeclared step or a mismatched pair.</exception>
    public void Validate()
    {
        if (_steps.Count == 0)
            throw new WorkflowConfigurationException("The workflow declares no steps.");

        foreach ((string name, IReadOnlyList<TStep> steps) in _plans)
        {
            ValidatePlan(name, steps);
        }

        _isValidated = true;
        Options.Fix();
    }

    private void ThrowIfValidated()
    {
        if (_isValidated)
            throw new WorkflowConfigurationException(
                "The workflow was validated when it was registered, and a step or plan added afterwards would never be checked.");
    }

    private void ValidatePlan(string name, IReadOnlyList<TStep> steps)
    {
        foreach (TStep step in steps.Where(step => !_steps.ContainsKey(step)))
        {
            throw new WorkflowConfigurationException(
                $"Plan '{name}' uses step '{step}', which is not declared.");
        }

        for (var i = 1; i < steps.Count; i++)
        {
            WorkflowStep<TStep> producedBy = _steps[steps[i - 1]];
            WorkflowStep<TStep> consumedBy = _steps[steps[i]];

            if (!consumedBy.Input.IsAssignableFrom(producedBy.Output))
                throw new WorkflowConfigurationException(
                    $"Plan '{name}' pipes step '{steps[i - 1]}' into '{steps[i]}', " +
                    $"but '{steps[i - 1]}' produces '{producedBy.Output.Name}' " +
                    $"and '{steps[i]}' accepts '{consumedBy.Input.Name}'.");
        }
    }

    private static string Names(IEnumerable<string> plans)
    {
        string declared = string.Join(", ", plans.Order(StringComparer.Ordinal));
        return declared.Length == 0 ? "none" : declared;
    }
}

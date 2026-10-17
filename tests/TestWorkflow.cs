namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

/// <summary>
/// The three-step workflow the tests are written against: a query is expanded into terms,
/// the terms are searched into documents, and the documents are ranked into an answer.
/// </summary>
public enum Step
{
    Expand,
    Search,
    Rank
}

public sealed record ExpandCommand(WorkflowContext<Step> Context) : IWorkflowCommand<Step>;

public sealed record SearchCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Terms)
    : IWorkflowCommand<Step>;

public sealed record RankCommand(WorkflowContext<Step> Context, IReadOnlyList<string> Documents)
    : IWorkflowCommand<Step>;

public static class TestWorkflow
{
    public const string FullSearch = "full-search";

    public static WorkflowBlueprint<Step> Blueprint() => new WorkflowBlueprint<Step>()
        .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
        .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(
            Step.Search, (context, terms) => new SearchCommand(context, terms))
        .Step<RankCommand, IReadOnlyList<string>, string>(
            Step.Rank, (context, documents) => new RankCommand(context, documents))
        .Plan(FullSearch, Step.Expand, Step.Search, Step.Rank);

    public static WorkflowContext<Step> Context(int currentStepIndex = 0) =>
        new(Guid.NewGuid(), "why is the sky blue", [Step.Expand, Step.Search, Step.Rank], currentStepIndex);
}

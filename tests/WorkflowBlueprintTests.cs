namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowBlueprintTests
{
    [Fact]
    public void Validate_PlanWhoseStepsFitTogether_Passes() =>
        TestWorkflow.Blueprint().Validate();

    /// <summary>
    /// The pairing of a step with the payload it accepts used to be a runtime pattern match inside
    /// each application's dispatcher, so a plan wired to the wrong payload was discovered by a job
    /// dying in production. It is now discovered by the application refusing to start.
    /// </summary>
    [Fact]
    public void Validate_StepPipedIntoOneThatCannotAcceptIt_NamesBothTypes()
    {
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, string>(Step.Expand, (context, _) => new ExpandCommand(context))
            .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(
                Step.Search, (context, terms) => new SearchCommand(context, terms))
            .Plan("broken", Step.Expand, Step.Search);

        var failure = Assert.Throws<WorkflowConfigurationException>(blueprint.Validate);

        Assert.Contains("String", failure.Message, StringComparison.Ordinal);
        Assert.Contains("IReadOnlyList`1", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_PlanUsingAnUndeclaredStep_NamesTheStep()
    {
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context))
            .Plan("broken", Step.Expand, Step.Rank);

        var failure = Assert.Throws<WorkflowConfigurationException>(blueprint.Validate);

        Assert.Contains("Rank", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A first step used to have to accept None, so a job could not be started with anything. What it is started
    /// with is now the caller's payload, checked when the job starts.
    /// </summary>
    [Fact]
    public void Validate_PlanStartingWithAStepThatExpectsAPayload_Passes()
    {
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<SearchCommand, IReadOnlyList<string>, IReadOnlyList<string>>(
                Step.Search, (context, terms) => new SearchCommand(context, terms))
            .Plan("search", Step.Search);

        blueprint.Validate();
    }

    [Fact]
    public void Validate_WorkflowWithoutSteps_IsRejected() =>
        Assert.Throws<WorkflowConfigurationException>(new WorkflowBlueprint<Step>().Validate);

    [Fact]
    public void Step_DeclaredTwice_IsRejected()
    {
        var blueprint = new WorkflowBlueprint<Step>()
            .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, (context, _) => new ExpandCommand(context));

        Assert.Throws<WorkflowConfigurationException>(() =>
            blueprint.Step<ExpandCommand, None, string>(Step.Expand, (context, _) => new ExpandCommand(context)));
    }

    [Fact]
    public void Plan_DeclaredTwice_IsRejected()
    {
        var blueprint = TestWorkflow.Blueprint();

        Assert.Throws<WorkflowConfigurationException>(() =>
            blueprint.Plan(TestWorkflow.FullSearch, Step.Expand));
    }

    [Fact]
    public void Steps_OfAnUnknownPlan_NamesWhatIsDeclared()
    {
        var failure = Assert.Throws<WorkflowConfigurationException>(() =>
            TestWorkflow.Blueprint().Steps("quick-scrape"));

        Assert.Contains(TestWorkflow.FullSearch, failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Steps_OfADeclaredPlan_AreItsSteps() =>
        Assert.Equal(
            [Step.Expand, Step.Search, Step.Rank],
            TestWorkflow.Blueprint().Steps(TestWorkflow.FullSearch));
}

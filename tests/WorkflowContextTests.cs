namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class WorkflowContextTests
{
    [Fact]
    public void Context_EmptyPlan_IsRejected()
    {
        var plan = Array.Empty<Step>();

        Assert.Throws<ArgumentException>(() => new WorkflowContext<Step>(Guid.NewGuid(), "query", plan));
    }

    [Fact]
    public void Context_IndexBeyondPlan_IsRejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new WorkflowContext<Step>(Guid.NewGuid(), "query", [Step.Expand], currentStepIndex: 1));

    /// <summary>
    /// A message whose json carries no steps used to deserialize into a context that threw
    /// NullReferenceException from every one of its members.
    /// </summary>
    [Fact]
    public void Context_PlanIsNull_IsRejected() =>
        Assert.Throws<ArgumentNullException>(() =>
            new WorkflowContext<Step>(Guid.NewGuid(), "query", null!));

    [Fact]
    public void CurrentStep_BeforeThePlanIsEntered_Throws()
    {
        var context = TestWorkflow.Context(currentStepIndex: -1);

        Assert.Throws<InvalidOperationException>(() => context.CurrentStep);
    }

    [Fact]
    public void MoveNext_OnTheLastStep_Throws()
    {
        var context = TestWorkflow.Context(currentStepIndex: 2);

        Assert.Throws<InvalidOperationException>(() => context.MoveNext());
    }

    [Fact]
    public void MoveNext_MidPlan_AdvancesOneStep()
    {
        var context = TestWorkflow.Context(currentStepIndex: 0);

        var next = context.MoveNext();

        Assert.Equal(Step.Search, next.CurrentStep);
        Assert.Equal(Step.Expand, next.PreviousStep);
    }

    /// <summary>
    /// The plan used to be stored as the caller's own mutable list, shared by every context the
    /// job produced, so anyone holding the list could rewrite the plan of a running job.
    /// </summary>
    [Fact]
    public void Plan_MutatedByItsCallerAfterwards_DoesNotChangeTheContext()
    {
        var plan = new List<Step> { Step.Expand, Step.Search };
        var context = new WorkflowContext<Step>(Guid.NewGuid(), "query", plan);

        plan.Add(Step.Rank);

        Assert.Equal(2, context.Steps.Count);
    }

    /// <summary>
    /// Equality compared the list by reference, so a context and its own round trip through the
    /// broker never compared equal and nothing could be deduplicated by it.
    /// </summary>
    [Fact]
    public void Contexts_SamePositionAndPlanInSeparateLists_AreEqual()
    {
        var jobId = Guid.NewGuid();
        var one = new WorkflowContext<Step>(jobId, "query", [Step.Expand, Step.Search]);
        var other = new WorkflowContext<Step>(jobId, "query", [Step.Expand, Step.Search]);

        Assert.Equal(one, other);
        Assert.Equal(one.GetHashCode(), other.GetHashCode());
    }

    [Fact]
    public void Contexts_DifferentPositionInTheSamePlan_AreNotEqual()
    {
        var jobId = Guid.NewGuid();
        var one = new WorkflowContext<Step>(jobId, "query", [Step.Expand, Step.Search]);
        var other = one.MoveNext();

        Assert.NotEqual(one, other);
    }

    [Fact]
    public void IsFinished_OnTheLastStep_IsTrue() =>
        Assert.True(TestWorkflow.Context(currentStepIndex: 2).IsFinished);

    [Fact]
    public void IsFinished_MidPlan_IsFalse() =>
        Assert.False(TestWorkflow.Context(currentStepIndex: 1).IsFinished);
}

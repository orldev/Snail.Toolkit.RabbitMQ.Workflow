namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

public class WorkflowContextChaosTests
{
    /// <summary>
    /// The validation used to live in the constructor only, and every property has a public <c>init</c>, so a
    /// <c>with</c> expression built a context the constructor never saw: its CurrentStep threw an index error, and a
    /// dispatcher handed it failed a job for a reason no one wrote.
    /// </summary>
    [Fact]
    public void Context_MovedPastItsPlanWithAWithExpression_IsRejected()
    {
        var context = TestWorkflow.Context(currentStepIndex: 0);

        var moved = Record.Exception(() => context with { CurrentStepIndex = 3 });

        Assert.IsType<ArgumentOutOfRangeException>(moved);
    }

    /// <summary>
    /// The same hole for the plan: an empty plan was refused by the constructor and accepted by <c>with</c>, and such a
    /// context reported itself finished before it ever started.
    /// </summary>
    [Fact]
    public void Context_GivenAnEmptyPlanWithAWithExpression_IsRejected()
    {
        var context = TestWorkflow.Context(currentStepIndex: -1);

        var emptied = Record.Exception(() => context with { Steps = [] });

        Assert.IsType<ArgumentException>(emptied);
    }
}

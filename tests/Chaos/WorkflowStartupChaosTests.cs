using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Chaos;

public class WorkflowStartupChaosTests
{
    /// <summary>
    /// The deadline reaches a timer only when the first message of a job arrives, and it used to be checked nowhere
    /// before that. Zero cancelled every job on arrival; a negative span other than infinite, or one past the timer's
    /// limit of about 49.7 days, made every <see cref="IJobCancellation.Token"/> call throw, which the consumer took
    /// for a transient failure, so every step was retried until the broker dropped it.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(-1_000L)]
    [InlineData(5_184_000_000L)]
    public void AddWorkflow_JobLifetimeNoTimerCanHold_IsRejectedAtStartup(long milliseconds)
    {
        var services = new ServiceCollection();

        void Declare(WorkflowBlueprint<Step> workflow)
        {
            workflow.Options.JobLifetime = TimeSpan.FromMilliseconds(milliseconds);
            workflow
                .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, Expand)
                .Plan("expand", Step.Expand);
        }

        Assert.Throws<WorkflowConfigurationException>(() => services.AddWorkflow<Step>(Declare));
    }

    private static ExpandCommand Expand(WorkflowContext<Step> context, None _) => new(context);

    private static RankCommand Rank(WorkflowContext<Step> context, IReadOnlyList<string> documents) =>
        new(context, documents);

    /// <summary>
    /// The blueprint is validated once, inside <c>AddWorkflow</c>, and used to stay open afterwards: a plan added to it
    /// later was never checked, and its first mismatch surfaced as a failed job in production — the outcome the startup
    /// check exists to prevent — while every dispatch read its dictionaries without a lock.
    /// </summary>
    [Fact]
    public void Blueprint_ChangedAfterItWasValidated_IsRefused()
    {
        WorkflowBlueprint<Step>? declared = null;
        new ServiceCollection().AddWorkflow<Step>(workflow =>
        {
            declared = workflow;
            workflow
                .Step<ExpandCommand, None, IReadOnlyList<string>>(Step.Expand, Expand)
                .Step<RankCommand, IReadOnlyList<string>, string>(Step.Rank, Rank)
                .Plan("expand-rank", Step.Expand, Step.Rank);
        });

        var late = Record.Exception(() => declared!.Plan("rank-first", Step.Rank));

        Assert.NotNull(late);
    }
}

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

/// <summary>
/// The context travels inside every message, written and read by the toolkit's serializer, whose default options are
/// plain reflection-based ones; these tests use the same.
/// </summary>
public class WorkflowContextJsonTests
{
    private static readonly JsonSerializerOptions Json = new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    /// <summary>
    /// The context validates in its accessors, and the serializer builds it through its constructor; a context that
    /// did not survive the trip would fail every step after the first.
    /// </summary>
    [Fact]
    public void Message_WrittenAndReadBack_CarriesTheSameContext()
    {
        WorkflowContext<Step> context = TestWorkflow.Context(currentStepIndex: 1) with
        {
            StartedAt = new DateTime(2026, 10, 23, 9, 0, 0, DateTimeKind.Utc)
        };
        var sent = new SearchCommand(context, ["blue sky"]);

        SearchCommand? received = JsonSerializer.Deserialize<SearchCommand>(JsonSerializer.Serialize(sent, Json), Json);

        Assert.Equal(sent.Context, received!.Context);
    }

    /// <summary>
    /// A job's deadline carries the job as it was started, before its first step. Writing that context read every
    /// public property, and the current step of a job not yet started throws — so no deadline was ever published.
    /// </summary>
    [Fact]
    public void Deadline_OfAJobNotYetOnAStep_IsWrittenAndReadBack()
    {
        var sent = new WorkflowDeadline<Step>(TestWorkflow.Context(currentStepIndex: -1));

        WorkflowDeadline<Step>? received = JsonSerializer.Deserialize<WorkflowDeadline<Step>>(JsonSerializer.Serialize(sent, Json), Json);

        Assert.Equal(sent.Context, received!.Context);
    }

    [Fact]
    public void Message_PositionedPastItsPlan_IsRefusedWhenRead()
    {
        string json = JsonSerializer.Serialize(new SearchCommand(TestWorkflow.Context(currentStepIndex: 2), []), Json)
            .Replace("\"CurrentStepIndex\":2", "\"CurrentStepIndex\":7", StringComparison.Ordinal);

        Assert.Throws<ArgumentOutOfRangeException>(() => JsonSerializer.Deserialize<SearchCommand>(json, Json));
    }
}

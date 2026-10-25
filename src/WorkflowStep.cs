using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// One declared step: its message type, the payload it accepts, the payload it produces, and how it is published.
/// </summary>
/// <remarks>
/// <see cref="Publish"/> takes any payload and casts it to the accepted type; <see cref="PublishAccepted"/> is the same
/// publish typed by the accepted payload, a <c>Func</c> of the context, that payload, the producer and the token, so a
/// result already of that type is handed over as it is, without a box.
/// </remarks>
internal sealed record WorkflowStep<TStep>(
    Type Command,
    Type Input,
    Type Output,
    Func<WorkflowContext<TStep>, object?, IRabbitProducer, CancellationToken, ValueTask> Publish,
    Delegate PublishAccepted)
    where TStep : struct, Enum;

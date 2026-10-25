using Snail.Toolkit.RabbitMQ.Queues;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Extensions;

/// <summary>
/// Declares a queue that carries one step of a workflow.
/// </summary>
public static class RabbitQueueBuilderExtensions
{
    extension<T>(IRabbitQueueBuilder<T> queue)
    {
        /// <summary>
        /// Declares the queue durable and retrying, as a workflow step requires.
        /// </summary>
        /// <param name="delay">How long a failed step waits before it is tried again.</param>
        /// <param name="maxAttempts">Attempts per message before the broker gives up on it.</param>
        /// <param name="parkTo">
        /// The queue that keeps a step whose attempts ran out; the step's queue name with <c>.parked</c> when not given.
        /// </param>
        /// <remarks>
        /// A workflow consumer rethrows transient failures on the assumption that the broker will
        /// bring the message back. On a queue with neither retry nor a dead letter exchange that
        /// assumption is false — the rejected message is discarded silently — and the job it
        /// belonged to hangs forever. A host whose step queue was declared otherwise refuses to start.
        /// <para>
        /// A step whose attempts run out used to be acknowledged and dropped, its job left hanging. It is
        /// parked now, so the message that could finish the job is still there to inspect and replay, and
        /// its job is reported failed. Size <paramref name="maxAttempts"/> so that outcome stays rare.
        /// </para>
        /// </remarks>
        public IRabbitQueueBuilder<T> AsWorkflowStep(TimeSpan delay, int maxAttempts = 3, string? parkTo = null) =>
            queue.Durable().WithRetry(delay, maxAttempts, parkTo ?? $"{queue.Declaration.Name}.parked");
    }
}

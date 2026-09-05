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
        /// <remarks>
        /// A workflow consumer rethrows transient failures on the assumption that the broker will
        /// bring the message back. On a queue with neither retry nor a dead letter exchange that
        /// assumption is false — the rejected message is discarded silently — and the job it
        /// belonged to hangs forever. The library cannot check this for itself, because the toolkit
        /// keeps its queue declarations internal, so this is the one obvious way to get it right.
        /// <para>
        /// A step whose attempts run out is dropped by the broker with a logged error and no
        /// terminal event, which leaves its job hanging. Until job state lives somewhere durable,
        /// size <paramref name="maxAttempts"/> so that outcome stays rare, and watch the log.
        /// </para>
        /// </remarks>
        public IRabbitQueueBuilder<T> AsWorkflowStep(TimeSpan delay, int maxAttempts = 3) =>
            queue.Durable().WithRetry(delay, maxAttempts);
    }
}

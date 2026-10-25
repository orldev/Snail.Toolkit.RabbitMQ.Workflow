using Microsoft.Extensions.DependencyInjection.Extensions;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Extensions;

/// <summary>
/// Declares the queues that bring a job's deadline back.
/// </summary>
public static class RabbitConnectionBuilderExtensions
{
    extension(IRabbitConnectionBuilder connection)
    {
        /// <summary>
        /// Gives every job of the workflow a deadline message, and a consumer that ends the job when it comes back.
        /// </summary>
        /// <param name="queue">The queue the deadlines come back on; they wait out the lifetime in <c>.waiting</c> beside it.</param>
        /// <remarks>
        /// A deadline is published into the waiting queue with the job's lifetime as its time to live; when it expires,
        /// the broker moves it to <paramref name="queue"/>, where one worker ends the job unless the ledger says it
        /// ended already. Every message of the waiting queue lives as long as the next, so they expire in the order
        /// they wait. The ledger has to be shared by the fleet — the startup check refuses a deadline without one —
        /// because only a shared ledger tells a job that ended on another worker from one still running.
        /// </remarks>
        public IRabbitConnectionBuilder AddWorkflowDeadline<TStep>(string queue)
            where TStep : struct, Enum
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queue);

            string waiting = $"{queue}.waiting";
            connection.AddQueue<WorkflowDeadline<TStep>>(waiting)
                .Durable()
                .Argument("x-dead-letter-exchange", string.Empty)
                .Argument("x-dead-letter-routing-key", queue);

            IRabbitQueueBuilder<WorkflowDeadline<TStep>> due = connection.AddQueue<WorkflowDeadline<TStep>>(queue).AsWorkflowStep(TimeSpan.FromSeconds(10));
            connection.AddConsumer(due).Subscribe<WorkflowDeadline<TStep>, WorkflowDeadlineConsumer<TStep>>();
            connection.AddProducer<WorkflowDeadline<TStep>>().RoutedTo(waiting).Persistent();
            connection.Services.TryAddSingleton<WorkflowDeadlines<TStep>>();

            return connection;
        }
    }
}

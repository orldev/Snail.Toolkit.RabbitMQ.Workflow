using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Snail.Toolkit.RabbitMQ.Workflow.Extensions;

/// <summary>
/// Registers a workflow and everything it needs to run.
/// </summary>
/// <remarks>
/// The library used to register nothing at all, leaving each application to work out that
/// cancellation has to be a singleton — get that wrong and every job silently becomes
/// uncancellable, with nothing failing to say so. There is now one way in.
/// </remarks>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Declares a workflow's steps and plans, proves they fit together, and registers it.
        /// </summary>
        /// <exception cref="WorkflowConfigurationException">
        /// A plan uses an undeclared step, or pipes a step into one that cannot accept what it produces.
        /// </exception>
        public IServiceCollection AddWorkflow<TStep>(Action<WorkflowBlueprint<TStep>> workflow)
            where TStep : struct, Enum
        {
            ArgumentNullException.ThrowIfNull(workflow);

            var blueprint = new WorkflowBlueprint<TStep>();
            workflow(blueprint);
            blueprint.Validate();

            return services
                .AddJobCancellation(blueprint.Options)
                .AddWorkflowDispatch(blueprint);
        }

        /// <summary>
        /// Registers the per-process cancellation and deadline of running jobs.
        /// </summary>
        /// <remarks>
        /// Singleton by construction: a scoped registry would hand every message its own map and
        /// lose every cancellation without a word.
        /// </remarks>
        public IServiceCollection AddJobCancellation(WorkflowOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            services.TryAddSingleton(TimeProvider.System);
            services.TryAddSingleton(options);
            services.TryAddSingleton<IJobCancellation, JobCancellation>();

            return services;
        }

        /// <summary>
        /// Registers the dispatcher that walks a blueprint, and a silent notifier unless one is already registered.
        /// </summary>
        public IServiceCollection AddWorkflowDispatch<TStep>(WorkflowBlueprint<TStep> blueprint)
            where TStep : struct, Enum
        {
            ArgumentNullException.ThrowIfNull(blueprint);

            services.TryAddSingleton(blueprint);
            services.TryAddSingleton<IWorkflowNotifier<TStep>, SilentNotifier<TStep>>();
            services.TryAddSingleton<IWorkflowDispatcher<TStep>, WorkflowDispatcher<TStep>>();

            return services;
        }
    }
}

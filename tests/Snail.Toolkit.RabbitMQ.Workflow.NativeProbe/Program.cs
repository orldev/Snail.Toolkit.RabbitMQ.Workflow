using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Workflow;
using Snail.Toolkit.RabbitMQ.Workflow.Extensions;
using Snail.Toolkit.RabbitMQ.Workflow.NativeProbe;

var producer = new KeepingProducer();
var services = new ServiceCollection();

services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Debug));
services.AddSingleton<IRabbitProducer>(producer);
services.AddWorkflow<Step>(workflow => workflow
    .Step<SearchCommand, None, string>(Step.Search, static (context, _) => new SearchCommand(context))
    .Step<AnswerCommand, string, string>(Step.Answer, static (context, found) => new AnswerCommand(context, found))
    .Plan("full", Step.Search, Step.Answer));
services.AddTransient<SearchStep>();

await using var provider = services.BuildServiceProvider();
var job = Guid.NewGuid();

await provider.GetRequiredService<IWorkflowDispatcher<Step>>().StartAsync("full", job, "what is the capital of France");
await provider.GetRequiredService<SearchStep>().HandleAsync((SearchCommand)producer.Published[0], CancellationToken.None);

var isWalked = producer.Published is [SearchCommand, AnswerCommand { Found: "found Paris" } answer]
    && answer.Context.JobId == job
    && answer.Context.CurrentStep == Step.Answer;

Console.WriteLine($"published {producer.Published.Count} step message(s): {string.Join(", ", producer.Published)}");

return isWalked ? 0 : 1;

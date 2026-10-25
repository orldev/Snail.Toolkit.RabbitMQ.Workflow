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
services.AddWorkflow<Step>(Declared.Workflow);
services.AddTransient<SearchStep>();

await using var provider = services.BuildServiceProvider();
var job = Guid.NewGuid();

await provider.GetRequiredService<IWorkflowDispatcher<Step>>().StartAsync("full", job, "what is the capital of France");
await provider.GetRequiredService<SearchStep>().HandleAsync((SearchCommand)producer.Published[0], CancellationToken.None);
await provider.GetRequiredService<SearchStep>().HandleAsync((SearchCommand)producer.Published[0], CancellationToken.None);

var isWalked = producer.Published is [SearchCommand { Question: "what is the capital of France" }, AnswerCommand { Found: "found Paris" } answer]
    && answer.Context.JobId == job
    && answer.Context.CurrentStep == Step.Answer;

Console.WriteLine($"published {producer.Published.Count} step message(s): {string.Join(", ", producer.Published)}");

var isFitPassed = await StartupCheck.PassesAsync<SearchStep>();
var isMisfitRefused = !await StartupCheck.PassesAsync<MisfitStep>();

Console.WriteLine($"startup check: a fitting consumer passed {isFitPassed}, a misfit refused {isMisfitRefused}");

return isWalked && isFitPassed && isMisfitRefused ? 0 : 1;

using System.Diagnostics;
using Testcontainers.RabbitMq;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests.Broker;

/// <summary>
/// A test that needs a broker in a container; skipped only when no Docker daemon answers.
/// </summary>
public sealed class BrokerFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> IsDockerRunning = new(AnswersDocker);

    public BrokerFactAttribute()
    {
        if (!IsDockerRunning.Value)
            Skip = "Docker is not available; the broker tests were skipped.";
    }

    private static bool AnswersDocker()
    {
        try
        {
            using Process? docker = Process.Start(new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            });

            if (docker is null)
                return false;

            if (docker.WaitForExit(10_000))
                return docker.ExitCode == 0;

            docker.Kill(entireProcessTree: true);
            return false;
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            return false;
        }
    }
}

/// <summary>
/// One RabbitMQ container, started by the first test that asks for it and shared by the tests of a class.
/// </summary>
/// <remarks>The tests start the container themselves, and Testcontainers removes it when they end.</remarks>
public sealed class BrokerFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private RabbitMqContainer? _container;

    public async Task<string> ConnectionStringAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_container is null)
            {
                _container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
                await _container.StartAsync();
            }

            return _container.GetConnectionString();
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();

        _gate.Dispose();
    }
}

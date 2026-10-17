using System.Text.Json;
using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.RabbitMQ.Workflow.Benchmarks;

/// <summary>A step's message on the wire: written and read as RabbitMQ's default serializer does, the context inside.</summary>
[MemoryDiagnoser]
public class Messages
{
    private readonly RankCommand _command = new(Search.Context(1), "found 12 documents");

    private byte[] _body = [];

    [GlobalSetup]
    public void Setup() => _body = JsonSerializer.SerializeToUtf8Bytes<object>(_command);

    [Benchmark]
    public byte[] Write() => JsonSerializer.SerializeToUtf8Bytes<object>(_command);

    [Benchmark]
    public object? Read() => JsonSerializer.Deserialize(_body, typeof(RankCommand));
}

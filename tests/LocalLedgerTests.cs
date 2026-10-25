using Microsoft.Extensions.Time.Testing;

namespace Snail.Toolkit.RabbitMQ.Workflow.Tests;

public class LocalLedgerTests
{
    private static readonly TimeSpan KeepFor = TimeSpan.FromMinutes(20);

    private readonly FakeTimeProvider _time = new();
    private readonly LocalLedger _ledger;
    private readonly Guid _job = Guid.NewGuid();

    public LocalLedgerTests() => _ledger = new LocalLedger(_time);

    [Fact]
    public async Task Start_TheSameJobTwice_IsTakenOnce()
    {
        bool first = await _ledger.TryHandOverAsync(_job, -1, KeepFor);
        bool second = await _ledger.TryHandOverAsync(_job, -1, KeepFor);

        Assert.Equal([true, false], [first, second]);
    }

    [Fact]
    public async Task HandOver_TheSameStepTwice_IsTakenOnce()
    {
        bool first = await _ledger.TryHandOverAsync(_job, 1, KeepFor);
        bool second = await _ledger.TryHandOverAsync(_job, 1, KeepFor);

        Assert.Equal([true, false], [first, second]);
    }

    /// <summary>
    /// A plan is a line: once a later step is handed on, an earlier one arriving again is a duplicate.
    /// </summary>
    [Fact]
    public async Task HandOver_OfAStepBeforeOneHandedOn_IsRefused()
    {
        await _ledger.TryHandOverAsync(_job, 0, KeepFor);
        await _ledger.TryHandOverAsync(_job, 2, KeepFor);

        Assert.False(await _ledger.TryHandOverAsync(_job, 1, KeepFor));
    }

    [Fact]
    public async Task HandOver_OfAnEndedJob_IsRefused()
    {
        await _ledger.TryEndAsync(_job, KeepFor);

        Assert.False(await _ledger.TryHandOverAsync(_job, 0, KeepFor));
    }

    [Fact]
    public async Task End_TheSameJobTwice_IsTakenOnce()
    {
        bool first = await _ledger.TryEndAsync(_job, KeepFor);
        bool second = await _ledger.TryEndAsync(_job, KeepFor);

        Assert.Equal([true, false], [first, second]);
    }

    /// <summary>
    /// A hand-over whose message could not be published is taken back, so the step's retry hands on instead of
    /// being dropped as its own duplicate.
    /// </summary>
    [Fact]
    public async Task HandOver_Released_CanBeTakenAgain()
    {
        await _ledger.TryHandOverAsync(_job, 1, KeepFor);

        await _ledger.ReleaseHandOverAsync(_job, 1);

        Assert.True(await _ledger.TryHandOverAsync(_job, 1, KeepFor));
    }

    [Fact]
    public async Task HandOver_AskedByManyAtOnce_IsTakenOnce()
    {
        bool[] taken = await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(async () =>
            await _ledger.TryHandOverAsync(_job, 3, KeepFor))));

        Assert.Single(taken, isTaken => isTaken);
    }

    [Fact]
    public async Task Marks_KeptPastTheirTime_AreForgottenByTheNextSweep()
    {
        await _ledger.TryHandOverAsync(_job, -1, KeepFor);

        _time.Advance(KeepFor + TimeSpan.FromMinutes(1));
        await _ledger.TryHandOverAsync(Guid.NewGuid(), -1, KeepFor);

        Assert.True(await _ledger.TryHandOverAsync(_job, -1, KeepFor));
    }
}

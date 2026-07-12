using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class LeaseNegotiationTests : IAsyncLifetime
{
    private readonly TestCluster _cluster = new();

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _cluster.DisposeAsync();

    [Fact]
    public async Task Stopping_a_node_cancels_its_held_lease_lost_tokens()
    {
        var node = await _cluster.StartNodeAsync();
        var acq = await node.TryAcquireAsync("t", "1");
        Assert.True(acq.Granted);
        var lost = acq.Lease!.Lost;
        Assert.False(lost.IsCancellationRequested);

        await _cluster.StopNodeAsync(node);

        Assert.True(lost.IsCancellationRequested,
            "held-lease Lost tokens must fire on shutdown so external-write guards halt");
    }

    [Fact]
    public async Task Member_self_fences_at_the_coordinators_shorter_lease_ttl()
    {
        // The coordinator keeps leases only ~2 s; the member is (mis)configured with a much longer
        // TTL. Partitioned into the minority, the member must self-fence at the COORDINATOR's TTL,
        // not its own — otherwise it still believes it owns K long after the coordinator reclaimed it
        // (dual ownership under a heterogeneous / rolling LeaseTtl).
        var coordinator = await _cluster.StartNodeAsync(o => o.LeaseTtl = TimeSpan.FromSeconds(2 * TestCluster.Scale));
        var member = await _cluster.StartNodeAsync(o => o.LeaseTtl = TimeSpan.FromSeconds(15 * TestCluster.Scale));
        var third = await _cluster.StartNodeAsync(o => o.LeaseTtl = TimeSpan.FromSeconds(2 * TestCluster.Scale));
        await TestCluster.WaitUntilAsync(() => coordinator.Members.Count == 3);
        Assert.True(await member.CanProcessAsync("t", "K", ProcessingPreference.This));

        _cluster.Partition([coordinator, third], [member]);

        await TestCluster.WaitUntilAsync(() => !member.HeldLeases.Contains(new LeaseKey("t", "K")),
            timeout: TimeSpan.FromSeconds(8 * TestCluster.Scale),
            because: "the member should drop K at the coordinator's ~2 s TTL, well before its own 15 s");
    }

    [Fact]
    public async Task Exactly_one_node_can_process_a_key()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();
        var c = await _cluster.StartNodeAsync();
        CoLibraNode[] nodes = [a, b, c];

        var answers = await Task.WhenAll(nodes.Select(n =>
            n.CanProcessAsync("sourceid", "source_1", ProcessingPreference.This).AsTask()));

        Assert.Equal(1, answers.Count(x => x));

        // The answer is stable on repeat (served from the local caches).
        var repeat = await Task.WhenAll(nodes.Select(n =>
            n.CanProcessAsync("sourceid", "source_1", ProcessingPreference.This).AsTask()));
        Assert.Equal(answers, repeat);
    }

    [Fact]
    public async Task Repeat_calls_are_local_cache_hits()
    {
        var node = await _cluster.StartNodeAsync();
        Assert.True(await node.CanProcessAsync("t", "1"));

        // Once granted, the fast path completes synchronously.
        var fast = node.CanProcessAsync("t", "1");
        Assert.True(fast.IsCompletedSuccessfully);
        Assert.True(await fast);
    }

    [Fact]
    public async Task Released_key_becomes_available_to_the_other_node()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();

        Assert.True(await a.CanProcessAsync("t", "1", ProcessingPreference.This));
        Assert.False(await b.CanProcessAsync("t", "1", ProcessingPreference.This));

        var available = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        b.LeaseAvailable += (_, e) =>
        {
            if (e.Keys.Contains(new LeaseKey("t", "1")))
                available.TrySetResult();
        };

        await a.ReleaseAsync("t", "1");
        await available.Task.WaitAsync(TestCluster.Eventually);
        await TestCluster.WaitUntilAsync(
            () => b.CanProcessAsync("t", "1", ProcessingPreference.This).AsTask().GetAwaiter().GetResult(),
            because: "the released key should be grantable to B");
        Assert.False(await a.CanProcessAsync("t", "1"));
    }

    [Fact]
    public async Task Balanced_grants_steer_toward_the_less_loaded_node()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();

        Assert.True(await a.CanProcessAsync("t", "1", ProcessingPreference.This));
        Assert.True(await a.CanProcessAsync("t", "2", ProcessingPreference.This));

        // A holds 2, B holds 0 (beyond tolerance 1): a Balanced request from A is steered away...
        Assert.False(await a.CanProcessAsync("t", "3", ProcessingPreference.Balanced));
        // ...and the same key is granted to B.
        Assert.True(await b.CanProcessAsync("t", "3", ProcessingPreference.Balanced));
    }

    [Fact]
    public async Task Other_preference_yields_to_a_willing_node()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();

        var reluctant = a.CanProcessAsync("t", "1", ProcessingPreference.Other).AsTask();
        await Task.Delay(50); // let the deferred request park on the coordinator
        var willing = await b.CanProcessAsync("t", "1", ProcessingPreference.This);

        Assert.True(willing);
        Assert.False(await reluctant.WaitAsync(TestCluster.Eventually));
    }

    [Fact]
    public async Task Other_preference_still_gets_the_work_when_nobody_else_wants_it()
    {
        var a = await _cluster.StartNodeAsync();
        _ = await _cluster.StartNodeAsync(); // a second node exists but never asks

        Assert.True(await a.CanProcessAsync("t", "1", ProcessingPreference.Other)
            .AsTask().WaitAsync(TestCluster.Eventually));
    }

    [Fact]
    public async Task Explicit_lease_exposes_fencing_token_and_releases_on_dispose()
    {
        var a = await _cluster.StartNodeAsync();
        var b = await _cluster.StartNodeAsync();

        var acquisition = await a.TryAcquireAsync("t", "1");
        Assert.True(acquisition.Granted);
        var lease = acquisition.Lease!;
        Assert.True(lease.IsHeld);
        Assert.True(lease.Token.Term > 0);

        var denied = await b.TryAcquireAsync("t", "1");
        Assert.False(denied.Granted);
        Assert.Equal(LeaseDenialReason.HeldByOther, denied.DenialReason);
        Assert.Equal(a.LocalNodeId, denied.CurrentOwner);

        await lease.DisposeAsync();
        await TestCluster.WaitUntilAsync(
            () => b.CanProcessAsync("t", "1", ProcessingPreference.This).AsTask().GetAwaiter().GetResult(),
            because: "disposing the lease should free the key for B");
    }

    [Fact]
    public async Task Isolated_member_stops_processing_before_the_key_is_regranted()
    {
        // Three nodes so the isolated member cannot claim a quorum of its own
        // (a 2-node cluster deliberately allows both halves to continue).
        var coordinator = await _cluster.StartNodeAsync();
        var survivor = await _cluster.StartNodeAsync();
        var isolated = await _cluster.StartNodeAsync();

        Assert.True(await isolated.CanProcessAsync("t", "1", ProcessingPreference.This));

        var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        isolated.LeaseLost += (_, e) =>
        {
            if (e.Key == new LeaseKey("t", "1"))
                lost.TrySetResult();
        };

        _cluster.Partition([coordinator, survivor], [isolated]);

        // The isolated member must flip to false via its local safety margin...
        await lost.Task.WaitAsync(TestCluster.Eventually);
        Assert.False(await ExpectNoThrowOrFalse(isolated));

        // ...before/at the point the coordinator's TTL frees the key for the survivor.
        await TestCluster.WaitUntilAsync(
            () => survivor.CanProcessAsync("t", "1", ProcessingPreference.This).AsTask().GetAwaiter().GetResult(),
            because: "after TTL expiry the coordinator can grant the key to the survivor");
    }

    private static async Task<bool> ExpectNoThrowOrFalse(CoLibraNode node)
    {
        try
        {
            return await node.CanProcessAsync("t", "1");
        }
        catch (SplitBrainException)
        {
            return false; // policy-dependent; either way the node is not processing
        }
    }
}

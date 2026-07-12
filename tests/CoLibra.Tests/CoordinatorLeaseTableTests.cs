using CoLibra.Leasing;
using Microsoft.Extensions.Time.Testing;

namespace CoLibra.Tests;

public class CoordinatorLeaseTableTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly NodeId _nodeA = new(Guid.Parse("aaaaaaaa-0000-0000-0000-000000000000"));
    private readonly NodeId _nodeB = new(Guid.Parse("bbbbbbbb-0000-0000-0000-000000000000"));
    private readonly NodeId _nodeC = new(Guid.Parse("cccccccc-0000-0000-0000-000000000000"));
    private static readonly LeaseKey Key = new("sourceid", "source_1");

    private CoordinatorLeaseTable Table(Action<CoLibraOptions>? mutate = null, long term = 1)
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "x" };
        mutate?.Invoke(options);
        return new CoordinatorLeaseTable(term, options, _time);
    }

    private long Now() => _time.GetTimestamp();

    [Fact]
    public void Grants_free_key_and_is_idempotent_for_same_owner()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);

        var first = table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.Balanced, Now());
        Assert.True(first.Immediate!.Value.Granted);

        var again = table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.Balanced, Now());
        Assert.True(again.Immediate!.Value.Granted);
        Assert.Equal(first.Immediate.Value.Token, again.Immediate.Value.Token);
        Assert.Equal(1, table.LeaseCount);
    }

    [Fact]
    public void Revoked_key_stays_held_down_until_the_old_owner_would_self_fence()
    {
        // The revocation hold-down must outlast the old owner's self-fence horizon
        // (LeaseTtl - LeaseRenewSafetyMargin), or a re-grant can race an owner that never heard the
        // revocation (lost push + stalled renewals) and both hold the key at once.
        var table = Table(o => o.LoadBalanceTolerance = 0); // defaults: LeaseTtl 15s, margin 3s, heartbeat 1s
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        var k1 = new LeaseKey("t", "1");
        var k2 = new LeaseKey("t", "2");
        table.Acquire(_nodeA, Guid.NewGuid(), k1, ProcessingPreference.This, Now());
        table.Acquire(_nodeA, Guid.NewGuid(), k2, ProcessingPreference.This, Now());

        var revoked = table.ForceRebalance(null, Now());
        Assert.Single(revoked); // A sheds its newest key down to the mean
        var shedKey = revoked[0].Key;

        // Between the old 2.5x-heartbeat hold-down (2.5 s) and the self-fence horizon (12 s), the
        // key must NOT be regrantable: the old owner may still believe it holds it.
        _time.Advance(TimeSpan.FromSeconds(5));
        var early = table.Acquire(_nodeB, Guid.NewGuid(), shedKey, ProcessingPreference.This, Now()).Immediate!.Value;
        Assert.False(early.Granted, "revoked key was regrantable before the old owner's self-fence horizon");

        // Past the self-fence horizon it is safely acquirable by another node.
        _time.Advance(TimeSpan.FromSeconds(9)); // total 14 s > 12 s
        var late = table.Acquire(_nodeB, Guid.NewGuid(), shedKey, ProcessingPreference.This, Now()).Immediate!.Value;
        Assert.True(late.Granted);
    }

    [Fact]
    public void Grants_never_regress_below_an_asserted_higher_term_token()
    {
        // A new coordinator elected with a LOW term (it was partitioned during a higher term) adopts
        // a member's held lease carrying a token from that higher term. A later grant of the SAME key
        // must still produce a strictly greater fencing token — otherwise an external system fenced on
        // the high token would reject the legitimate new owner (and accept a stale writer).
        var table = Table(term: 6); // elected low, having missed term 9
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        var highToken = new FencingToken(9, 42); // node A held this from term 9
        table.AssertHeld(_nodeA, [(Key, highToken)], Now());

        table.Release(_nodeA, Key, out _);
        var regrant = table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.This, Now()).Immediate!.Value;

        Assert.True(regrant.Granted);
        Assert.True(regrant.Token > highToken, $"fencing token regressed: {regrant.Token} <= asserted {highToken}");
    }

    [Fact]
    public void Denies_key_held_by_another_node_and_reports_owner()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.This, Now());

        var denied = table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.This, Now()).Immediate!.Value;
        Assert.False(denied.Granted);
        Assert.Equal(LeaseDenialReason.HeldByOther, denied.Reason);
        Assert.Equal(_nodeA, denied.CurrentOwner);
    }

    [Fact]
    public void Fencing_tokens_are_monotonic_within_a_term()
    {
        var table = Table(term: 5);
        table.NodeUp(_nodeA, 1);
        var t1 = table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "1"), ProcessingPreference.This, Now()).Immediate!.Value.Token;
        var t2 = table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "2"), ProcessingPreference.This, Now()).Immediate!.Value.Token;
        Assert.Equal(5, t1.Term);
        Assert.True(t2 > t1);
    }

    [Fact]
    public void Release_frees_key_and_returns_interested_nodes()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.This, Now());
        table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.This, Now()); // denied -> interest

        Assert.True(table.Release(_nodeA, Key, out var interested));
        Assert.Equal([_nodeB], interested);
        Assert.Equal(0, table.LeaseCount);
    }

    [Fact]
    public void Expiry_sweep_frees_unrenewed_leases_and_notifies_interest()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "x" };
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.This, Now());
        table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.This, Now());

        _time.Advance(options.LeaseTtl + TimeSpan.FromSeconds(1));
        var sweep = table.Sweep(Now());

        var freed = Assert.Single(sweep.Freed);
        Assert.Equal(Key, freed.Key);
        Assert.Equal([_nodeB], freed.Interested);
    }

    [Fact]
    public void Renewal_extends_the_deadline()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "x" };
        var table = Table();
        table.NodeUp(_nodeA, 1);
        var token = table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.This, Now()).Immediate!.Value.Token;

        _time.Advance(options.LeaseTtl - TimeSpan.FromSeconds(1));
        var lost = table.Renew(_nodeA, [(Key, token)], Now());
        Assert.Empty(lost);

        _time.Advance(options.LeaseTtl - TimeSpan.FromSeconds(1));
        Assert.Empty(table.Sweep(Now()).Freed);
    }

    [Fact]
    public void Renewal_reports_keys_now_owned_by_someone_else()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        var oldToken = new FencingToken(0, 5);
        var newToken = new FencingToken(1, 1);
        table.AssertHeld(_nodeB, [(Key, newToken)], Now());

        var lost = table.Renew(_nodeA, [(Key, oldToken)], Now());
        Assert.Equal([Key], lost);
    }

    [Fact]
    public void Assert_conflicts_resolve_by_fencing_token()
    {
        var table = Table(term: 3);
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);

        // A asserts with the newer token first; B's stale assert is rejected.
        table.AssertHeld(_nodeA, [(Key, new FencingToken(2, 9))], Now());
        var rejected = table.AssertHeld(_nodeB, [(Key, new FencingToken(1, 4))], Now());
        Assert.Equal([Key], rejected);

        // A newer token beats an existing older one.
        var key2 = new LeaseKey("sourceid", "source_2");
        table.AssertHeld(_nodeA, [(key2, new FencingToken(1, 2))], Now());
        var accepted = table.AssertHeld(_nodeB, [(key2, new FencingToken(2, 1))], Now());
        Assert.Empty(accepted);
        Assert.Equal([key2], table.Renew(_nodeA, [(key2, new FencingToken(1, 2))], Now()));
    }

    [Fact]
    public void LeastLeases_steers_grants_away_from_the_loaded_node()
    {
        var table = Table(o => o.LoadBalanceTolerance = 1);
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);

        Assert.True(table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "1"), ProcessingPreference.Balanced, Now()).Immediate!.Value.Granted);
        Assert.True(table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "2"), ProcessingPreference.Balanced, Now()).Immediate!.Value.Granted);

        // A now holds 2, B holds 0: A is beyond min+tolerance and gets steered away.
        var denied = table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "3"), ProcessingPreference.Balanced, Now()).Immediate!.Value;
        Assert.False(denied.Granted);
        Assert.Equal(LeaseDenialReason.Rebalance, denied.Reason);

        Assert.True(table.Acquire(_nodeB, Guid.NewGuid(), new LeaseKey("t", "3"), ProcessingPreference.Balanced, Now()).Immediate!.Value.Granted);
    }

    [Fact]
    public void This_preference_bypasses_load_balancing()
    {
        var table = Table(o => o.LoadBalanceTolerance = 0);
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "1"), ProcessingPreference.This, Now());

        var granted = table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "2"), ProcessingPreference.This, Now()).Immediate!.Value;
        Assert.True(granted.Granted);
    }

    [Fact]
    public void Weighted_balancing_normalizes_by_node_weight()
    {
        var table = Table(o =>
        {
            o.DefaultLoadBalance = LoadBalanceType.Weighted;
            o.LoadBalanceTolerance = 0;
        });
        table.NodeUp(_nodeA, 3.0); // three times the capacity
        table.NodeUp(_nodeB, 1.0);

        // A holding 2 with weight 3 has load 0.67; B holding 1 with weight 1 has load 1.
        table.AssertHeld(_nodeA, [(new LeaseKey("t", "1"), new FencingToken(1, 1)), (new LeaseKey("t", "2"), new FencingToken(1, 2))], Now());
        table.AssertHeld(_nodeB, [(new LeaseKey("t", "3"), new FencingToken(1, 3))], Now());

        // B (load 1.0) is above the weighted minimum (A at 0.67) and gets steered away...
        var denied = table.Acquire(_nodeB, Guid.NewGuid(), new LeaseKey("t", "5"), ProcessingPreference.Balanced, Now()).Immediate!.Value;
        Assert.False(denied.Granted);
        Assert.Equal(LeaseDenialReason.Rebalance, denied.Reason);
        // ...while A, below the minimum, is granted.
        Assert.True(table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", "4"), ProcessingPreference.Balanced, Now()).Immediate!.Value.Granted);
    }

    [Fact]
    public void None_balancing_grants_first_come_first_served()
    {
        var table = Table(o => o.DefaultLoadBalance = LoadBalanceType.None);
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        for (var i = 0; i < 10; i++)
            Assert.True(table.Acquire(_nodeA, Guid.NewGuid(), new LeaseKey("t", $"{i}"), ProcessingPreference.Balanced, Now()).Immediate!.Value.Granted);
    }

    [Fact]
    public void Other_preference_grants_immediately_when_alone()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        var result = table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.Other, Now());
        Assert.True(result.Immediate!.Value.Granted);
    }

    [Fact]
    public void Other_preference_defers_then_grants_when_nobody_claims()
    {
        var options = new CoLibraOptions { ServiceId = "svc", SharedSecret = "x" };
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);

        var deferred = table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.Other, Now());
        Assert.Null(deferred.Immediate);

        _time.Advance(options.OtherPreferenceGraceWindow + TimeSpan.FromSeconds(1));
        var sweep = table.Sweep(Now());
        var matured = Assert.Single(sweep.MaturedGrants);
        Assert.True(matured.Granted);
        Assert.Equal(_nodeA, matured.Requester);
    }

    [Fact]
    public void Other_preference_yields_to_a_willing_claimant()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);

        var deferredId = Guid.NewGuid();
        table.Acquire(_nodeA, deferredId, Key, ProcessingPreference.Other, Now());
        var claim = table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.Balanced, Now());

        Assert.True(claim.Immediate!.Value.Granted);
        var resolved = Assert.Single(claim.Resolved);
        Assert.Equal(_nodeA, resolved.Requester);
        Assert.Equal(deferredId, resolved.RequestId);
        Assert.False(resolved.Granted);
        Assert.Equal(LeaseDenialReason.PreferredElsewhere, resolved.Reason);
    }

    [Fact]
    public void NodeDown_frees_all_its_leases_and_notifies_interest()
    {
        var table = Table();
        table.NodeUp(_nodeA, 1);
        table.NodeUp(_nodeB, 1);
        table.NodeUp(_nodeC, 1);
        table.Acquire(_nodeA, Guid.NewGuid(), Key, ProcessingPreference.This, Now());
        table.Acquire(_nodeB, Guid.NewGuid(), Key, ProcessingPreference.This, Now()); // interest
        table.Acquire(_nodeC, Guid.NewGuid(), Key, ProcessingPreference.This, Now()); // interest

        var freed = table.NodeDown(_nodeA);
        var (key, interested) = Assert.Single(freed);
        Assert.Equal(Key, key);
        Assert.Equal(2, interested.Count);
        Assert.Contains(_nodeB, interested);
        Assert.Contains(_nodeC, interested);
    }
}

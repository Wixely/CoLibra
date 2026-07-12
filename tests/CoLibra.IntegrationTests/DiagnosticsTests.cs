using CoLibra.Runtime;

namespace CoLibra.IntegrationTests;

public class DiagnosticsTests : IAsyncLifetime
{
    private readonly TestCluster _cluster = new();

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _cluster.DisposeAsync();

    [Fact]
    public async Task Diagnostics_report_membership_and_who_is_coordinator()
    {
        var alice = await _cluster.StartNodeAsync(o => o.NodeName = "alice");
        var bob = await _cluster.StartNodeAsync(o => o.NodeName = "bob");

        await TestCluster.WaitUntilAsync(() => alice.Members.Count == 2 && bob.Members.Count == 2,
            because: "both should see the full cluster");

        var onCoord = await alice.GetDiagnosticsAsync();
        var onMember = await bob.GetDiagnosticsAsync();

        // Both agree on who the coordinator is.
        Assert.Equal(alice.LocalNodeId, onCoord.CoordinatorId);
        Assert.Equal(alice.LocalNodeId, onMember.CoordinatorId);
        Assert.Equal("alice", onCoord.CoordinatorName);
        Assert.Equal("alice", onMember.CoordinatorName);

        // Roles are reflected.
        Assert.True(onCoord.IsCoordinator);
        Assert.False(onMember.IsCoordinator);
        Assert.NotNull(onCoord.AsCoordinator);   // coordinator-only view present
        Assert.Null(onMember.AsCoordinator);     // members don't have it

        // Each sees both members, flags itself, and identifies the coordinator member.
        Assert.Equal(2, onCoord.MemberCount);
        Assert.Contains(onCoord.Members, m => m.IsSelf && m.IsCoordinator && m.Name == "alice");
        Assert.Contains(onMember.Members, m => !m.IsSelf && m.IsCoordinator && m.Name == "alice");
        Assert.Contains(onMember.Members, m => m.IsSelf && !m.IsCoordinator && m.Name == "bob");

        // Terms agree and are positive once a coordinator exists.
        Assert.True(onCoord.Term > 0);
        Assert.Equal(onCoord.Term, onMember.Term);
    }

    [Fact]
    public async Task Diagnostics_reflect_held_leases_as_work_progresses()
    {
        var coord = await _cluster.StartNodeAsync();
        var worker = await _cluster.StartNodeAsync();

        await TestCluster.WaitUntilAsync(() => worker.Members.Count == 2);

        Assert.True(await worker.CanProcessAsync("orders", "A"));
        Assert.True(await worker.CanProcessAsync("orders", "B"));
        Assert.True(await worker.CanProcessAsync("invoices", "X"));

        var local = await worker.GetDiagnosticsAsync();
        Assert.Equal(3, local.HeldLeaseCount);
        Assert.Equal(2, local.HeldLeasesByType["orders"]);
        Assert.Equal(1, local.HeldLeasesByType["invoices"]);

        // The coordinator's authoritative table sees the same leases attributed to the worker.
        var onCoord = await coord.GetDiagnosticsAsync();
        Assert.NotNull(onCoord.AsCoordinator);
        Assert.Equal(3, onCoord.AsCoordinator!.TrackedLeaseCount);
        Assert.Equal(1, onCoord.AsCoordinator.SessionCount); // one member session (the worker)

        var workerId = worker.LocalNodeId.Value.ToString(); // full id — the map keys by full id, not the 8-char prefix
        Assert.Equal(2, onCoord.AsCoordinator.LeasesByTypePerNode["orders"][workerId]);
        Assert.Equal(1, onCoord.AsCoordinator.LeasesByTypePerNode["invoices"][workerId]);
    }

    [Fact]
    public async Task Coordinator_diagnostics_handle_nodes_with_colliding_id_prefixes()
    {
        // NodeId.ToString() is only an 8-char prefix; these two ids share it (as any two nodes
        // started in the same ~65s window do, GUID v7 timestamp bits). The per-node lease map
        // must key by the FULL id, or building it throws a duplicate-key ArgumentException.
        var a = Guid.Parse("12345678-0000-0000-0000-000000000001");
        var b = Guid.Parse("12345678-0000-0000-0000-000000000002");
        var coord = await _cluster.StartNodeAsync(o => o.NodeId = a);
        var worker = await _cluster.StartNodeAsync(o => o.NodeId = b);

        await TestCluster.WaitUntilAsync(() => coord.Members.Count == 2 && worker.Members.Count == 2);

        // Both nodes hold a lease of the SAME type, so both land under that type's per-node map.
        Assert.True(await coord.CanProcessAsync("orders", "C"));
        Assert.True(await worker.CanProcessAsync("orders", "W"));

        var diag = await coord.GetDiagnosticsAsync(); // must not throw on the prefix collision
        Assert.NotNull(diag.AsCoordinator);
        Assert.Equal(2, diag.AsCoordinator!.LeasesByTypePerNode["orders"].Count);
        Assert.Contains(a.ToString(), diag.AsCoordinator.LeasesByTypePerNode["orders"].Keys);
        Assert.Contains(b.ToString(), diag.AsCoordinator.LeasesByTypePerNode["orders"].Keys);
    }

    [Fact]
    public async Task Diagnostics_echo_configuration_without_secrets()
    {
        var node = await _cluster.StartNodeAsync(o =>
        {
            o.NodeName = "solo";
            o.LeaseTtl = TimeSpan.FromSeconds(7 * TestCluster.Scale);
        });

        var diag = await node.GetDiagnosticsAsync();

        Assert.Equal("solo", diag.NodeName);
        Assert.Equal("svc", diag.ServiceId);
        Assert.Equal(node.LocalNodeId, diag.LocalNodeId);
        Assert.Equal(TimeSpan.FromSeconds(7 * TestCluster.Scale), diag.Configuration.LeaseTtl);
        Assert.Equal(node.State, diag.State);

        // The snapshot type carries no secret-bearing member at all — a structural guarantee.
        Assert.DoesNotContain(
            typeof(ConfigurationDiagnostics).GetProperties(),
            p => p.Name.Contains("Secret", StringComparison.OrdinalIgnoreCase) ||
                 p.Name.Contains("Key", StringComparison.OrdinalIgnoreCase));
    }
}

using System.Net;

namespace CoLibra;

/// <summary>A node currently known to be part of the cluster.</summary>
public sealed record ClusterMember
{
    /// <summary>The member's stable identity.</summary>
    public required NodeId NodeId { get; init; }

    /// <summary>Monotonic start-time marker distinguishing restarts of the same configured NodeId.</summary>
    public required long Incarnation { get; init; }

    /// <summary>The member's mesh (TCP) endpoint.</summary>
    public required IPEndPoint Endpoint { get; init; }

    /// <summary>The member's advertised service version.</summary>
    public required Version ServiceVersion { get; init; }

    /// <summary>
    /// The member's application-defined name (<see cref="CoLibraOptions.NodeName"/>) — a machine
    /// id, a username, whatever the service chooses. Null when the member didn't set one.
    /// Names are not required to be unique; name-addressed messaging delivers to every match.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>Relative capacity used by <see cref="LoadBalanceType.Weighted"/>.</summary>
    public double Weight { get; init; } = 1.0;

    /// <summary>True when this member is the current coordinator.</summary>
    public bool IsCoordinator { get; init; }

    /// <summary>Whether this member accepts work leases (see <see cref="CoLibraOptions.AcceptWork"/>).</summary>
    public bool AcceptsWork { get; init; } = true;

    /// <summary>
    /// Compact per-term node id assigned by the coordinator, used in UDP data-plane packet
    /// headers instead of the 16-byte <see cref="NodeId"/>. 0 = none assigned.
    /// </summary>
    public int WireId { get; init; }

    /// <summary>The member's UDP data-plane port; 0 when the member has no UDP engine.</summary>
    public int UdpPort { get; init; }
}

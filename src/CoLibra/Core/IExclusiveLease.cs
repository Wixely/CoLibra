namespace CoLibra;

/// <summary>
/// A handle to an exclusively-owned lease. Disposing the handle releases the lease
/// so another node can claim the key.
/// </summary>
public interface IExclusiveLease : IAsyncDisposable
{
    /// <summary>The owned key.</summary>
    LeaseKey Key { get; }

    /// <summary>The fencing token for this grant; use it to fence writes to external systems.</summary>
    FencingToken Token { get; }

    /// <summary>True while the lease is held and renewing.</summary>
    bool IsHeld { get; }

    /// <summary>Fires if ownership is lost (missed renewals or a conflict after partition heal).</summary>
    CancellationToken Lost { get; }
}

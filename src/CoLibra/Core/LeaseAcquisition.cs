namespace CoLibra;

/// <summary>Result of an explicit lease acquisition attempt.</summary>
public sealed class LeaseAcquisition
{
    /// <summary>True when the local node now owns the key.</summary>
    public required bool Granted { get; init; }

    /// <summary>The lease handle when granted; null otherwise.</summary>
    public IExclusiveLease? Lease { get; init; }

    /// <summary>Why the acquisition was denied, when it was.</summary>
    public LeaseDenialReason DenialReason { get; init; }

    /// <summary>The current owner of the key when denied with <see cref="LeaseDenialReason.HeldByOther"/>.</summary>
    public NodeId? CurrentOwner { get; init; }
}

/// <summary>Options for an explicit lease acquisition.</summary>
public sealed class LeaseAcquireOptions
{
    /// <summary>The instance's preference suggestion to the coordinator. Defaults to <see cref="ProcessingPreference.Balanced"/>.</summary>
    public ProcessingPreference Preference { get; set; } = ProcessingPreference.Balanced;
}

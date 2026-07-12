using System.Collections.Immutable;
using System.Net;
using System.Threading.Channels;
using CoLibra.Leasing;
using CoLibra.Protocol;
using CoLibra.Security;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

/// <summary>
/// The local CoLibra node. All cluster state is owned by a single-threaded actor loop
/// (posted actions executed serially), so protocol logic never needs fine-grained locking;
/// the only concurrent structures are the lock-free fast paths of
/// <see cref="CanProcessAsync"/> (held-lease snapshot + decision cache).
/// </summary>
internal sealed partial class CoLibraNode : ICoLibraCluster, IAsyncDisposable
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    private readonly CoLibraOptions _options;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly ITransport _transport;
    private readonly ClusterKeys _keys;
    private readonly DiscoveryCodec _discoveryCodec;
    private readonly DecisionCache _negativeCache;
    private readonly CompletionRegistry? _completions;
    private readonly Version _serviceVersion;
    private readonly long _incarnation;

    private readonly Channel<Func<ValueTask>> _actions = Channel.CreateUnbounded<Func<ValueTask>>(
        new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource _clusterReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<Task> _pumps = [];

    // ---- lease state (held-lease writes only from the actor; reads from any thread) ----
    private readonly Dictionary<LeaseKey, LocalLease> _held = [];
    private volatile ImmutableDictionary<LeaseKey, LocalLease> _heldSnapshot =
        ImmutableDictionary<LeaseKey, LocalLease>.Empty;
    private long _lastAckTimestamp;
    private volatile bool _isCoordinatorRole;
    private volatile bool _acceptWork;
    private readonly Dictionary<Guid, PendingAcquire> _pendingAcquires = [];

    // ---- cluster state (actor-owned; volatile snapshots for public reads) ----
    private volatile ClusterState _state = ClusterState.Starting;
    private volatile IReadOnlyList<ClusterMember> _members = [];
    private long _highestTerm;
    private int _lastKnownClusterSize = 1;
    private long _discoveryDeadlineTs;
    private long _lastProbeTs;
    private CoordinatorRole? _coordinator;
    private MemberRole? _member;
    private ElectionRound? _election;
    private readonly HashSet<Guid> _incompatibleCoordinators = [];
    private int _joinRedirects;

    public CoLibraNode(CoLibraOptions options, ILogger logger, TimeProvider timeProvider, ITransport? transport = null,
        IUdpMessagingEngine? udpEngine = null)
    {
        _udpEngine = udpEngine;
        _options = options;
        _logger = logger;
        _time = timeProvider;
        _serviceVersion = options.ResolveServiceVersion();
        _incarnation = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        LocalNodeId = options.NodeId is { } fixedId ? new NodeId(fixedId) : NodeId.NewId();
        _keys = new ClusterKeys(options.ServiceId, options.SharedSecret);
        _discoveryCodec = new DiscoveryCodec(_keys, options.ServiceId, timeProvider);
        _negativeCache = new DecisionCache(
            options.EnableDecisionCache, options.DecisionCacheTtl, options.DecisionCacheMaxEntries, timeProvider);
        _completions = options.CompletionTracking.Enabled
            ? new CompletionRegistry(options.CompletionTracking, timeProvider)
            : null;
        _acceptWork = options.AcceptWork;
        _transport = transport ?? new SocketTransport(
            options,
            CertificateProvider.GetOrCreate(options.ResolveCertificatePath(), options.ServiceId, logger),
            logger);
    }

    public NodeId LocalNodeId { get; }

    public ClusterState State => _state;

    public IReadOnlyList<ClusterMember> Members => _members;

    public IReadOnlyCollection<LeaseKey> HeldLeases => _heldSnapshot.Keys.ToList();

    public event EventHandler<LeaseLostEventArgs>? LeaseLost;
    public event EventHandler<LeaseAvailableEventArgs>? LeaseAvailable;
    public event EventHandler<MembershipChangedEventArgs>? MembershipChanged;
    public event EventHandler<ClusterStateChangedEventArgs>? StateChanged;
    public event EventHandler<SplitBrainDetectedEventArgs>? SplitBrainDetected;

    public Task WaitForClusterAsync(CancellationToken cancellationToken = default) =>
        _clusterReady.Task.WaitAsync(cancellationToken);

    // =====================================================================================
    // Lifecycle
    // =====================================================================================

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
        await StartUdpEngineAsync(cancellationToken).ConfigureAwait(false);
        _pumps.Add(Task.Run(RunActorAsync, CancellationToken.None));
        _pumps.Add(Task.Run(() => RunTicksAsync(_stopping.Token), CancellationToken.None));
        _pumps.Add(Task.Run(() => PumpDatagramsAsync(_stopping.Token), CancellationToken.None));
        _pumps.Add(Task.Run(() => PumpInboundAsync(_stopping.Token), CancellationToken.None));

        Post(() =>
        {
            SetState(ClusterState.Discovering);
            _discoveryDeadlineTs = Now() + ToTicks(_options.DiscoveryWindow);
            SendProbes();
            return ValueTask.CompletedTask;
        });
    }

    public async ValueTask DisposeAsync()
    {
        if (_stopping.IsCancellationRequested)
            return;

        var stopped = PostWithResult<bool>(async tcs =>
        {
            SetState(ClusterState.Stopped);
            FailAllPending(LeaseDenialReason.NoCoordinator);
            FailAllPendingResolves();
            foreach (var pendingAck in _pendingMessageAcks.Values.ToList())
                pendingAck.TrySetResult(DirectAckStatus.Unreachable);
            _pendingMessageAcks.Clear();
            CloseAllUdpLinks("node stopping");
            foreach (var pooled in _directChannels.Values)
                _ = pooled.Channel.DisposeAsync();
            _directChannels.Clear();

            // Tell the coordinator we are leaving cleanly so it reclaims our leases at once instead
            // of holding them to their TTL (which it must do for a silent partition or crash). Best
            // effort — a dead link just means we fall back to the timeout path.
            if (_member is { Connection: { } coordinatorChannel })
                await SendSafeAsync(coordinatorChannel, new LeaveNoticeMessage(LocalNodeId.Value)).ConfigureAwait(false);

            _member?.Dispose();
            _coordinator?.DisposeAllSessions();
            tcs.TrySetResult(true);
        });
        try
        {
            await stopped.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
        }

        _stopping.Cancel();
        _actions.Writer.TryComplete();
        if (_udpEngine is not null)
            await _udpEngine.DisposeAsync().ConfigureAwait(false);
        await _transport.DisposeAsync().ConfigureAwait(false);
        _clusterReady.TrySetCanceled();
        try
        {
            await Task.WhenAll(_pumps).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // pumps end on their own cancellation; residual faults are not actionable here
        }

        _stopping.Dispose();
    }

    // =====================================================================================
    // Public lease API
    // =====================================================================================

    public ValueTask<bool> CanProcessAsync(
        string type, string id, ProcessingPreference preference = ProcessingPreference.Balanced,
        CancellationToken cancellationToken = default)
    {
        var key = new LeaseKey(type, id);
        ThrowIfSplitBrainPolicyThrows();

        if (IsHeldAndFresh(key))
            return ValueTask.FromResult(true); // held work continues even when not accepting new
        if (!_acceptWork)
            return ValueTask.FromResult(false);
        if (_completions?.Contains(key) == true)
            return ValueTask.FromResult(false);
        if (_negativeCache.IsDenied(key))
            return ValueTask.FromResult(false);

        return new ValueTask<bool>(AcquireCoreAsync(key, preference, cancellationToken)
            .ContinueWith(static t => t.Result.Granted, TaskContinuationOptions.OnlyOnRanToCompletion));
    }

    public async ValueTask<LeaseAcquisition> TryAcquireAsync(
        string type, string id, LeaseAcquireOptions? options = null, CancellationToken cancellationToken = default)
    {
        var key = new LeaseKey(type, id);
        ThrowIfSplitBrainPolicyThrows();

        var result = await AcquireCoreAsync(key, options?.Preference ?? ProcessingPreference.Balanced, cancellationToken)
            .ConfigureAwait(false);
        if (!result.Granted)
        {
            return new LeaseAcquisition
            {
                Granted = false,
                DenialReason = result.Reason,
                CurrentOwner = result.Owner,
            };
        }

        var lease = await PostWithResult<IExclusiveLease>(tcs =>
        {
            tcs.TrySetResult(_held.TryGetValue(key, out var local)
                ? new ExclusiveLease(this, key, local.Token, local.LostCts.Token)
                : new ExclusiveLease(this, key, result.Token, new CancellationToken(true)));
            return ValueTask.CompletedTask;
        }).ConfigureAwait(false);

        return new LeaseAcquisition { Granted = true, Lease = lease };
    }

    public ValueTask ReleaseAsync(string type, string id)
    {
        var key = new LeaseKey(type, id);
        return new ValueTask(PostWithResult<bool>(tcs =>
        {
            ReleaseCore(key);
            tcs.TrySetResult(true);
            return ValueTask.CompletedTask;
        }));
    }

    public bool IsCompleted(string type, string id) =>
        _completions?.Contains(new LeaseKey(type, id)) ?? false;

    public bool IsAcceptingWork => _acceptWork;

    public ValueTask<RebalanceResult> ForceRebalanceAsync(string? type = null, CancellationToken cancellationToken = default) =>
        new(PostWithResult<RebalanceResult>(tcs =>
        {
            if (_coordinator is not { } coordinator)
            {
                tcs.TrySetResult(new RebalanceResult(false, 0, 0)); // not the coordinator: silently do nothing
                return ValueTask.CompletedTask;
            }

            var revocations = coordinator.Table.ForceRebalance(type, Now());
            foreach (var group in revocations.GroupBy(r => r.Owner))
            {
                if (group.Key == LocalNodeId)
                {
                    foreach (var (_, key) in group)
                        RemoveHeld(key, LeaseLossReason.Rebalanced);
                }
                else if (coordinator.Sessions.TryGetValue(group.Key, out var session))
                {
                    _ = SendSafeAsync(session.Connection,
                        new LeaseRevokedMessage([.. group.Select(r => LeaseKeyDto.From(r.Key))]));
                }
            }

            var nodesShed = revocations.Select(r => r.Owner).Distinct().Count();
            if (revocations.Count > 0)
            {
                _logger.LogInformation("Forced rebalance revoked {Count} lease(s) from {Nodes} node(s){Type}",
                    revocations.Count, nodesShed, type is null ? "" : $" (type '{type}')");
            }

            tcs.TrySetResult(new RebalanceResult(true, revocations.Count, nodesShed));
            return ValueTask.CompletedTask;
        }).WaitAsync(cancellationToken));

    public ValueTask SetAcceptingWorkAsync(bool accept, CancellationToken cancellationToken = default) =>
        new(PostWithResult<bool>(tcs =>
        {
            _acceptWork = accept;
            if (_coordinator is { } coordinator)
            {
                coordinator.Table.SetAcceptsWork(LocalNodeId, accept);
                UpdateCoordinatorMembership(coordinator); // members see the flip immediately
            }
            else if (_member is { } member)
            {
                member.LastHeartbeatSentTs = 0; // advertise on the next tick instead of a full interval
            }

            tcs.TrySetResult(true);
            return ValueTask.CompletedTask;
        }).WaitAsync(cancellationToken));

    public ValueTask MarkCompletedAsync(string type, string id, CancellationToken cancellationToken = default)
    {
        if (_completions is null)
        {
            throw new InvalidOperationException(
                "Completion tracking is disabled; set CoLibraOptions.CompletionTracking.Enabled = true to use MarkCompletedAsync.");
        }

        var key = new LeaseKey(type, id);
        return new ValueTask(PostWithResult<bool>(tcs =>
        {
            MarkCompletedCore(key);
            tcs.TrySetResult(true);
            return ValueTask.CompletedTask;
        }).WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Records the tombstone locally (its own fact, kept even while disconnected — re-synced on
    /// the next join), releases the lease if held, and propagates: as coordinator straight into
    /// the broadcast queue, as member via release-as-completed. Completions are monotonic facts
    /// about finished work, so they are accepted under every <see cref="SplitBrainPolicy"/>.
    /// </summary>
    private void MarkCompletedCore(LeaseKey key)
    {
        _held.TryGetValue(key, out var local);
        RemoveHeld(key, lossReason: null);

        if (_coordinator is { } coordinator)
        {
            coordinator.Table.Release(LocalNodeId, key, out _); // completed, not available: no notify
            EnqueueCompletionBroadcast(coordinator, [key]);     // records locally + queues broadcast
        }
        else
        {
            _completions!.Add(key, Now());
            if (_member is { Connection: { } conn })
            {
                _ = SendSafeAsync(conn, new LeaseReleaseMessage(
                    key.Type, key.Id, local?.Token.Term ?? 0, local?.Token.Sequence ?? 0, AsCompleted: true));
            }
            // else: recorded locally; the snapshot upload on the next join re-syncs it.
        }
    }

    private void ReleaseCore(LeaseKey key)
    {
        if (!_held.TryGetValue(key, out var local))
            return;

        RemoveHeld(key, lossReason: null);
        if (_coordinator is { } coordinator)
        {
            if (coordinator.Table.Release(LocalNodeId, key, out var interested))
                NotifyAvailable(coordinator, [(key, interested)]);
        }
        else if (_member is { Connection: { } conn })
        {
            _ = SendSafeAsync(conn, new LeaseReleaseMessage(key.Type, key.Id, local.Token.Term, local.Token.Sequence));
        }
    }

    private bool IsHeldAndFresh(LeaseKey key)
    {
        if (!_heldSnapshot.TryGetValue(key, out var local))
            return false;
        TouchLease(local);
        if (_isCoordinatorRole)
            return true;
        var elapsed = _time.GetElapsedTime(Volatile.Read(ref _lastAckTimestamp));
        return elapsed < _options.LeaseTtl - _options.LeaseRenewSafetyMargin;
    }

    /// <summary>
    /// The 50% sliding-renewal rule: an ownership check refreshes the idle expiry only once it
    /// has fallen below half — one conditional volatile write on a stable object, so the hot
    /// path stays lock-free and untouched checks cost a read and a compare.
    /// </summary>
    private void TouchLease(LocalLease local)
    {
        var deadline = Volatile.Read(ref local.IdleDeadlineTs);
        if (deadline == 0)
            return; // never expires

        var now = _time.GetTimestamp();
        if (now > deadline - local.IdleExpiryTicks / 2)
            Volatile.Write(ref local.IdleDeadlineTs, now + local.IdleExpiryTicks);
    }

    /// <summary>Idle-expiry ticks for a lease type (per-type override, then global); 0 = never.</summary>
    private long ResolveIdleExpiryTicks(string type)
    {
        var expiry = _options.PerTypeLeaseIdleExpiry.TryGetValue(type, out var perType)
            ? perType
            : _options.LeaseIdleExpiry;
        return expiry is { } value ? ToTicks(value) : 0;
    }

    private void ThrowIfSplitBrainPolicyThrows()
    {
        if (_options.SplitBrainPolicy == SplitBrainPolicy.ThrowOnAllOperations && _state == ClusterState.QuorumLost)
            throw new SplitBrainException("CoLibra cluster has lost quorum and SplitBrainPolicy is ThrowOnAllOperations.");
        if (_state == ClusterState.Faulted)
            throw new InvalidOperationException("CoLibra node is faulted (duplicate NodeId rejected by the cluster).");
    }

    private Task<GrantResult> AcquireCoreAsync(LeaseKey key, ProcessingPreference preference, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<GrantResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        Post(() =>
        {
            // Re-check under the actor: another caller may have acquired it meanwhile.
            if (_held.ContainsKey(key) && IsHeldAndFresh(key))
            {
                tcs.TrySetResult(new GrantResult(true, _held[key].Token, LeaseDenialReason.None, null));
                return ValueTask.CompletedTask;
            }

            if (!_acceptWork)
            {
                tcs.TrySetResult(new GrantResult(false, default, LeaseDenialReason.NotAcceptingWork, null));
                return ValueTask.CompletedTask;
            }

            if (_completions?.Contains(key) == true)
            {
                tcs.TrySetResult(new GrantResult(false, default, LeaseDenialReason.Completed, null));
                return ValueTask.CompletedTask;
            }

            if (_state is ClusterState.QuorumLost && _options.SplitBrainPolicy != SplitBrainPolicy.Continue)
            {
                tcs.TrySetResult(new GrantResult(false, default, LeaseDenialReason.SplitBrain, null));
                return ValueTask.CompletedTask;
            }

            if (_state is ClusterState.Faulted or ClusterState.Stopped)
            {
                tcs.TrySetResult(new GrantResult(false, default, LeaseDenialReason.NoCoordinator, null));
                return ValueTask.CompletedTask;
            }

            var pending = new PendingAcquire(Guid.NewGuid(), key, preference, tcs, Now());
            _pendingAcquires[pending.RequestId] = pending;
            ct.Register(() => Post(() =>
            {
                if (_pendingAcquires.Remove(pending.RequestId))
                    tcs.TrySetCanceled(ct);
                return ValueTask.CompletedTask;
            }));

            DispatchPendingAcquire(pending);
            return ValueTask.CompletedTask;
        });
        return tcs.Task;
    }

    /// <summary>Routes a pending acquire to the local coordinator role or over the member connection; otherwise it stays queued for the next join.</summary>
    private void DispatchPendingAcquire(PendingAcquire pending)
    {
        if (_coordinator is { } coordinator)
        {
            HandleAcquireAsCoordinator(coordinator, LocalNodeId, new LeaseAcquireMessage(
                pending.RequestId, pending.Key.Type, pending.Key.Id, pending.Preference));
            return;
        }

        if (_member is { Connection: { } conn })
        {
            pending.Sent = true;
            _ = SendSafeAsync(conn, new LeaseAcquireMessage(
                pending.RequestId, pending.Key.Type, pending.Key.Id, pending.Preference));
        }
        // else: queued; flushed by CompleteJoin/BecomeCoordinator, timed out by the tick.
    }

    private void CompletePending(Guid requestId, GrantResult result)
    {
        if (!_pendingAcquires.Remove(requestId, out var pending))
            return;

        if (result.Granted)
        {
            AddHeld(pending.Key, result.Token);
            _negativeCache.Invalidate([pending.Key]);
        }
        else if (result.Reason == LeaseDenialReason.Completed)
        {
            // The coordinator knew before we did (stale local registry); backfill it.
            _completions?.Add(pending.Key, Now());
        }
        else if (result.Reason is LeaseDenialReason.HeldByOther or LeaseDenialReason.Rebalance or LeaseDenialReason.PreferredElsewhere)
        {
            _negativeCache.Deny(pending.Key);
        }

        pending.Tcs.TrySetResult(result);
    }

    private void FailAllPending(LeaseDenialReason reason)
    {
        foreach (var pending in _pendingAcquires.Values.ToList())
            pending.Tcs.TrySetResult(new GrantResult(false, default, reason, null));
        _pendingAcquires.Clear();
    }

    // =====================================================================================
    // Held-lease bookkeeping (actor only)
    // =====================================================================================

    private void AddHeld(LeaseKey key, FencingToken token)
    {
        var idleTicks = ResolveIdleExpiryTicks(key.Type);
        if (_held.TryGetValue(key, out var existing))
        {
            existing.Token = token;
            existing.IdleExpiryTicks = idleTicks;
            Volatile.Write(ref existing.IdleDeadlineTs, idleTicks == 0 ? 0 : Now() + idleTicks);
        }
        else
        {
            _held[key] = new LocalLease
            {
                Token = token,
                LostCts = new CancellationTokenSource(),
                IdleExpiryTicks = idleTicks,
                IdleDeadlineTs = idleTicks == 0 ? 0 : Now() + idleTicks,
            };
        }

        _heldSnapshot = _held.ToImmutableDictionary();
        Volatile.Write(ref _lastAckTimestamp, Now());
    }

    private void RemoveHeld(LeaseKey key, LeaseLossReason? lossReason)
    {
        if (!_held.Remove(key, out var local))
            return;

        _heldSnapshot = _held.ToImmutableDictionary();
        local.LostCts.Cancel();
        local.LostCts.Dispose();
        if (lossReason is { } reason)
            Raise(LeaseLost, new LeaseLostEventArgs { Key = key, Reason = reason });
    }

    private List<HeldLeaseDto> BuildHeldDtos() =>
        [.. _held.Select(kv => new HeldLeaseDto(kv.Key.Type, kv.Key.Id, kv.Value.Token.Term, kv.Value.Token.Sequence))];

    private Dictionary<string, int> BuildTypeCounts() =>
        _held.Keys.GroupBy(k => k.Type, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

    // =====================================================================================
    // Actor plumbing
    // =====================================================================================

    private void Post(Func<ValueTask> action) => _actions.Writer.TryWrite(action);

    private Task<T> PostWithResult<T>(Func<TaskCompletionSource<T>, ValueTask> action)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        // Fault the result if the action throws instead of leaving the caller awaiting forever
        // (the actor loop otherwise just logs and moves on).
        if (!_actions.Writer.TryWrite(async () =>
            {
                try
                {
                    await action(tcs).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            }))
            tcs.TrySetException(new ObjectDisposedException(nameof(CoLibraNode)));
        return tcs.Task;
    }

    private async Task RunActorAsync()
    {
        await foreach (var action in _actions.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "CoLibra actor action failed");
            }
        }
    }

    private async Task RunTicksAsync(CancellationToken ct)
    {
        var interval = TimeSpan.FromMilliseconds(Math.Clamp(_options.HeartbeatInterval.TotalMilliseconds / 2, 5, 200));
        using var timer = new PeriodicTimer(interval, _time);
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                Post(() =>
                {
                    HandleTick();
                    return ValueTask.CompletedTask;
                });
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private long Now() => _time.GetTimestamp();

    private long ToTicks(TimeSpan span) => (long)(span.TotalSeconds * _time.TimestampFrequency);

    private TimeSpan Since(long timestamp) => _time.GetElapsedTime(timestamp);

    private void SetState(ClusterState next)
    {
        var previous = _state;
        if (previous == next)
            return;

        _state = next;
        _isCoordinatorRole = _coordinator is not null && next is ClusterState.Coordinator or ClusterState.QuorumLost;
        _logger.LogInformation("CoLibra node {NodeId} state {Previous} -> {Next}", LocalNodeId, previous, next);
        if (next is ClusterState.Member or ClusterState.Coordinator)
            _clusterReady.TrySetResult();
        Raise(StateChanged, new ClusterStateChangedEventArgs { Previous = previous, Current = next });
    }

    private void Raise<T>(EventHandler<T>? handler, T args)
        where T : EventArgs
    {
        try
        {
            handler?.Invoke(this, args);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "CoLibra event handler threw");
        }
    }

    private async Task SendSafeAsync(IMessageChannel channel, Message message)
    {
        try
        {
            await channel.SendAsync(message, _stopping.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "CoLibra send of {Type} failed; the read pump will surface the closure", message.Type);
        }
    }

    private sealed class LocalLease
    {
        public required FencingToken Token;
        public required CancellationTokenSource LostCts;

        /// <summary>Ticks of idle lifetime for this lease's type; 0 = never expires.</summary>
        public long IdleExpiryTicks;

        /// <summary>
        /// Monotonic deadline after which the untouched lease ages out; 0 = never. Written by
        /// the actor at grant and by ANY thread via the 50% touch rule (benign volatile races).
        /// </summary>
        public long IdleDeadlineTs;
    }

    private sealed record PendingAcquire(
        Guid RequestId, LeaseKey Key, ProcessingPreference Preference,
        TaskCompletionSource<GrantResult> Tcs, long CreatedTs)
    {
        public bool Sent { get; set; }
    }

    internal readonly record struct GrantResult(bool Granted, FencingToken Token, LeaseDenialReason Reason, NodeId? Owner);

    private sealed class ExclusiveLease(CoLibraNode node, LeaseKey key, FencingToken token, CancellationToken lost) : IExclusiveLease
    {
        public LeaseKey Key => key;
        public FencingToken Token => token;
        public CancellationToken Lost => lost;
        public bool IsHeld => !lost.IsCancellationRequested && node.IsHeldAndFresh(key);

        public ValueTask DisposeAsync() => lost.IsCancellationRequested ? ValueTask.CompletedTask : node.ReleaseAsync(key.Type, key.Id);
    }
}

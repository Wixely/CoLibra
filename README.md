# CoLibra

**Decentralized discovery and distributed work negotiation for .NET — no central server, no external dependencies.**

CoLibra lets instances of the same service find each other on the network and negotiate exclusive, heartbeat-backed ownership of work, so parallel workers never duplicate work and a dead worker's load is picked up automatically.

- 🔍 **Zero-config discovery** — instances find each other over IPv4 UDP multicast; add a static DNS/IP seed list for WAN or multicast-restricted networks (Docker, Kubernetes, cloud VNets).
- 🔒 **Exclusive work leases** — one call, `CanProcessAsync(type, id)`, answers "should *this* instance process *this* thing?" Exactly one node gets `true` per key.
- 💓 **Built-in resilience** — leases are backed by heartbeats; if an owner dies or is cut off, its keys are released and other nodes claim them.
- ⚖️ **Load balancing** — grants steer toward the least-loaded node per work type (equal or weighted), without ever revoking work in progress.
- ✅ **Completion tracking (opt-in)** — `MarkCompletedAsync` replicates "this key is finished" to every node, so a dead node's finished work is never recomputed.
- 📬 **Routed delivery (opt-in)** — for load-balancer/partitioned-queue topologies where only one node receives a message: any node can hand data to the cluster and it arrives at the key's owner, force-assigned on first contact.
- 💬 **Node-to-node messaging (opt-in)** — send payloads directly to a node by id or by an app-defined name (machine id, username…); enough to build presence, control signals, or a whole chat app.
- 🎮 **Resilient-UDP data plane (opt-in package)** — game-server messaging via LiteNetLib: per-message `Reliable`/`Sequenced`/`Unreliable` delivery, AES-GCM encrypted, 2-byte coordinator-assigned wire ids, automatic TCP fallback.
- 🗳️ **1 to N nodes** — a single node coordinates itself; at 3+ nodes majority-quorum rules apply, with configurable split-brain behavior.
- 🔐 **Secure by default** — discovery packets are HMAC-signed with your cluster secret and mesh traffic is TLS-encrypted with a self-signed certificate generated on first startup. Zero certificate setup.

MIT licensed. One package, no third-party dependencies.

## Installing

**From a GitHub Release** (produced automatically for every `vX.Y.Z` tag — grab the assets from the [Releases page](../../releases)):

- `CoLibra.X.Y.Z.nupkg` — add the download folder as a local NuGet source and reference the package:

  ```
  dotnet nuget add source C:\path\to\downloads --name colibra-local
  dotnet add package CoLibra --version X.Y.Z
  ```

- `CoLibra-X.Y.Z-net10.0.zip` — the raw `CoLibra.dll` (plus XML docs and symbols) for direct referencing when a package feed isn't an option:

  ```xml
  <ItemGroup>
    <Reference Include="CoLibra">
      <HintPath>libs\CoLibra.dll</HintPath>
    </Reference>
    <!-- CoLibra's only dependencies are Microsoft.Extensions abstractions: -->
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.*" />
    <PackageReference Include="Microsoft.Extensions.Options" Version="10.*" />
  </ItemGroup>
  ```

  The NuGet package is the recommended route — it wires those dependencies up for you.

**From source**: `dotnet pack src/CoLibra/CoLibra.csproj -c Release -o out` and consume the resulting nupkg the same way.

**Cutting a release** (maintainers): tag the commit and push the tag — the [Release workflow](.github/workflows/release.yml) builds, runs the full test suite, and attaches both artifacts to a GitHub Release, versioned from the tag:

```
git tag v0.2.0
git push origin v0.2.0
```

Publishing to nuget.org is one commented-out step in the same workflow (needs a `NUGET_API_KEY` repository secret).

## Quick start

```csharp
using CoLibra;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddCoLibra(options =>
        {
            options.ServiceId = "order-processor";       // only nodes of the same service cluster together
            options.SharedSecret = "your-cluster-secret"; // authenticates every packet and connection
        });

        await builder.Build().RunAsync();
    }
}
```

Then, anywhere in your service:

```csharp
public sealed class OrderWorker(ICoLibraCluster cluster) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await cluster.WaitForClusterAsync(ct);

        await foreach (var evt in ReadEventsAsync(ct))
        {
            // First call negotiates ownership with the cluster; subsequent calls are
            // lock-free local reads. Exactly one instance gets true for each source.
            if (await cluster.CanProcessAsync("sourceid", evt.SourceId, ProcessingPreference.Balanced, ct))
            {
                Process(evt);
            }
            // else: another instance owns this source — discard and move on.
        }
    }
}
```

Run the same service on three machines (or three terminals — see [samples/CoLibra.Sample.Worker](samples/CoLibra.Sample.Worker/)): they discover each other, split the sources between them, and if you kill one, its sources migrate to the survivors within the lease TTL.

## The core primitive: `CanProcessAsync`

```csharp
ValueTask<bool> CanProcessAsync(string type, string id, ProcessingPreference preference = Balanced, CancellationToken ct = default);
```

- `type` groups related work (e.g. `"sourceid"`); `id` names one unit of it (e.g. `"source_12345"`).
- The **first** call for a key asks the cluster coordinator for an exclusive lease. Every later call is answered from a local cache — a lock-free read that completes synchronously — so it is safe to call in a hot loop.
- `true` means this node exclusively owns the key **and** its lease renewals are being acknowledged. If the node loses contact with the cluster, the answer flips to `false` *before* the key can be granted to anyone else (the local safety margin expires earlier than the coordinator's TTL). Outside a deliberately-accepted split brain, two nodes never see `true` for the same key.
- `false` answers are cached too, and invalidated by push when the key frees up — no polling, no repeated round-trips.

The `preference` is your instance's suggestion:

| Preference | Meaning |
|---|---|
| `Balanced` (default) | The coordinator decides, steering the grant toward the least-loaded node for this type. |
| `This` | This instance wants the work — granted unless another node already owns the key. |
| `Other` | This instance would rather not — another node gets a grace window to claim the key first, but if nobody does (or this is the only node), it is granted here so work is never orphaned. |

Release ownership explicitly with `ReleaseAsync(type, id)`, or hold an explicit handle when you need a **fencing token** for writes to external systems:

```csharp
var acquisition = await cluster.TryAcquireAsync("sourceid", "source_12345");
if (acquisition.Granted)
{
    await using var lease = acquisition.Lease!;
    // lease.Token is monotonic across coordinator failovers: attach it to external writes
    // and reject writes carrying an older token than the last one seen.
    // lease.Lost fires if ownership is ever lost (e.g. after a partition heal conflict).
}
```

Events keep you informed: `LeaseLost`, `LeaseAvailable`, `MembershipChanged`, `StateChanged`, `SplitBrainDetected`.

## Two delivery patterns

**Broadcast feed (the primary pattern).** Every instance sees every message — a bus subscription, a multicast feed, a shared topic — and `CanProcessAsync` decides which ones are *yours*; the rest are discarded, because their owner also saw them. Ownership and data-visibility always travel together, so nothing is ever missed. Grants are strictly pull-based: the coordinator never hands a key to a node that didn't ask. This is the model both quick-start snippets above use, and the [PrimeGrid sample](samples/CoLibra.Sample.PrimeGrid/) shows it partitioning a compute grid.

**Routed delivery (advanced, opt-in).** For topologies where only *one* node receives a given message — behind a load balancer, a partitioned queue, per-node ingest — enable `options.Routing` and hand the payload to the cluster:

```csharp
// On processing nodes: registering a handler advertises this node as an owner candidate.
await using var registration = cluster.Router.RegisterHandler("order", (delivery, ct) =>
{
    Process(delivery.Key.Id, delivery.Payload, delivery.Token); // runs on the key's owner
    return ValueTask.CompletedTask;
});

// On whichever node the data lands: deliver it to the owner, wherever that is.
RouteResult result = await cluster.Router.RouteAsync("order", orderId, payloadBytes, ct);
```

Payloads are raw bytes on the wire, but generic overloads handle serialization for you:

```csharp
public sealed record OrderUpdate(string Market, decimal Price, DateTimeOffset At);

await using var reg = cluster.Router.RegisterHandler<OrderUpdate>("order", (delivery, ct) =>
{
    Apply(delivery.Key.Id, delivery.Value); // delivery.Value is the deserialized OrderUpdate
    return ValueTask.CompletedTask;
});

await cluster.Router.RouteAsync("order", orderId, new OrderUpdate("main", 3.75m, DateTimeOffset.UtcNow));
```

The default serializer is System.Text.Json (no extra dependencies; pass your own `JsonSerializerOptions` — e.g. a source-generated resolver for Native AOT — via `new JsonPayloadSerializer(options)`). To use MessagePack, MemoryPack or anything else, implement the two-method `IRoutedPayloadSerializer` and assign `options.Routing.PayloadSerializer`; all nodes must agree on it. Raw `byte[]`/`ReadOnlyMemory<byte>` overloads always bypass the serializer — ideal for forwarding already-encoded messages without a decode/re-encode cycle.

If the key already has an owner, the payload goes there (in-process when it's you, over a pooled direct TLS channel otherwise, relayed via the coordinator as fallback). If the key is **unowned**, the coordinator force-assigns the lease to the least-loaded node that registered a handler for the type — the one deliberate exception to pull-only grants, safe because a registered handler proves the assignee can process. The first route for a key pays that assignment round-trip; after that the owner is cached locally. Delivery is at-least-once with acknowledgments: on `Timeout` you may retry and the handler may see a duplicate — `delivery.Token` (the fencing token) is provided for idempotency. Payloads travel as raw bytes (never base64) up to `MaxPayloadBytes`. See the [Router sample](samples/CoLibra.Sample.Router/).

## Node-to-node messaging (opt-in)

Sometimes nodes just need to talk to each other — control signals, presence, cache hints, or a full chat application. Enable `options.Messaging`, optionally give each node a name, and the member list becomes your address book:

```csharp
options.NodeName = "alice";          // any identifier the service chooses: username, machine id...
options.Messaging.Enabled = true;
```

```csharp
public sealed record ChatLine(string Text, DateTimeOffset At);

// The inbox: one handler per channel (an app-defined label).
await using var reg = cluster.Messenger.RegisterHandler<ChatLine>("chat", (message, ct) =>
{
    Console.WriteLine($"<{message.OriginName}> {message.Value.Text}");  // Origin = reply address
    return ValueTask.CompletedTask;
});

// The address book: every member, with its name.
foreach (var member in cluster.Members)
    Console.WriteLine($"{member.Name} ({member.NodeId})");

// Send by id (exact), or by name (delivers to every node bearing it):
await cluster.Messenger.SendAsync(member.NodeId, "chat", new ChatLine("hi", DateTimeOffset.UtcNow));
await cluster.Messenger.SendByNameAsync("bob", "chat", new ChatLine("hi bob", DateTimeOffset.UtcNow));
```

Names are advertised through membership (no directory service), need not be unique — `SendByNameAsync` returns one acknowledged result per match — and raw-byte overloads skip serialization just like routing. Sends are acknowledged (`Delivered` / `NoHandler` / `UnknownTarget` / `Timeout`) with at-least-once semantics, and payloads travel over the same encrypted paths as routed delivery: pooled direct member↔member channels with coordinator relay as fallback. Broadcast is deliberately just a loop over `Members` — see the [Chat sample](samples/CoLibra.Sample.Chat/) for a working terminal chat (`dotnet run -- --Name alice`).

## UDP messaging for game servers (opt-in)

The Messenger's default transport is the encrypted TCP mesh — right for control traffic, wrong for 20 Hz position updates where a retransmitted stale packet is worse than no packet. The **`CoLibra.Messaging.LiteNetLib`** package adds a resilient-UDP data plane built on [LiteNetLib](https://github.com/RevenantX/LiteNetLib) (MIT, pure C#):

```csharp
builder.Services.AddCoLibra(options =>
{
    options.Messaging.Enabled = true;
    options.Messaging.PreferUdp = true;    // use UDP links when both peers have the engine
});
builder.Services.AddCoLibraUdpMessaging(); // from CoLibra.Messaging.LiteNetLib

// Per-message delivery modes (same API on TCP, where hints collapse to reliable-ordered):
await messenger.SendAsync(peer, "fire",      shot,     MessageDelivery.Reliable);    // retransmitted, any order
await messenger.SendAsync(peer, "positions", position, MessageDelivery.Sequenced);   // latest-wins, late packets dropped
await messenger.SendAsync(peer, "chat",      line);                                  // ReliableOrdered (default), acked
```

`Reliable*` modes keep the acknowledged `SendResult`; `Sequenced`/`Unreliable` return `SendStatus.Sent` (fire-and-forget). How it stays safe and cheap:

- **Keys ride the TLS mesh.** A per-link AES-256-GCM key pair (one per direction) is derived from the cluster secret + nonces exchanged over the existing TCP channel; every datagram is authenticated-encrypted with replay suppression. No certificates on the UDP path, nothing readable or spoofable on the wire.
- **Compact headers.** The coordinator assigns each member a 2-byte wire id (visible as `ClusterMember.WireId`), channel names compress to 1 byte per link, and payloads travel raw: **15 bytes of header + 16-byte auth tag** instead of GUIDs and JSON. Wire ids are term-scoped, so coordinator failovers can't confuse identities — links just re-handshake.
- **Automatic fallback.** No engine registered, peer without UDP, handshake failure, or payload over `MaxUdpPayloadBytes` (8 KiB default) → that message silently takes the TCP path. Mixed clusters (game server on UDP, tooling on TCP) just work.
- **NAT hole punching, coordinator-mediated.** When two members sit behind NATs and the direct connect fails, the coordinator — which already holds authenticated TCP to both and runs a UDP socket — acts as the rendezvous: both sides send it an introduce request (opening their NAT mappings), it observes their public endpoints and punches them together, and the normal authenticated connect completes. On by default (`Messaging.EnableNatPunch`); it only runs after a failed direct connect. Honest limits: cone-type NATs punch fine, symmetric NATs don't (traffic then stays on the TCP fallback), and the coordinator must run the UDP engine to act as master.

Try it: `dotnet run -- --Name alice --Udp true` in two [Chat sample](samples/CoLibra.Sample.Chat/) terminals — chat lines go ReliableOrdered while a `positions` channel streams 20 Hz Sequenced updates over the same link.

License note: LiteNetLib is MIT (© Ruslan Pyrch); its notice ships in [THIRD-PARTY-NOTICES.txt](THIRD-PARTY-NOTICES.txt), inside the `CoLibra.Messaging.LiteNetLib` package, and in the release zip — where the DLL is a single file with LiteNetLib merged in (internalized), so one file drop-in still satisfies the license.

## Completion tracking (opt-in)

By default a finished piece of work is only remembered while its owner lives — if a node dies, even its *completed* keys become grantable again and get redone (at-least-once semantics). Enable `options.CompletionTracking` to remember completions cluster-wide:

```csharp
if (await cluster.CanProcessAsync("bucket", id, ct))
{
    var result = Process(id);
    await cluster.MarkCompletedAsync("bucket", id); // releases the lease + replicates "done, forever"
}
```

Every node keeps a full copy of the completion set (union-merged — completions are monotonic facts, so replication needs no conflict resolution), so any single node dying loses nothing: survivors skip its finished keys and redo only what was in flight. `CanProcessAsync` answers `false` for completed keys from local memory, `IsCompleted(type, id)` queries explicitly, and a full snapshot syncs to joiners and across coordinator failovers. Bounds: `MaxEntriesPerType` (FIFO eviction, default 100k) and optional `Retention`. The worst case is losing the last ~heartbeat of unsynced completions when a node dies — those get redone. A full-cluster restart still starts clean (state is in-memory only); if you need durable exactly-once, record completion in your own store as well.

## How it works

- **Discovery**: nodes announce and probe over UDP multicast (`239.255.41.10:41100` by default), scoped by `ServiceId` — unrelated CoLibra services on the same network or machine never mix. Every datagram is HMAC-SHA256-signed with the shared secret (with replay protection); packets from other clusters are dropped before parsing. Where multicast is unavailable, list seeds in `StaticSeeds` (`"host:port"`, DNS re-resolved per probe) — both mechanisms run concurrently.
- **Coordination**: nodes elect a coordinator (bully algorithm with monotonic terms). The coordinator grants leases, enforces load balancing, and tracks membership over per-member TLS connections with 1-second heartbeats that piggyback lease renewals. If the coordinator dies, survivors elect a successor and re-assert their held leases — **`CanProcessAsync` stays `true` for held keys throughout a normal failover**.
- **Fencing**: every grant carries a `(term, sequence)` token. After a partition heals, conflicting owners are resolved in favor of the higher token; the loser gets `LeaseLost`. Terms also fence stale coordinators out of the protocol.
- **Security**: TLS provides confidentiality; the shared secret provides authentication (mutual HMAC challenge-response inside the TLS channel). The self-signed certificate is auto-generated on first startup and stored at `<app dir>/colibra/<ServiceId>.pfx` — per-service, so multiple services on one machine never collide. Point `CertificatePath` elsewhere (or pre-provision your own PFX) if you prefer.
- **Versioning**: nodes advertise their service version (defaults to the entry assembly's). `VersionCompatibility` controls who may cluster together: `MajorMatch` (default), `Strict`, `Minimum(version)` — handy for rolling deploys — or `Any`. Incompatible nodes simply form separate clusters. The wire protocol is versioned independently and incompatible protocol versions are always rejected at join.

## Asymmetric clusters: forcing (or forbidding) coordinatorship

By default any node can be elected coordinator. For asymmetric architectures — a game server that must be the authority, a head node with the fast hardware, dedicated broker processes — pin the roles:

```csharp
// The game server: IS the coordinator, always.
options.CoordinatorMode = CoordinatorMode.Forced;

// The game clients: members only, never elected.
options.CoordinatorMode = CoordinatorMode.Never;
```

A `Forced` node never joins a non-forced coordinator: it claims leadership with a superseding term at startup and takes over an existing cluster cleanly (the incumbent steps down and rejoins as a member; held leases are re-asserted and survive). It never yields afterwards — a rival coordinator gets out-termed, not obeyed — and its claims bypass the quorum gate, so a lone game server always serves. If several `Forced` nodes meet, the first one up wins and later ones join it (simultaneous claims settle by the usual term/node-id rules). `Never` nodes join and work like any member, but when no coordinator is reachable they wait instead of claiming — deploy at least one `Eligible` or `Forced` node per cluster. While a forced coordinator is down, remaining `Eligible` nodes elect a stand-in among themselves; the forced node reclaims leadership on return.

The other half of asymmetry is **work acceptance**. A node that coordinates, observes, or serves some other purpose can declare that it never takes work leases:

```csharp
options.AcceptWork = false;                    // typical pairing: CoordinatorMode.Forced + AcceptWork = false
await cluster.SetAcceptingWorkAsync(false);    // or flip it at runtime (drain mode); back on with true
```

A non-accepting node is denied acquisitions (locally, instantly — `LeaseDenialReason.NotAcceptingWork`), excluded from load-balance math (an idle authority no longer pins the `Balanced` minimum at zero and starves real workers — this closes the mixed-cluster caveat), and skipped by routed forced assignment even where it has handlers registered. Everything else — membership, messaging, routing payloads *out*, coordinating — works normally. Acceptance is advertised in the join handshake and on every heartbeat (runtime flips propagate within about a heartbeat and are visible on `ClusterMember.AcceptsWork`), and turning it off never revokes leases already held: they keep renewing until released, completed, or the node stops.

## Cluster sizes, quorum and split brain

| Nodes | Behavior |
|---|---|
| 1 | The node coordinates itself; every grant is local and instant. |
| 2 | Quorum is treated as 1 (a strict majority of 2 would deadlock on any partition), so **both halves of a 2-node partition keep operating** — split brain is possible by design and is detected and reported when the partition heals. Prefer 3+ nodes if this matters to you. |
| 3+ | A coordinator claimant must see a majority of the last-known cluster. A minority partition stops granting new leases (by default) until quorum returns. |

Split brain is detected two ways: a coordinator discovering a rival (partitions merging — the higher term wins, the loser steps down and rejoins), and quorum loss. What happens next is your call:

```csharp
options.SplitBrainPolicy = SplitBrainPolicy.DenyNewLeases;      // default: existing work continues, nothing new is granted
// SplitBrainPolicy.Continue                                    // carry on, just raise the event
// SplitBrainPolicy.ThrowOnAllOperations                        // every lease operation throws SplitBrainException
```

Held leases keep renewing under every policy — a node never silently stops work it already owns unless it genuinely loses the lease.

## Configuration reference

| Option | Default | Notes |
|---|---|---|
| `ServiceId` | *(required)* | Scopes discovery to your kind of service. |
| `SharedSecret` | *(required)* | Authenticates packets and connections. |
| `ServiceVersion` | entry assembly version | Advertised to peers. |
| `VersionCompatibility` | `MajorMatch` | `Strict` / `MajorMatch` / `Minimum(v)` / `Any`. |
| `NodeId` | auto (GUID v7) | Fix it for stable identity across restarts. |
| `DiscoveryPort` | `41100` | UDP; shared across services on a machine via `ReuseAddress`. |
| `MeshPort` | `0` (OS-assigned) | TCP; advertised in announces. Pin it for firewalled/WAN setups. |
| `MulticastAddress` / `EnableMulticast` / `EnableBroadcastFallback` | `239.255.41.10` / `true` / `false` | |
| `StaticSeeds` | empty | `"host:port"` unicast probe targets (WAN / no multicast). |
| `AnnounceInterval` / `HeartbeatInterval` | 2 s / 1 s | |
| `MemberTimeout` / `ElectionTimeout` / `RebuildWindow` / `DiscoveryWindow` | 5 s / 3 s / 2 s / 3 s | |
| `LeaseTtl` / `LeaseRenewSafetyMargin` | 15 s / 3 s | Owner death → keys reclaimable within ~`LeaseTtl`. |
| `SplitBrainPolicy` / `QuorumPolicy` | `DenyNewLeases` / `Majority` | |
| `CoordinatorMode` | `Eligible` | `Forced` (this node IS the coordinator) / `Never` (member only) for asymmetric clusters. |
| `AcceptWork` | `true` | Whether this node takes work leases; runtime-toggleable via `SetAcceptingWorkAsync`. |
| `EnableDecisionCache` / `DecisionCacheTtl` / `DecisionCacheMaxEntries` | `true` / 30 s / 10 000 | Caching of negative `CanProcess` answers. |
| `OtherPreferenceGraceWindow` | 5 s | How long an `Other`-preference request waits for a willing node. |
| `CertificatePath` | `<app dir>/colibra/<ServiceId>.pfx` | Auto-generated self-signed cert. |
| `DefaultLoadBalance` / `PerTypeLoadBalance` / `LoadBalanceTolerance` / `Weight` | `LeastLeases` / empty / 1 / 1.0 | `None`, `LeastLeases` or `Weighted` per lease type. |
| `CompletionTracking.Enabled` / `.MaxEntriesPerType` / `.Retention` | `false` / 100 000 / null | Replicated "done" registry (see above). |
| `Routing.Enabled` / `.MaxPayloadBytes` / `.DeliveryTimeout` | `false` / 1 MiB / 5 s | Routed delivery (see above). |
| `Routing.UseDirectChannels` / `.IdleChannelTimeout` / `.OwnerCacheTtl` / `.AssignmentAckTimeout` | `true` / 60 s / 30 s / 2 s | Direct member↔member payload channels vs coordinator relay. |
| `NodeName` | null | App-defined name shown on `ClusterMember.Name`, addressable via `SendByNameAsync`. |
| `Messaging.Enabled` / `.MaxPayloadBytes` / `.DeliveryTimeout` / `.UseDirectChannels` | `false` / 1 MiB / 5 s / `true` | Node-to-node messaging (see above). |
| `Messaging.PreferUdp` / `.UdpPort` / `.LinkHandshakeTimeout` / `.MaxUdpPayloadBytes` | `false` / 0 (auto) / 2 s / 8 KiB | Resilient-UDP data plane (requires the CoLibra.Messaging.LiteNetLib package). |
| `Messaging.EnableNatPunch` | `true` | Coordinator-mediated hole punching when direct UDP connects fail (cone NATs). |

## Networking notes

- **Windows Firewall**: allow inbound UDP on the discovery port (41100) and inbound TCP on the mesh port for your service executable. Without the UDP rule, nodes can still *send* probes/announces but won't hear others.
- **Kubernetes / Docker / cloud VNets**: multicast is usually unavailable. Set `EnableMulticast = false` and point `StaticSeeds` at a headless service DNS name or known peer addresses (`"my-service-headless:41100"`), and pin `MeshPort` so peers can dial back in.
- **Several services on one machine**: they can all share UDP 41100 (the socket is opened with `ReuseAddress` and filtering happens by `ServiceId`/secret). Unicast probe replies may be delivered to only one of the co-hosted processes; multicast announces cover the rest, so LAN discovery is unaffected.
- IPv4 only (by design, for now).

## Building

```
dotnet build
dotnet test
```

Requires the .NET 10 SDK. The integration test suite spins up multi-node clusters in-process over an in-memory transport with scripted partitions — see [tests/CoLibra.IntegrationTests](tests/CoLibra.IntegrationTests/).

## License

[MIT](LICENSE)

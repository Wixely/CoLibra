CoLibra sample applications
===========================

Five runnable demos of CoLibra's decentralized discovery and work negotiation,
each a single self-sufficient Windows executable (all libraries bundled inside).
Instances discover each other automatically on the same LAN (UDP multicast) -
just start several copies, on one machine or many.

Requires the .NET 10 runtime: https://dotnet.microsoft.com/download
(Other platforms: build from source - `dotnet publish -r linux-x64` works too.)

Configuration: chat.json / gameserver.json / primegrid.json hold each demo's
settings (deliberately distinct names so all demos coexist in this one folder).
Command-line arguments override the files, e.g. `--Name alice`.

CoLibra.Sample.Worker.exe
    The hello-world: 8 work sources split across however many instances you
    start. Kill one and watch its sources migrate.

CoLibra.Sample.PrimeGrid.exe            (primegrid.json)
    Distributed prime counting. Instances carve the number space into buckets,
    never duplicate work, and remember completed buckets cluster-wide - killing
    an instance only costs its in-flight bucket. Per-node prime counts always
    sum to the exact answer: pi(2,000,000,000) = 98,222,287 with the defaults.

CoLibra.Sample.Router.exe
    Routed delivery: each instance receives events only IT sees (like traffic
    behind a load balancer) and hands them to the cluster, which delivers each
    order's events to whichever node owns it - force-assigning owners on first
    contact and rebalancing ownership across instances.

CoLibra.Sample.Chat.exe                 (chat.json)
    A terminal chat. Type to broadcast, '@name text' for direct messages, /who
    to list participants, /quit to exit. With --Udp true (both sides), messages
    ride the encrypted resilient-UDP data plane and a 20 Hz position stream
    demonstrates Sequenced (latest-wins) delivery - the game-server traffic shape.

CoLibra.Sample.GameServer.exe           (gameserver.json)
    The asymmetric topology. Start one with --Server true (forced coordinator,
    accepts no work, aggregates scores, auto-rebalances) and any number of
    players (--Name player1). Watch zones split evenly, shift instantly when a
    late player joins, and migrate when one dies.

CoLibra.Sample.Maze.exe                  (maze.json)
    A multiplayer ASCII maze in 24-bit color. The first player generates the
    map (elected by a lease); everyone who joins loads it over messaging and
    spawns at a random spot. Move with WASD / arrows - you are '@', others are
    colored dots - and every move is broadcast to all players. Needs a
    truecolor terminal (Windows Terminal or similar). Run several with
    --Name alice, --Name bob, ...

CoLibra.Sample.HostedMaze.exe            (hostedmaze.json)
    The same maze, but the game IS the server: the current CoLibra coordinator
    is the authoritative host (crown), owns all state, and broadcasts it. Kill
    whichever instance is the host and the crown migrates to another player
    within a second or two - no lost state, nobody dropped - because CoLibra
    elects a new coordinator and every client already holds the last snapshot.
    Run 3+ (--Name alice, bob, carol) so there's always a successor.

All demos use hardcoded demo secrets - fine on a trusted LAN, change them in
source for anything else. Windows Firewall: allow inbound UDP 41100 (discovery)
and the mesh/UDP ports when prompted, or discovery stays one-directional.

CoLibra is MIT licensed (see LICENSE). The Chat demo's UDP mode bundles
LiteNetLib (MIT) - see THIRD-PARTY-NOTICES.txt.
https://github.com/Wixely/CoLibra

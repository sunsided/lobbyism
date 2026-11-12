# lobbyism

Lobbies and matchmaking for Rust games: players find each other here, then connect with whatever transport the game already uses.

Rust has good transports (renet, quinn, matchbox, ggrs), but not so much on getting players together before they connect: there is no counterpart to Steam lobbies or a small master server you can embed. lobbyism is an attempt at that missing piece, and it deliberately stops where the existing crates start: when a match is formed, every player gets a connection ticket for the transport (a renet `ConnectToken`, or a matchbox room), and the lobby never sees a single gameplay packet.

This is v0.1 and not on crates.io yet. Everything lives in memory, so a restart forgets all lobbies and queues.

## How it fits together

A player who queues for a match goes through this, end to end:

```mermaid
sequenceDiagram
    autonumber
    participant A as Your auth backend
    participant C as Game client
    participant L as lobbyism-server
    participant G as Game server (renet)
    C->>A: log in (your own flow)
    A-->>C: lobby token (HmacValidator::sign)
    C->>L: WebSocket: Hello { token }
    L-->>C: Welcome { player }
    C->>L: Enqueue { mode, region, skill }
    L-->>C: Queued
    Note over L: enough players in the same<br/>mode, region and skill bucket
    L-->>C: Matched { players, handle, ticket }
    C->>G: UDP: connect with the ConnectToken
    Note over C,G: gameplay traffic never touches the lobby
```

Lobbies end the same way: instead of `Enqueue`, players create or join a lobby, mark themselves ready, and the host's `StartMatch` produces the same `Matched` message for every member.

| Crate | What it is |
|---|---|
| `lobbyism-proto` | The message types, JSON-encoded and tagged by `type`. |
| `lobbyism-server` | The lobby and queue state machine (`Core`), the WebSocket front end (`serve`), token validation and ticket issuers, plus a `lobbyism-server` binary. |
| `lobbyism-client` | An async client (`connect`, `send`, `recv`) on `tokio-tungstenite-wasm`, so the same code builds natively and for `wasm32-unknown-unknown`. |

## Try it

The example starts a renet game server, the lobby server and two clients in one process; both clients queue for a duel, get matched, and connect to the game server with their tickets:

```shell
cargo run -p lobbyism-client --example renet_match
```

```text
lobby matched players [1002, 1001] into match 1
game server: player 1001 joined match 1
game server: player 1002 joined match 1
game client 1001: server says "welcome 1001"
game client 1002: server says "welcome 1002"
both players connected over renet
```

## How do I use this?

### As a game developer

lobbyism touches four parts of a multiplayer setup, and two of them are yours already:

- Your auth backend logs players in however it likes and hands each one a lobby token, created with `HmacValidator::sign` (or anything else, if you put your own `TokenValidator` in front of the server).
- Your game client uses `lobbyism-client` to queue or meet in a lobby, waits for `Matched`, and passes the ticket to renet or matchbox.
- Your dedicated game server (renet only) runs `ServerAuthentication::Secure` with the same netcode key and protocol id as the lobby, and reads the match id from each connection's user data.
- The lobby server is either the `lobbyism-server` binary or the library embedded in a binary of yours, e.g. to add a host-migration hook or a different ticket issuer.

On the client side, queueing looks like this:

```rust
use lobbyism_client::{proto::{ClientMsg, ServerMsg, Ticket}, LobbyClient};

let mut client = LobbyClient::connect("wss://lobby.example.com", &token).await?;
client.send(ClientMsg::Enqueue { mode: "duel".into(), region: "eu".into(), skill: 1200 }).await?;
while let Some(msg) = client.recv().await? {
    if let ServerMsg::Matched(m) = msg {
        // m.players is the match in handle order, m.handle is our index in it
        if let Ticket::Netcode { connect_token, .. } = m.ticket {
            // renet_netcode::ConnectToken::read(&mut connect_token.as_slice())
        }
        break;
    }
}
```

Lobbies work the same way with `CreateLobby`, `JoinLobby`, `SetMetadata`, `SetReady`, `StartMatch` and `LeaveLobby`; every member receives the full lobby state after each change, and a `HostMigrated` message when the host leaves and the next member in join order takes over.

Embedding the server instead of running the binary looks like this:

```rust
use lobbyism_server::{serve, Config, Core, HmacValidator, NetcodeIssuer};

let issuer = NetcodeIssuer {
    protocol_id: 7,
    private_key,                       // [u8; 32], shared with the game server
    server_addresses: vec!["203.0.113.10:5000".parse()?],
    expire_seconds: 30,
    timeout_seconds: 15,
};
let core = Core::new(Config::default(), issuer)
    .on_host_migration(Box::new(|lobby, old, new| println!("lobby {lobby}: {old} -> {new}")));
let listener = tokio::net::TcpListener::bind("127.0.0.1:7878").await?;
serve(listener, core, HmacValidator::new(secret)?).await?;
```

`Config` holds the queue modes, regions, skill bucket width (100 by default), the largest allowed lobby (16) and the metadata budget per lobby (4096 bytes of keys and values).

### As an operator

The binary is configured through environment variables:

| Variable | Meaning | Default |
|---|---|---|
| `LOBBYISM_BIND` | Listen address | `127.0.0.1:7878` |
| `LOBBYISM_HMAC_SECRET` | Secret for client tokens, at least 32 bytes | required |
| `LOBBYISM_NETCODE_KEY` | netcode private key shared with the game server, 64 hex characters | required |
| `LOBBYISM_PROTOCOL_ID` | netcode protocol id of your game | required |
| `LOBBYISM_GAME_SERVERS` | Comma-separated public game server addresses | required |
| `LOBBYISM_MODES` | Queue modes and players per match, e.g. `duel=2,squad=4` | `duel=2` |
| `LOBBYISM_REGIONS` | Comma-separated regions clients may queue in | `eu,us` |

For a local run:

```shell
LOBBYISM_HMAC_SECRET="$(openssl rand -hex 32)" \
LOBBYISM_NETCODE_KEY="$(openssl rand -hex 32)" \
LOBBYISM_PROTOCOL_ID=7 \
LOBBYISM_GAME_SERVERS=127.0.0.1:5000 \
cargo run -p lobbyism-server
```

The binary also reads a `.env` file from the working directory (or one of its parents) before it starts, so `cp .env.example .env`, filling in the two secrets and `cargo run -p lobbyism-server` works too. Variables that are already set in the environment win over the file, and a `.env` that doesn't parse stops the server instead of being skipped. Building with `--no-default-features --features cli` leaves this out, for deployments where the environment comes from the orchestrator anyway.

In a real deployment the game server needs the same netcode key and protocol id, and your auth backend the same HMAC secret, so all three come from your secret store rather than a fresh `openssl` call. The netcode key and the HMAC secret must never reach a client.

#### Where to host it

The server is one small process with all state in memory: it needs little CPU and memory, but exactly one instance per game, as two instances would not see each other's lobbies or queues (and a restart drops everything, so clients should reconnect and re-queue). A small VM or a single container, close to most of your players, does the job. Serverless platforms that cut requests after a few seconds or minutes don't fit, since every client holds one WebSocket open for as long as it is looking for a game.

A typical setup, with the lobby behind a TLS proxy and the secrets each pair of services has to share:

```mermaid
flowchart TB
    C[Game client]
    A[Your auth backend]
    subgraph lobbyhost[Lobby host]
        P["Reverse proxy<br/>TLS on :443"] -->|"127.0.0.1:7878"| L[lobbyism-server]
    end
    G["Game server (renet)<br/>UDP :5000"]
    C -->|"1 log in"| A
    C -->|"2 wss://"| P
    C -->|"3 UDP + ConnectToken"| G
    A -. "same HMAC secret" .- L
    L -. "same netcode key + protocol id" .- G
```

#### Network things to be aware of

- The server speaks plain `ws://`. Put a reverse proxy (Caddy, nginx, Traefik, a cloud load balancer) in front that terminates TLS and forwards WebSocket upgrades, and keep `LOBBYISM_BIND` on `127.0.0.1` behind it; browsers on an `https://` page only allow `wss://` anyway.
- A player waiting in a lobby can be silent for minutes, and the server doesn't send keepalive pings yet. Raise the proxy's idle or read timeout accordingly (nginx closes WebSockets after 60 seconds of silence by default, see `proxy_read_timeout`).
- `LOBBYISM_GAME_SERVERS` ends up inside every `ConnectToken`, and the renet server only accepts a token that lists one of its own `public_addresses`. So it has to be the address clients actually reach (not `0.0.0.0`, not a private IP behind NAT), and it has to match the game server's configuration exactly. The game server's UDP port needs to be open separately; the lobby only needs its TCP port.
- Lobby tokens and `ConnectToken`s both carry expiry times, and a `ConnectToken` has to be used within `expire_seconds` (30 in the binary). Keep the lobby, the game server and your auth backend on NTP, or players will get rejected for no visible reason.
- The server caps frames at 64 KiB, closes connections that don't authenticate within 10 seconds and drops clients that stop reading, but it has no per-IP connection or rate limits. Those belong in the proxy.
- A matchbox ticket points at your matchbox_server, which you host separately; peers still need STUN (and for some networks TURN) to reach each other, which lobbyism doesn't provide.

## Tickets

For renet / netcode.io, `NetcodeIssuer` generates a real `ConnectToken` per player, encrypted with the private key the lobby shares with the game server, which then runs `ServerAuthentication::Secure` with the same key. The renet client id is the player id, and the first eight bytes of the token's user data hold the match id (little-endian), so the game server can tell which connections belong to which match.

For matchbox / GGRS, `MatchboxIssuer` sends every player of a match the same signaling room URL with `?next=N`, so matchbox_server groups exactly those players; the player list in the match doubles as the GGRS peer list. The room name is an HMAC of the match id under a random per-process key, which keeps it unguessable (match ids are sequential, and a guessable room is a free seat for a stranger).

Other transports implement `TicketIssuer`, which gets the match id, the player list and the recipient.

## Authentication

The first message on a connection must be `Hello { token }`, sent within 10 seconds. The default `HmacValidator` expects tokens of the form

```text
<player id>.<expiry, unix seconds>.<lowercase hex HMAC-SHA256 of the first two parts>
```

which your backend creates with `HmacValidator::sign` after its own login. The lobby only checks them, and a player can hold one connection at a time. Anything else (sessions, JWTs, Steam tickets) goes behind the `TokenValidator` trait.

## Limitations

Skill is reported by the client, so a player can claim whatever bucket they like; it belongs in the signed token claims once rankings matter. Queue matches all go to the fixed `server_addresses` list, as there is no game server allocation yet. The matchbox ticket has not been tried against a real matchbox_server, and relay or NAT traversal, persistence, friends, chat and a Bevy plugin are out of scope for now.

## Development

The usual tasks are in `Taskfile.dist.yaml` (run `task` for the list):

```shell
task test     # unit, property and integration tests
task check    # fmt, clippy, the client on wasm32, the server without default features
task ci       # check, test, then a short fuzz run
```

The property tests throw random message sequences from several players at the state machine and check its internal bookkeeping after every step, and do the same with mutated login tokens. The fuzz targets in `fuzz/` (cargo-fuzz, nightly) cover incoming WebSocket frames, incoming tokens and the state machine again; `task fuzz:smoke` runs each briefly, `task fuzz:run TARGET=core_ops` for as long as you like.

GitHub Actions runs the same checks on every push to `main` and every pull request, plus the renet example, a 30-second fuzz run per target, and [actionlint](https://github.com/rhysd/actionlint) and [zizmor](https://github.com/zizmorcore/zizmor) on the workflows themselves (`task lint:actions` does the latter locally). Pushing a tag `vX.Y.Z` that matches the crate version builds a static `x86_64-unknown-linux-musl` server binary and attaches it, with a SHA-256 checksum, to a GitHub release; nothing is published to crates.io yet.

## License

Licensed under the [European Union Public Licence v. 1.2](LICENSE) (EUPL-1.2).

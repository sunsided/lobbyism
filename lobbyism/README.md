# lobbyism

Lobbies and matchmaking for Rust games: this crate is the wire protocol and the async client; players find each other through a [`lobbyism-server`](https://crates.io/crates/lobbyism-server), then connect with the transport the game already uses.

The client authenticates with a token from your own login backend, joins a lobby or a matchmaking queue, and receives a connection ticket when a match forms: a renet / netcode.io `ConnectToken` for a dedicated game server, or a matchbox room URL for peer-to-peer play (the player list doubles as the GGRS peer list). The lobby never carries gameplay traffic. The client runs on tokio and in the browser (`wasm32-unknown-unknown`), as it's built on `tokio-tungstenite-wasm`.

## Usage

```shell
cargo add lobbyism
```

Queueing for a match and picking up the ticket:

```rust
use lobbyism::{ClientMsg, LobbyClient, ServerMsg, Ticket};

let mut client = LobbyClient::connect("wss://lobby.example.com", &token).await?;
client.send(ClientMsg::Enqueue { mode: "duel".into(), region: "eu".into(), skill: 1200 }).await?;
while let Some(msg) = client.recv().await? {
    if let ServerMsg::Matched(m) = msg {
        // m.players is the match in handle order, m.handle is our index in it
        match m.ticket {
            Ticket::Netcode { connect_token, .. } => {
                // renet_netcode::ConnectToken::read(&mut connect_token.as_slice())
            }
            Ticket::Matchbox { room_url } => {
                // matchbox_socket::WebRtcSocket::builder(room_url)
            }
        }
        break;
    }
}
```

Lobbies work the same way with `CreateLobby`, `JoinLobby`, `SetMetadata`, `SetReady`, `StartMatch` and `LeaveLobby`. Every member receives the full lobby state after each change, and a `HostMigrated` message when the host leaves and the next member in join order takes over.

## Features

- `client` (default): the async WebSocket client, `LobbyClient`.
- Without it (`default-features = false`), the crate is the protocol only: the `ClientMsg` / `ServerMsg` enums and the types around them, serialized as JSON tagged by `type`. `lobbyism-server` uses it that way.

## More

The [project README](https://github.com/sunsided/lobbyism#readme) covers the whole setup: how the pieces fit together, running and hosting the server, and NAT traversal for peer-to-peer play. API documentation is on [docs.rs](https://docs.rs/lobbyism).

## License

Licensed under the [European Union Public Licence v. 1.2](https://interoperable-europe.ec.europa.eu/collection/eupl/eupl-text-eupl-12) (EUPL-1.2); the full text is in `LICENSE`.

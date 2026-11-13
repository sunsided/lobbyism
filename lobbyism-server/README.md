# lobbyism-server

The lobby and matchmaking server for [`lobbyism`](https://crates.io/crates/lobbyism): a WebSocket service where players meet in lobbies or queue by mode, region and skill bucket, and get a connection ticket for the game's own transport when a match forms. It ships as a binary and as a library to embed.

## Running the binary

```shell
cargo install lobbyism-server
```

Static Linux binaries are attached to the [GitHub releases](https://github.com/sunsided/lobbyism/releases), built with cargo-auditable and with a build provenance attestation (`gh attestation verify <archive> -R sunsided/lobbyism`); v0.1.0 predates both and only has a checksum.

The binary is configured through environment variables, or a `.env` file in the working directory (variables already set in the environment win):

| Variable | Meaning | Default |
|---|---|---|
| `LOBBYISM_BIND` | Listen address | `127.0.0.1:7878` |
| `LOBBYISM_HMAC_SECRET` | Secret for client tokens, at least 32 bytes | required |
| `LOBBYISM_NETCODE_KEY` | netcode private key shared with the game server, 64 hex characters | required |
| `LOBBYISM_PROTOCOL_ID` | netcode protocol id of your game | required |
| `LOBBYISM_GAME_SERVERS` | Comma-separated public game server addresses, 1 to 32 | required |
| `LOBBYISM_MODES` | Queue modes and players per match (at least 1), e.g. `duel=2,squad=4` | `duel=2` |
| `LOBBYISM_REGIONS` | Comma-separated regions clients may queue in | `eu,us` |

Your login backend signs client tokens with the same HMAC secret, and your renet game server runs `ServerAuthentication::Secure` with the same netcode key and protocol id; neither secret may reach a client. The server speaks plain `ws://`, so in production it belongs behind a reverse proxy that terminates TLS. The [operator section of the project README](https://github.com/sunsided/lobbyism#as-an-operator) covers hosting, proxy timeouts, clocks and the other network details.

## Embedding

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

`Core` is the lobby and queue state machine; it doesn't touch sockets, so it can also sit behind a different front end. Authentication goes through the `TokenValidator` trait (`HmacValidator` is the default), and tickets through `TicketIssuer`: `NetcodeIssuer` for renet / netcode.io, `MatchboxIssuer` for matchbox and GGRS.

## Features

- `netcode` (default): `NetcodeIssuer`, via `renet_netcode`.
- `cli` (default): the `lobbyism-server` binary.
- `dotenv` (default): the binary reads `.env`.

With `default-features = false`, the crate is the library with `MatchboxIssuer` only.

## License

Licensed under the [European Union Public Licence v. 1.2](https://interoperable-europe.ec.europa.eu/collection/eupl/eupl-text-eupl-12) (EUPL-1.2); the full text is in `LICENSE`.

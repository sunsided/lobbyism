//! LAN play: one player's process hosts the lobby and the renet game server,
//! another player on the same network finds it by UDP broadcast and joins.
//!
//! On one machine: `cargo run -p lobbyism --example lan_match --features lan-broadcast -- host`
//! On another (or the same): `cargo run -p lobbyism --example lan_match --features lan-broadcast -- join`
//! Without discovery: `... -- join ws://192.168.1.20:41234 <lobby id>`, with
//! the URL and lobby id the host prints.

use lobbyism::LobbyClient;
use lobbyism::lan::discover_broadcast;
use lobbyism::proto::{ClientMsg, LanAnnouncement, MatchInfo, Metadata, ServerMsg, Ticket};
use lobbyism_server::lan::{announce_broadcast, lan_addresses};
use lobbyism_server::{Config, Core, LanValidator, NetcodeIssuer, serve};
use renet::{ConnectionConfig, DefaultChannel, RenetClient, RenetServer, ServerEvent};
use renet_netcode::{
    ClientAuthentication, ConnectToken, NetcodeClientTransport, NetcodeServerTransport,
    ServerAuthentication, ServerConfig, generate_random_bytes,
};
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// netcode protocol id of this example game.
const PROTOCOL_ID: u64 = 0x10bb_1500;
/// Game name in announcements; joiners ignore other games.
const GAME: &str = "lobbyism-lan-example/1";
/// UDP port announcements are broadcast to.
const DISCOVERY_PORT: u16 = 47_800;
/// Tick of the renet loops.
const TICK: Duration = Duration::from_millis(10);

/// Error type that crosses task boundaries.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Runs as `host`, or as `join` with or without an explicit lobby URL and id.
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["host"] => host().await,
        ["join"] => {
            let hosts = discover_broadcast(DISCOVERY_PORT, GAME, Duration::from_secs(3)).await?;
            let found = hosts.first().ok_or("no host found on the network")?;
            println!("found {:?} at {}", found.announcement.name, found.url());
            join(&found.url(), found.announcement.lobby).await
        }
        ["join", url, lobby] => join(url, lobby.parse()?).await,
        _ => Err("usage: lan_match host | join [ws://<ip>:<port> <lobby id>]".into()),
    }
}

/// Hosts the lobby and the game server, announces the lobby and plays once someone joined.
async fn host() -> Result<(), BoxError> {
    // The netcode key is made up per session and never leaves this process.
    let private_key: [u8; 32] = generate_random_bytes();
    let game_socket = UdpSocket::bind("0.0.0.0:0")?;
    let addresses = lan_addresses(game_socket.local_addr()?.port())?;
    if addresses.is_empty() {
        return Err("no network interface besides loopback".into());
    }
    let issuer = NetcodeIssuer {
        protocol_id: PROTOCOL_ID,
        private_key,
        server_addresses: addresses.clone(),
        expire_seconds: 30,
        timeout_seconds: 5,
    };
    let game_server =
        std::thread::spawn(move || game_server(game_socket, private_key, addresses, 2));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let lobby_port = listener.local_addr()?.port();
    let validator = LanValidator::open();
    let code_required = validator.code_required();
    // A lobby-only server: no queue modes.
    let config = Config {
        modes: HashMap::new(),
        ..Config::default()
    };
    tokio::spawn(serve(listener, Core::new(config, issuer), validator));

    let mut me = LobbyClient::connect(&format!("ws://127.0.0.1:{lobby_port}"), "").await?;
    me.send(ClientMsg::CreateLobby {
        max_players: 2,
        metadata: Metadata::new(),
    })
    .await?;
    let lobby = loop {
        match me.recv().await?.ok_or("lobby server closed")? {
            ServerMsg::Lobby(info) => break info.id,
            ServerMsg::Error { message } => return Err(message.into()),
            _ => {}
        }
    };
    let announcement = LanAnnouncement {
        game: GAME.into(),
        name: "lan_match host".into(),
        lobby_port,
        lobby,
        code_required,
    };
    let announcer =
        tokio::spawn(async move { announce_broadcast(DISCOVERY_PORT, &announcement).await });
    println!("hosting lobby {lobby} on port {lobby_port}, waiting for a player to join");

    me.send(ClientMsg::SetReady { ready: true }).await?;
    let m = loop {
        match me.recv().await?.ok_or("lobby server closed")? {
            ServerMsg::Lobby(info)
                if info.members.len() == 2 && info.members.iter().all(|m| m.ready) =>
            {
                me.send(ClientMsg::StartMatch).await?;
            }
            ServerMsg::Matched(m) => break m,
            ServerMsg::Error { message } => return Err(message.into()),
            _ => {}
        }
    };
    announcer.abort();
    tokio::task::spawn_blocking(move || play(m)).await??;
    game_server.join().map_err(|_| "game server panicked")?
}

/// Joins the lobby at `url`, marks itself ready and plays once the host starts.
async fn join(url: &str, lobby: u64) -> Result<(), BoxError> {
    let mut client = LobbyClient::connect(url, "").await?;
    client.send(ClientMsg::JoinLobby { lobby }).await?;
    client.send(ClientMsg::SetReady { ready: true }).await?;
    while let Some(msg) = client.recv().await? {
        match msg {
            ServerMsg::Matched(m) => return tokio::task::spawn_blocking(move || play(m)).await?,
            ServerMsg::Error { message } => return Err(message.into()),
            _ => {}
        }
    }
    Err("lobby closed before the match started".into())
}

/// The renet game server: greets every player and returns once `players` connected.
fn game_server(
    socket: UdpSocket,
    private_key: [u8; 32],
    public_addresses: Vec<SocketAddr>,
    players: usize,
) -> Result<(), BoxError> {
    let mut server = RenetServer::new(ConnectionConfig::default());
    let mut transport = NetcodeServerTransport::new(
        ServerConfig {
            current_time: now(),
            max_clients: players,
            protocol_id: PROTOCOL_ID,
            public_addresses,
            authentication: ServerAuthentication::Secure { private_key },
        },
        socket,
    )?;
    let mut greeted = 0;
    let mut last = Instant::now();
    // Keep serving a little after the last greeting, so it gets delivered.
    let mut done_at = None;
    while done_at.is_none_or(|t: Instant| t.elapsed() < Duration::from_secs(1)) {
        std::thread::sleep(TICK);
        let dt = last.elapsed();
        last = Instant::now();
        server.update(dt);
        transport.update(dt, &mut server)?;
        while let Some(event) = server.get_event() {
            if let ServerEvent::ClientConnected { client_id } = event {
                println!("game server: player {client_id} connected");
                server.send_message(
                    client_id,
                    DefaultChannel::ReliableOrdered,
                    format!("welcome {client_id}"),
                );
                greeted += 1;
                if greeted == players {
                    done_at = Some(Instant::now());
                }
            }
        }
        transport.send_packets(&mut server);
    }
    Ok(())
}

/// Connects to the game server with the match ticket and waits for the greeting.
fn play(m: MatchInfo) -> Result<(), BoxError> {
    let Ticket::Netcode { connect_token, .. } = m.ticket else {
        return Err("expected a netcode ticket".into());
    };
    let connect_token = ConnectToken::read(&mut connect_token.as_slice())?;
    let mut client = RenetClient::new(ConnectionConfig::default());
    let mut transport = NetcodeClientTransport::new(
        now(),
        ClientAuthentication::Secure { connect_token },
        UdpSocket::bind("0.0.0.0:0")?,
    )?;
    let start = Instant::now();
    let mut last = start;
    while start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(TICK);
        let dt = last.elapsed();
        last = Instant::now();
        client.update(dt);
        transport.update(dt, &mut client)?;
        if let Some(msg) = client.receive_message(DefaultChannel::ReliableOrdered) {
            println!(
                "connected over renet, server says {:?}",
                String::from_utf8_lossy(&msg)
            );
            return Ok(());
        }
        transport.send_packets(&mut client)?;
    }
    Err("timed out connecting to the game server".into())
}

/// Time since the Unix epoch, as netcode wants it.
fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

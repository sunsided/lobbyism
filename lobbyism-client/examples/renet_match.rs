//! Two players queue for a duel, get matched, and connect to a renet game
//! server with the `ConnectToken` the lobby issued.
//!
//! Everything runs in one process: a renet game server, the lobby server and
//! two lobby clients. Run with `cargo run -p lobbyism-client --example renet_match`.

use lobbyism_client::LobbyClient;
use lobbyism_client::proto::{ClientMsg, MatchInfo, ServerMsg, Ticket};
use lobbyism_server::{Config, Core, HmacValidator, NetcodeIssuer, serve};
use renet::{ConnectionConfig, DefaultChannel, RenetClient, RenetServer, ServerEvent};
use renet_netcode::{
    ClientAuthentication, ConnectToken, NetcodeClientTransport, NetcodeServerTransport,
    ServerAuthentication, ServerConfig, generate_random_bytes,
};
use std::net::UdpSocket;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PROTOCOL_ID: u64 = 0x10bb_1500;
const HMAC_SECRET: &[u8] = b"example-secret-example-secret-32";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // The game server and the lobby share the netcode private key; clients never see it.
    let private_key: [u8; 32] = generate_random_bytes();
    let game_socket = UdpSocket::bind("127.0.0.1:0")?;
    let game_addr = game_socket.local_addr()?;

    let issuer = NetcodeIssuer {
        protocol_id: PROTOCOL_ID,
        private_key,
        server_addresses: vec![game_addr],
        expire_seconds: 30,
        timeout_seconds: 5,
    };
    let lobby_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let lobby_url = format!("ws://{}", lobby_listener.local_addr()?);
    let validator = HmacValidator::new(HMAC_SECRET)?;
    tokio::spawn(serve(
        lobby_listener,
        Core::new(Config::default(), issuer),
        validator.clone(),
    ));

    // Your auth backend would hand these out; here we sign them directly.
    let expires = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 60;
    let (a, b) = tokio::try_join!(
        queue_for_duel(&lobby_url, validator.sign(1001, expires)),
        queue_for_duel(&lobby_url, validator.sign(1002, expires)),
    )?;
    println!("lobby matched players {:?} into match {}", a.players, a.id);

    tokio::task::spawn_blocking(move || play(game_socket, private_key, [a, b])).await?
}

async fn queue_for_duel(url: &str, token: String) -> Result<MatchInfo, BoxError> {
    let mut client = LobbyClient::connect(url, &token).await?;
    client
        .send(ClientMsg::Enqueue {
            mode: "duel".into(),
            region: "eu".into(),
            skill: 1200,
        })
        .await?;
    while let Some(msg) = client.recv().await? {
        match msg {
            ServerMsg::Matched(m) => return Ok(m),
            ServerMsg::Error { message } => return Err(message.into()),
            _ => {}
        }
    }
    Err("lobby closed before a match was found".into())
}

/// Run the renet game server and both game clients until both are connected.
fn play(
    game_socket: UdpSocket,
    private_key: [u8; 32],
    matches: [MatchInfo; 2],
) -> Result<(), BoxError> {
    let now = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let mut server = RenetServer::new(ConnectionConfig::default());
    let mut server_transport = NetcodeServerTransport::new(
        ServerConfig {
            current_time: now(),
            max_clients: 8,
            protocol_id: PROTOCOL_ID,
            public_addresses: vec![game_socket.local_addr()?],
            authentication: ServerAuthentication::Secure { private_key },
        },
        game_socket,
    )?;

    let mut clients = Vec::new();
    for m in &matches {
        let Ticket::Netcode { connect_token, .. } = &m.ticket else {
            return Err("expected netcode ticket".into());
        };
        let connect_token = ConnectToken::read(&mut connect_token.as_slice())?;
        let transport = NetcodeClientTransport::new(
            now(),
            ClientAuthentication::Secure { connect_token },
            UdpSocket::bind("127.0.0.1:0")?,
        )?;
        clients.push((RenetClient::new(ConnectionConfig::default()), transport));
    }

    let start = Instant::now();
    let mut last = start;
    let mut greeted = 0;
    while greeted < clients.len() {
        if start.elapsed() > Duration::from_secs(5) {
            return Err("timed out waiting for renet connections".into());
        }
        std::thread::sleep(Duration::from_millis(10));
        let dt = last.elapsed();
        last = Instant::now();

        server.update(dt);
        server_transport.update(dt, &mut server)?;
        while let Some(event) = server.get_event() {
            if let ServerEvent::ClientConnected { client_id } = event {
                let user_data = server_transport.user_data(client_id).unwrap();
                let match_id = u64::from_le_bytes(user_data[..8].try_into().unwrap());
                println!("game server: player {client_id} joined match {match_id}");
                server.send_message(
                    client_id,
                    DefaultChannel::ReliableOrdered,
                    format!("welcome {client_id}"),
                );
            }
        }
        server_transport.send_packets(&mut server);

        for (client, transport) in &mut clients {
            client.update(dt);
            transport.update(dt, client)?;
            while let Some(msg) = client.receive_message(DefaultChannel::ReliableOrdered) {
                println!(
                    "game client {}: server says {:?}",
                    transport.client_id(),
                    String::from_utf8_lossy(&msg)
                );
                greeted += 1;
            }
            transport.send_packets(client)?;
        }
    }
    println!("both players connected over renet");
    Ok(())
}

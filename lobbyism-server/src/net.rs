//! WebSocket front end: JSON text frames carrying `lobbyism::proto` messages.

use crate::{Core, Outbox, TicketIssuer, TokenValidator};
use futures_util::{SinkExt, StreamExt};
use lobbyism::proto::{ClientMsg, PlayerId, ServerMsg};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

/// Largest accepted client frame; protocol messages are tiny.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
/// Outgoing messages buffered per client before it counts as too slow and is dropped.
const OUTBOX_CAPACITY: usize = 256;
/// Pause after a failed `accept()`, so running out of file descriptors doesn't spin.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Time limits for one connection.
#[derive(Debug, Clone, Copy)]
struct Timeouts {
    /// Deadline from accepting the TCP connection to receiving `Hello`,
    /// WebSocket handshake included.
    hello: Duration,
    /// Interval of the server's WebSocket pings.
    ping: Duration,
    /// A client that sends nothing for this long, not even a pong, counts as gone.
    idle: Duration,
    /// Longest time one outgoing frame may take; a peer that stops reading counts as gone.
    send: Duration,
}

/// The limits `serve` uses; tests run with shorter ones.
const TIMEOUTS: Timeouts = Timeouts {
    hello: Duration::from_secs(10),
    ping: Duration::from_secs(20),
    idle: Duration::from_secs(60),
    send: Duration::from_secs(10),
};

/// State shared by all connections, behind one mutex.
struct Shared<I> {
    /// The lobby and queue state machine.
    core: Core<I>,
    /// Live connection per player, tagged with a connection id so a stale
    /// connection never tears down a newer one for the same player.
    sessions: HashMap<PlayerId, (u64, mpsc::Sender<ServerMsg>)>,
}

impl<I: TicketIssuer> Shared<I> {
    /// Registers connection `conn` as `player`'s session and queues its
    /// `Welcome`; refuses (`false`) while the player has another session.
    fn register(&mut self, player: PlayerId, conn: u64, tx: mpsc::Sender<ServerMsg>) -> bool {
        if self.sessions.contains_key(&player) {
            return false;
        }
        tx.try_send(ServerMsg::Welcome { player })
            .expect("fresh channel has room");
        self.sessions.insert(player, (conn, tx));
        true
    }

    /// Applies one client message and queues the replies.
    ///
    /// Handling and delivery happen under the same lock on purpose: with the
    /// lock released in between, two lobby members acting at once could queue
    /// their snapshots in the opposite order and both end up showing the older one.
    fn handle(&mut self, player: PlayerId, msg: ClientMsg) {
        let out = self.core.handle(player, msg);
        self.deliver(out);
    }

    /// Ends connection `conn` of `player`: removes the session and the player's
    /// lobby and queue state, unless a newer connection has taken over since.
    fn unregister(&mut self, player: PlayerId, conn: u64) {
        let superseded = self.sessions.get(&player).is_some_and(|(c, _)| *c != conn);
        if !superseded {
            self.sessions.remove(&player);
            let out = self.core.disconnect(player);
            self.deliver(out);
        }
    }
}

impl<I> Shared<I> {
    /// Queues each message for its recipient; recipients that are gone or too
    /// slow to keep up lose their session.
    fn deliver(&mut self, out: Outbox) {
        for (player, msg) in out {
            let Some((_, tx)) = self.sessions.get(&player) else {
                continue;
            };
            if tx.try_send(msg).is_err() {
                // Dropping the sender lets the writer flush what's queued and stop;
                // a writer stuck on a dead peer stops at the send timeout. Either
                // way the connection's read loop then ends and removes the player.
                tracing::warn!(player, "client outbox full or closed; dropping session");
                self.sessions.remove(&player);
            }
        }
    }
}

/// Accepts WebSocket clients on `listener` and serves them.
///
/// Runs until the surrounding task is dropped: failed accepts are logged and
/// retried, so it doesn't return in practice. The `Result` is kept for
/// compatibility.
pub async fn serve<I, V>(listener: TcpListener, core: Core<I>, validator: V) -> std::io::Result<()>
where
    I: TicketIssuer + 'static,
    V: TokenValidator,
{
    serve_with(listener, core, validator, TIMEOUTS).await
}

/// [`serve`] with explicit time limits.
async fn serve_with<I, V>(
    listener: TcpListener,
    core: Core<I>,
    validator: V,
    timeouts: Timeouts,
) -> std::io::Result<()>
where
    I: TicketIssuer + 'static,
    V: TokenValidator,
{
    let shared = Arc::new(Mutex::new(Shared {
        core,
        sessions: HashMap::new(),
    }));
    let validator = Arc::new(validator);
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(conn) => conn,
            // Aborted handshakes and running out of file descriptors are
            // transient; only this one connection is lost, not the server.
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let (shared, validator) = (shared.clone(), validator.clone());
        tokio::spawn(async move {
            if let Err(e) = connection(stream, shared, &*validator, timeouts).await {
                tracing::debug!(%addr, "connection ended: {e}");
            }
        });
    }
}

/// Serves one client: WebSocket handshake, `Hello` authentication, then
/// messages until the client leaves; cleans up the player's lobby and queue state.
async fn connection<I: TicketIssuer>(
    stream: TcpStream,
    shared: Arc<Mutex<Shared<I>>>,
    validator: &dyn TokenValidator,
    timeouts: Timeouts,
) -> Result<(), String> {
    // One deadline for handshake and Hello: a client that connects and stays
    // silent must not hold a task and a file descriptor indefinitely.
    let deadline = tokio::time::Instant::now() + timeouts.hello;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let ws = tokio::time::timeout_at(
        deadline,
        tokio_tungstenite::accept_async_with_config(stream, Some(config)),
    )
    .await
    .map_err(|_| "handshake timeout".to_string())?
    .map_err(|e| e.to_string())?;
    let (mut sink, mut source) = ws.split();

    // Authenticate before touching shared state.
    let hello = tokio::time::timeout_at(deadline, next_msg(&mut source, timeouts.idle))
        .await
        .map_err(|_| "hello timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let auth = match hello {
        Some(ClientMsg::Hello { token }) => validator.validate(&token),
        Some(_) => Err("first message must be hello".into()),
        None => return Ok(()),
    };
    let player = match auth {
        Ok(player) => player,
        Err(message) => {
            let _ = sink
                .send(encode(&ServerMsg::Error {
                    message: message.clone(),
                }))
                .await;
            return Err(message);
        }
    };

    /// Source of per-connection ids that tell sessions of the same player apart.
    static NEXT_CONN: AtomicU64 = AtomicU64::new(0);
    let conn = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel(OUTBOX_CAPACITY);
    if !shared.lock().unwrap().register(player, conn, tx) {
        let message = "player already connected".to_string();
        let _ = sink
            .send(encode(&ServerMsg::Error {
                message: message.clone(),
            }))
            .await;
        return Err(message);
    }

    // The writer also pings, so a vanished client (no TCP close, e.g. a phone
    // losing signal) stops producing pongs and the read loop's idle timeout fires.
    let mut writer = tokio::spawn(async move {
        let first_ping = tokio::time::Instant::now() + timeouts.ping;
        let mut ping = tokio::time::interval_at(first_ping, timeouts.ping);
        loop {
            let frame = tokio::select! {
                msg = rx.recv() => match msg {
                    Some(msg) => encode(&msg),
                    None => break,
                },
                _ = ping.tick() => Message::Ping(Vec::new().into()),
            };
            match tokio::time::timeout(timeouts.send, sink.send(frame)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        let _ = tokio::time::timeout(timeouts.send, sink.close()).await;
    });

    let result = loop {
        let next = tokio::select! {
            next = next_msg(&mut source, timeouts.idle) => next,
            // Writer stopped: socket broken, peer not reading, or session dropped as too slow.
            _ = &mut writer => break Err("writer closed".into()),
        };
        match next {
            Ok(Some(msg)) => shared.lock().unwrap().handle(player, msg),
            Ok(None) => break Ok(()),
            // A malformed message gets an error reply; the connection stays usable.
            Err(ReadError::Bad(message)) => shared
                .lock()
                .unwrap()
                .deliver(vec![(player, ServerMsg::Error { message })]),
            Err(ReadError::Fatal(e)) => break Err(e),
        }
    };

    shared.lock().unwrap().unregister(player, conn);
    writer.abort();
    result
}

/// Why reading the next client message failed.
#[derive(Debug)]
enum ReadError {
    /// The frame arrived but isn't a valid message; reply and keep the connection.
    Bad(String),
    /// The connection itself failed; stop serving it.
    Fatal(String),
}

impl std::fmt::Display for ReadError {
    /// The reason, without the kind.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Bad(reason) | ReadError::Fatal(reason) => f.write_str(reason),
        }
    }
}

/// Next client message; `Ok(None)` on a clean close. Fails when no frame at
/// all (pongs included) arrives within `idle`.
async fn next_msg<S>(source: &mut S, idle: Duration) -> Result<Option<ClientMsg>, ReadError>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let Ok(frame) = tokio::time::timeout(idle, source.next()).await else {
            return Err(ReadError::Fatal("idle timeout".into()));
        };
        match frame {
            None | Some(Ok(Message::Close(_))) => return Ok(None),
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text)
                    .map(Some)
                    .map_err(|e| ReadError::Bad(format!("bad message: {e}")));
            }
            Some(Ok(Message::Binary(_))) => {
                return Err(ReadError::Bad("bad message: expected text frame".into()));
            }
            Some(Ok(_)) => continue, // ping/pong are handled by tungstenite
            Some(Err(e)) => return Err(ReadError::Fatal(e.to_string())),
        }
    }
}

/// Encodes a server message as a JSON text frame.
fn encode(msg: &ServerMsg) -> Message {
    Message::text(serde_json::to_string(msg).expect("ServerMsg serializes"))
}

/// End-to-end tests of the WebSocket front end over real sockets.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, HmacValidator, MatchboxIssuer};
    use lobbyism::proto::Metadata;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    /// A WebSocket client connection as the tests use it.
    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    /// Test-only HMAC secret, exactly 32 bytes.
    const SECRET: &[u8] = b"net-test-secret-net-test-secret!";

    /// Short limits, so timeout tests finish quickly.
    const TEST_TIMEOUTS: Timeouts = Timeouts {
        hello: Duration::from_millis(300),
        ping: Duration::from_millis(100),
        idle: Duration::from_millis(300),
        send: Duration::from_millis(300),
    };

    /// Starts a server with the default config and [`TEST_TIMEOUTS`] on a random
    /// local port; returns its address.
    async fn start() -> String {
        start_with(TEST_TIMEOUTS).await
    }

    /// Like [`start`], with explicit time limits.
    async fn start_with(timeouts: Timeouts) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let core = Core::new(Config::default(), MatchboxIssuer::new("ws://mb").unwrap());
        let validator = HmacValidator::new(SECRET).unwrap();
        tokio::spawn(serve_with(listener, core, validator, timeouts));
        addr
    }

    /// Opens a WebSocket connection without authenticating.
    async fn connect(addr: &str) -> Client {
        tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap()
            .0
    }

    /// Opens a connection and authenticates as `player`.
    async fn login(addr: &str, player: PlayerId) -> Client {
        let mut ws = connect(addr).await;
        let token = HmacValidator::new(SECRET)
            .unwrap()
            .sign(player, u64::MAX / 2);
        send(&mut ws, &ClientMsg::Hello { token }).await;
        assert_eq!(recv(&mut ws).await, ServerMsg::Welcome { player });
        ws
    }

    /// Sends one client message.
    async fn send(ws: &mut Client, msg: &ClientMsg) {
        ws.send(Message::text(serde_json::to_string(msg).unwrap()))
            .await
            .unwrap();
    }

    /// The next server message, failing the test after 5 seconds.
    async fn recv(ws: &mut Client) -> ServerMsg {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("server message within 5 s")
                .expect("connection open")
                .unwrap();
            if let Message::Text(text) = frame {
                return serde_json::from_str(&text).unwrap();
            }
        }
    }

    /// Sends a duel queue request for the `eu` region.
    async fn enqueue(ws: &mut Client) {
        let msg = ClientMsg::Enqueue {
            mode: "duel".into(),
            region: "eu".into(),
            skill: 100,
        };
        send(ws, &msg).await;
    }

    /// A frame that isn't a valid message gets an error reply, and the
    /// connection keeps working afterwards.
    #[tokio::test]
    async fn malformed_message_gets_error_and_connection_survives() {
        let addr = start().await;
        let mut ws = login(&addr, 1).await;

        ws.send(Message::text("not json")).await.unwrap();
        assert!(matches!(recv(&mut ws).await, ServerMsg::Error { .. }));

        enqueue(&mut ws).await;
        assert!(matches!(recv(&mut ws).await, ServerMsg::Queued { .. }));
    }

    /// A TCP connection that never starts the WebSocket handshake is closed
    /// by the server once the hello deadline passes.
    #[tokio::test]
    async fn silent_tcp_connection_is_closed() {
        use tokio::io::AsyncReadExt;
        let addr = start().await;
        let mut tcp = TcpStream::connect(&addr).await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf))
            .await
            .expect("server closes the connection within 5 s");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "connection closed, got {read:?}"
        );
    }

    /// The next text message within `wait`, or `None`; answers pings meanwhile
    /// by polling the stream.
    async fn try_recv(ws: &mut Client, wait: Duration) -> Option<ServerMsg> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match tokio::time::timeout_at(deadline, ws.next()).await {
                Err(_) => return None,
                Ok(Some(Ok(Message::Text(text)))) => {
                    return Some(serde_json::from_str(&text).unwrap());
                }
                Ok(Some(Ok(_))) => continue,
                Ok(other) => panic!("connection ended: {other:?}"),
            }
        }
    }

    /// A queued client that stops responding (no pongs, no close) is removed
    /// from the queue, so the next player is not matched with it.
    #[tokio::test]
    async fn vanished_client_is_not_matched() {
        let addr = start().await;
        let mut gone = login(&addr, 1).await;
        enqueue(&mut gone).await;
        assert!(matches!(recv(&mut gone).await, ServerMsg::Queued { .. }));
        // From here on `gone` is never polled again, like a peer whose network vanished.

        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut next = login(&addr, 2).await;
        enqueue(&mut next).await;
        assert!(matches!(recv(&mut next).await, ServerMsg::Queued { .. }));
        let matched = try_recv(&mut next, Duration::from_secs(1)).await;
        assert_eq!(matched, None, "matched with a vanished player");
        drop(gone);
    }

    /// A client that keeps polling answers the pings and outlives the idle timeout.
    #[tokio::test]
    async fn responsive_client_outlives_idle_timeout() {
        let addr = start().await;
        let mut ws = login(&addr, 1).await;
        enqueue(&mut ws).await;
        assert!(matches!(recv(&mut ws).await, ServerMsg::Queued { .. }));

        // Several idle periods long; polling answers the server's pings.
        assert_eq!(try_recv(&mut ws, Duration::from_secs(1)).await, None);
        send(&mut ws, &ClientMsg::CancelQueue).await;
        assert_eq!(recv(&mut ws).await, ServerMsg::Dequeued);
    }

    /// A lobby member that is connected but never reads is removed once the
    /// server can't get frames to it, even with a long idle timeout: the full
    /// outbox drops the session and the blocked writer gives up at the send timeout.
    #[tokio::test]
    async fn client_that_stops_reading_is_removed() {
        let addr = start_with(Timeouts {
            idle: Duration::from_secs(60),
            ..TEST_TIMEOUTS
        })
        .await;
        let mut host = login(&addr, 1).await;
        let create = ClientMsg::CreateLobby {
            max_players: 2,
            metadata: Default::default(),
        };
        send(&mut host, &create).await;
        let ServerMsg::Lobby(lobby) = recv(&mut host).await else {
            panic!("lobby expected")
        };
        let mut reader = login(&addr, 2).await;
        send(&mut reader, &ClientMsg::JoinLobby { lobby: lobby.id }).await;
        assert!(matches!(recv(&mut reader).await, ServerMsg::Lobby(_)));
        // From here on `reader` is never polled; every metadata change below is
        // broadcast to it and piles up in its socket and outbox.

        let big = Metadata::from([("k".to_string(), "v".repeat(4000))]);
        for _ in 0..2000 {
            let update = ClientMsg::SetMetadata {
                metadata: big.clone(),
            };
            send(&mut host, &update).await;
            let ServerMsg::Lobby(state) = recv(&mut host).await else {
                panic!("lobby update expected")
            };
            if state.members.len() == 1 {
                drop(reader);
                return;
            }
        }
        // The removal can land after the flood: the blocked writer first has to
        // hit its send timeout.
        while let Some(msg) = try_recv(&mut host, Duration::from_secs(5)).await {
            if matches!(&msg, ServerMsg::Lobby(state) if state.members.len() == 1) {
                drop(reader);
                return;
            }
        }
        panic!("the non-reading member was never removed");
    }
}

/// Loom models of the session bookkeeping. Connection tasks touch [`Shared`]
/// only through single method calls under the lock; these models check every
/// interleaving of two such calls. (Sockets and timers are out of loom's reach,
/// so they stay with the end-to-end tests above.)
#[cfg(test)]
mod loom_model {
    use super::*;
    use crate::{Config, MatchboxIssuer};
    use lobbyism::proto::{LobbyInfo, Metadata};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    /// A fresh server state with no sessions.
    fn shared() -> Shared<MatchboxIssuer> {
        Shared {
            core: Core::new(Config::default(), MatchboxIssuer::new("ws://mb").unwrap()),
            sessions: HashMap::new(),
        }
    }

    /// Registers `player` on connection `conn` with an outbox of `capacity`.
    fn session(
        s: &mut Shared<MatchboxIssuer>,
        player: PlayerId,
        conn: u64,
        capacity: usize,
    ) -> mpsc::Receiver<ServerMsg> {
        let (tx, rx) = mpsc::channel(capacity);
        assert!(s.register(player, conn, tx));
        rx
    }

    /// Drains `rx` and returns the last lobby snapshot in it.
    fn last_lobby(rx: &mut mpsc::Receiver<ServerMsg>) -> Option<LobbyInfo> {
        let mut last = None;
        while let Ok(msg) = rx.try_recv() {
            if let ServerMsg::Lobby(info) = msg {
                last = Some(info);
            }
        }
        last
    }

    /// Two members changing their lobby at once both end up with a snapshot
    /// that has both changes, whichever order the changes land in.
    #[test]
    fn concurrent_lobby_changes_end_on_the_latest_snapshot() {
        loom::model(|| {
            let mut s = shared();
            let mut host = session(&mut s, 1, 1, OUTBOX_CAPACITY);
            let mut guest = session(&mut s, 2, 2, OUTBOX_CAPACITY);
            s.handle(
                1,
                ClientMsg::CreateLobby {
                    max_players: 2,
                    metadata: Metadata::new(),
                },
            );
            let lobby = last_lobby(&mut host).unwrap().id;
            s.handle(2, ClientMsg::JoinLobby { lobby });

            let s = Arc::new(Mutex::new(s));
            let set_map = thread::spawn({
                let s = s.clone();
                move || {
                    let metadata = Metadata::from([("map".into(), "dust".into())]);
                    s.lock()
                        .unwrap()
                        .handle(1, ClientMsg::SetMetadata { metadata });
                }
            });
            let set_ready = thread::spawn({
                let s = s.clone();
                move || {
                    s.lock()
                        .unwrap()
                        .handle(2, ClientMsg::SetReady { ready: true })
                }
            });
            set_map.join().unwrap();
            set_ready.join().unwrap();

            for rx in [&mut host, &mut guest] {
                let info = last_lobby(rx).unwrap();
                assert_eq!(info.metadata.get("map").map(String::as_str), Some("dust"));
                assert!(info.members.iter().all(|m| m.ready || m.player == 1));
            }
        });
    }

    /// A dropped connection that is still winding down never removes the
    /// session of a newer connection for the same player.
    #[test]
    fn ending_connection_never_removes_a_newer_one() {
        loom::model(|| {
            let mut s = shared();
            // Room for the Welcome only: the lobby snapshot overflows the outbox,
            // so deliver() drops the session as too slow, like a stalled client.
            let _old = session(&mut s, 1, 1, 1);
            s.handle(
                1,
                ClientMsg::CreateLobby {
                    max_players: 2,
                    metadata: Metadata::new(),
                },
            );
            assert!(!s.sessions.contains_key(&1), "slow session not dropped");

            let s = Arc::new(Mutex::new(s));
            let ending = thread::spawn({
                let s = s.clone();
                move || s.lock().unwrap().unregister(1, 1)
            });
            let (tx, _newer_rx) = mpsc::channel(OUTBOX_CAPACITY);
            let newer = thread::spawn({
                let s = s.clone();
                move || s.lock().unwrap().register(1, 2, tx)
            });
            ending.join().unwrap();
            assert!(newer.join().unwrap(), "the slot was free");

            let s = s.lock().unwrap();
            assert_eq!(s.sessions.get(&1).map(|(conn, _)| *conn), Some(2));
            s.core.check_invariants();
        });
    }

    /// Two connections of one player racing to register: exactly one wins.
    #[test]
    fn one_session_per_player() {
        loom::model(|| {
            let s = Arc::new(Mutex::new(shared()));
            let racers: Vec<_> = (1..=2)
                .map(|conn| {
                    let s = s.clone();
                    thread::spawn(move || {
                        let (tx, _rx) = mpsc::channel(OUTBOX_CAPACITY);
                        s.lock().unwrap().register(7, conn, tx)
                    })
                })
                .collect();
            let wins = racers
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(|won| *won)
                .count();
            assert_eq!(wins, 1);
        });
    }
}

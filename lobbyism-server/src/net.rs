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
}

/// The limits `serve` uses; tests run with shorter ones.
const TIMEOUTS: Timeouts = Timeouts {
    hello: Duration::from_secs(10),
};

/// State shared by all connections, behind one mutex.
struct Shared<I> {
    /// The lobby and queue state machine.
    core: Core<I>,
    /// Live connection per player, tagged with a connection id so a stale
    /// connection never tears down a newer one for the same player.
    sessions: HashMap<PlayerId, (u64, mpsc::Sender<ServerMsg>)>,
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
                // Dropping the sender ends the writer task, which closes the socket.
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
    let hello = tokio::time::timeout_at(deadline, next_msg(&mut source))
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
    let registered = {
        let mut s = shared.lock().unwrap();
        let free = !s.sessions.contains_key(&player);
        if free {
            tx.try_send(ServerMsg::Welcome { player })
                .expect("fresh channel has room");
            s.sessions.insert(player, (conn, tx));
        }
        free
    };
    if !registered {
        let message = "player already connected".to_string();
        let _ = sink
            .send(encode(&ServerMsg::Error {
                message: message.clone(),
            }))
            .await;
        return Err(message);
    }

    let mut writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(encode(&msg)).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    let result = loop {
        let next = tokio::select! {
            next = next_msg(&mut source) => next,
            // Writer stopped: socket broken or session dropped as too slow.
            _ = &mut writer => break Err("writer closed".into()),
        };
        let out = match next {
            Ok(Some(msg)) => shared.lock().unwrap().core.handle(player, msg),
            Ok(None) => break Ok(()),
            // A malformed message gets an error reply; the connection stays usable.
            Err(ReadError::Bad(message)) => vec![(player, ServerMsg::Error { message })],
            Err(ReadError::Fatal(e)) => break Err(e),
        };
        shared.lock().unwrap().deliver(out);
    };

    {
        let mut s = shared.lock().unwrap();
        let superseded = s.sessions.get(&player).is_some_and(|(c, _)| *c != conn);
        if !superseded {
            s.sessions.remove(&player);
            let out = s.core.disconnect(player);
            s.deliver(out);
        }
    }
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

/// Next client message; `Ok(None)` on a clean close.
async fn next_msg<S>(source: &mut S) -> Result<Option<ClientMsg>, ReadError>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match source.next().await {
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
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    /// A WebSocket client connection as the tests use it.
    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    /// Test-only HMAC secret, exactly 32 bytes.
    const SECRET: &[u8] = b"net-test-secret-net-test-secret!";

    /// Short limits, so timeout tests finish quickly.
    const TEST_TIMEOUTS: Timeouts = Timeouts {
        hello: Duration::from_millis(300),
    };

    /// Starts a server with the default config on a random local port; returns its address.
    async fn start() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let core = Core::new(Config::default(), MatchboxIssuer::new("ws://mb").unwrap());
        let validator = HmacValidator::new(SECRET).unwrap();
        tokio::spawn(serve_with(listener, core, validator, TEST_TIMEOUTS));
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
}

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
/// A client must authenticate within this time.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Outgoing messages buffered per client before it counts as too slow and is dropped.
const OUTBOX_CAPACITY: usize = 256;

struct Shared<I> {
    core: Core<I>,
    /// Live connection per player, tagged with a connection id so a stale
    /// connection never tears down a newer one for the same player.
    sessions: HashMap<PlayerId, (u64, mpsc::Sender<ServerMsg>)>,
}

impl<I> Shared<I> {
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

/// Accept WebSocket clients on `listener` until it fails.
pub async fn serve<I, V>(listener: TcpListener, core: Core<I>, validator: V) -> std::io::Result<()>
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
        let (stream, addr) = listener.accept().await?;
        let (shared, validator) = (shared.clone(), validator.clone());
        tokio::spawn(async move {
            if let Err(e) = connection(stream, shared, &*validator).await {
                tracing::debug!(%addr, "connection ended: {e}");
            }
        });
    }
}

async fn connection<I: TicketIssuer>(
    stream: TcpStream,
    shared: Arc<Mutex<Shared<I>>>,
    validator: &dyn TokenValidator,
) -> Result<(), String> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let ws = tokio_tungstenite::accept_async_with_config(stream, Some(config))
        .await
        .map_err(|e| e.to_string())?;
    let (mut sink, mut source) = ws.split();

    // Authenticate before touching shared state.
    let hello = tokio::time::timeout(HELLO_TIMEOUT, next_msg(&mut source))
        .await
        .map_err(|_| "hello timeout".to_string())??;
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
            Err(e) if e.starts_with("bad message") => {
                vec![(player, ServerMsg::Error { message: e })]
            }
            Err(e) => break Err(e),
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

/// Next client message; `Ok(None)` on a clean close.
async fn next_msg<S>(source: &mut S) -> Result<Option<ClientMsg>, String>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match source.next().await {
            None | Some(Ok(Message::Close(_))) => return Ok(None),
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text)
                    .map(Some)
                    .map_err(|e| format!("bad message: {e}"));
            }
            Some(Ok(Message::Binary(_))) => return Err("bad message: expected text frame".into()),
            Some(Ok(_)) => continue, // ping/pong are handled by tungstenite
            Some(Err(e)) => return Err(e.to_string()),
        }
    }
}

fn encode(msg: &ServerMsg) -> Message {
    Message::text(serde_json::to_string(msg).expect("ServerMsg serializes"))
}

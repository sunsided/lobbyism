//! WebSocket client for `lobbyism-server`.

use crate::proto::{ClientMsg, PlayerId, ServerMsg};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use tokio_tungstenite_wasm::{Message, WebSocketStream};

/// Everything that can go wrong talking to the lobby server.
#[derive(Debug)]
pub enum Error {
    /// The WebSocket failed to connect, send or receive.
    WebSocket(tokio_tungstenite_wasm::Error),
    /// A message could not be encoded, or the server sent one that doesn't parse.
    Json(serde_json::Error),
    /// The server refused the `Hello` (bad or expired token, already connected).
    Rejected(String),
    /// The connection closed before the server answered `Hello`.
    Closed,
}

impl fmt::Display for Error {
    /// One line with the error kind and its cause.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::WebSocket(e) => write!(f, "websocket: {e}"),
            Error::Json(e) => write!(f, "protocol: {e}"),
            Error::Rejected(m) => write!(f, "rejected: {m}"),
            Error::Closed => f.write_str("connection closed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_tungstenite_wasm::Error> for Error {
    /// Wraps a WebSocket error.
    fn from(e: tokio_tungstenite_wasm::Error) -> Self {
        Error::WebSocket(e)
    }
}

impl From<serde_json::Error> for Error {
    /// Wraps a JSON encoding or decoding error.
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

/// An authenticated connection to the lobby server.
///
/// Keep calling [`LobbyClient::recv`] while the connection should stay open:
/// polling answers the server's WebSocket pings, and the server drops a client
/// that sends nothing, pongs included, for 60 seconds.
pub struct LobbyClient {
    /// The underlying WebSocket, carrying JSON text frames.
    ws: WebSocketStream,
    /// The id from the server's `Welcome`.
    player: PlayerId,
}

impl LobbyClient {
    /// Connect and authenticate with `token`; resolves once the server says `Welcome`.
    pub async fn connect(url: &str, token: &str) -> Result<Self, Error> {
        let ws = tokio_tungstenite_wasm::connect(url).await?;
        let mut client = Self { ws, player: 0 };
        client
            .send(ClientMsg::Hello {
                token: token.into(),
            })
            .await?;
        match client.recv().await? {
            Some(ServerMsg::Welcome { player }) => {
                client.player = player;
                Ok(client)
            }
            Some(ServerMsg::Error { message }) => Err(Error::Rejected(message)),
            Some(other) => Err(Error::Rejected(format!("unexpected {other:?}"))),
            None => Err(Error::Closed),
        }
    }

    /// The id the server assigned from the token.
    pub fn player(&self) -> PlayerId {
        self.player
    }

    /// Send one message to the server.
    pub async fn send(&mut self, msg: ClientMsg) -> Result<(), Error> {
        self.ws
            .send(Message::text(serde_json::to_string(&msg)?))
            .await?;
        Ok(())
    }

    /// Next server message; `Ok(None)` once the server closed the connection.
    pub async fn recv(&mut self) -> Result<Option<ServerMsg>, Error> {
        while let Some(msg) = self.ws.next().await {
            match msg? {
                Message::Text(text) => return Ok(Some(serde_json::from_str(&text)?)),
                Message::Close(_) => return Ok(None),
                Message::Binary(_) => continue,
            }
        }
        Ok(None)
    }
}

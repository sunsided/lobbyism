//! WebSocket client for `lobbyism-server`.

use crate::proto::{ClientMsg, PlayerId, ServerMsg};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use tokio_tungstenite_wasm::{Message, WebSocketStream};

#[derive(Debug)]
pub enum Error {
    WebSocket(tokio_tungstenite_wasm::Error),
    Json(serde_json::Error),
    /// The server refused the `Hello` (bad or expired token, already connected).
    Rejected(String),
    /// The connection closed before the server answered `Hello`.
    Closed,
}

impl fmt::Display for Error {
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
    fn from(e: tokio_tungstenite_wasm::Error) -> Self {
        Error::WebSocket(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

/// An authenticated connection to the lobby server.
pub struct LobbyClient {
    ws: WebSocketStream,
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

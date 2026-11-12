//! Lobbies and matchmaking for Rust games.
//!
//! This crate holds the wire protocol ([`proto`], re-exported at the root) and,
//! with the default `client` feature, an async client for `lobbyism-server`.
//! The client uses `tokio-tungstenite-wasm`, so the same code runs natively
//! (tokio) and in the browser (`wasm32-unknown-unknown`). The server depends on
//! this crate with `default-features = false` and gets the protocol only.
//!
//! ```no_run
//! # async fn demo(token: String) -> Result<(), lobbyism::Error> {
//! use lobbyism::{ClientMsg, LobbyClient, ServerMsg};
//! let mut client = LobbyClient::connect("ws://127.0.0.1:7878", &token).await?;
//! client.send(ClientMsg::Enqueue { mode: "duel".into(), region: "eu".into(), skill: 1200 }).await?;
//! while let Some(msg) = client.recv().await? {
//!     if let ServerMsg::Matched(m) = msg {
//!         // hand m.ticket to renet / matchbox
//!         break;
//!     }
//! }
//! # Ok(()) }
//! ```

pub mod proto;
pub use proto::*;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::{Error, LobbyClient};

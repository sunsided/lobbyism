//! Lobby and matchmaking server.
//!
//! - [`Core`]: in-memory lobby/queue state machine, transport-free.
//! - [`serve`]: WebSocket front end speaking the `lobbyism` JSON protocol.
//! - [`TokenValidator`] / [`HmacValidator`]: authentication at the trust boundary.
//! - [`TicketIssuer`]: turns a formed match into a transport ticket
//!   ([`NetcodeIssuer`] for renet / netcode.io, [`MatchboxIssuer`] for matchbox / GGRS).
//! - [`LanValidator`] and, behind the `lan*` features, the `lan` helpers: hosting
//!   a lobby from a player's game on the local network.

mod auth;
mod core;
#[cfg(feature = "lan")]
pub mod lan;
mod net;
mod ticket;

pub use auth::{HmacValidator, LanValidator, TokenValidator};
pub use core::{Config, Core, HostMigrationHook, Outbox};
pub use lobbyism::proto;
pub use net::serve;
#[cfg(feature = "netcode")]
pub use ticket::NetcodeIssuer;
pub use ticket::{MatchboxIssuer, TicketIssuer};

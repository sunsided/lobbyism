//! Lobby and matchmaking server.
//!
//! - [`Core`]: in-memory lobby/queue state machine, transport-free.
//! - [`serve`]: WebSocket front end speaking the `lobbyism-proto` JSON protocol.
//! - [`TokenValidator`] / [`HmacValidator`]: authentication at the trust boundary.
//! - [`TicketIssuer`]: turns a formed match into a transport ticket
//!   ([`NetcodeIssuer`] for renet / netcode.io, [`MatchboxIssuer`] for matchbox / GGRS).

mod auth;
mod core;
mod net;
mod ticket;

pub use auth::{HmacValidator, TokenValidator};
pub use core::{Config, Core, HostMigrationHook, Outbox};
pub use lobbyism_proto as proto;
pub use net::serve;
#[cfg(feature = "netcode")]
pub use ticket::NetcodeIssuer;
pub use ticket::{MatchboxIssuer, TicketIssuer};

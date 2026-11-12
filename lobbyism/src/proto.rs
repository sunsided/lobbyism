//! Wire protocol shared by the client and `lobbyism-server`.
//!
//! Messages travel as JSON text frames over a WebSocket. The first client
//! message on a connection must be [`ClientMsg::Hello`].

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;

/// Stable player identity, assigned by the server's token validator.
pub type PlayerId = u64;
/// Lobby identity, assigned by the server.
pub type LobbyId = u64;
/// Match identity, assigned by the server when a lobby starts or the queue pops.
pub type MatchId = u64;

/// Free-form lobby metadata (map name, game mode label, ...).
pub type Metadata = BTreeMap<String, String>;

/// Messages sent from client to server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Authenticate the connection. Must be the first message.
    Hello { token: String },
    /// Create a lobby and become its host.
    CreateLobby {
        max_players: u32,
        metadata: Metadata,
    },
    /// Join an existing lobby.
    JoinLobby { lobby: LobbyId },
    /// Leave the current lobby.
    LeaveLobby,
    /// Replace the lobby metadata. Host only.
    SetMetadata { metadata: Metadata },
    /// Set this player's ready flag in the current lobby.
    SetReady { ready: bool },
    /// Start the match. Host only; every member must be ready.
    StartMatch,
    /// Enter the matchmaking queue.
    Enqueue {
        mode: String,
        region: String,
        skill: u32,
    },
    /// Leave the matchmaking queue.
    CancelQueue,
}

/// Messages sent from server to client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Authentication succeeded.
    Welcome { player: PlayerId },
    /// Current lobby state; sent to all members after every change.
    Lobby(LobbyInfo),
    /// The host left and `new_host` took over. Followed by a [`ServerMsg::Lobby`] update.
    HostMigrated {
        lobby: LobbyId,
        old_host: PlayerId,
        new_host: PlayerId,
    },
    /// This player is no longer in a lobby (left, or the lobby closed).
    LeftLobby { lobby: LobbyId },
    /// This player is waiting in the matchmaking queue.
    Queued {
        mode: String,
        region: String,
        bucket: u32,
    },
    /// This player left the matchmaking queue.
    Dequeued,
    /// A match was formed. Hand `ticket` to the game transport.
    Matched(MatchInfo),
    /// A request failed.
    Error { message: String },
}

/// Snapshot of a lobby.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LobbyInfo {
    pub id: LobbyId,
    pub host: PlayerId,
    pub max_players: u32,
    pub metadata: Metadata,
    /// Members in join order, with their ready flag.
    pub members: Vec<Member>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub player: PlayerId,
    pub ready: bool,
}

/// A formed match, as seen by one of its players.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchInfo {
    pub id: MatchId,
    /// All players, in a stable order. A player's index is its handle
    /// (for example its GGRS player handle).
    pub players: Vec<PlayerId>,
    /// Index of the receiving player in `players`.
    pub handle: usize,
    pub ticket: Ticket,
}

/// What the game transport needs to connect. The lobby never carries gameplay packets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ticket {
    /// A netcode.io / renet `ConnectToken`, serialized with `ConnectToken::write`.
    Netcode {
        addresses: Vec<SocketAddr>,
        connect_token: Vec<u8>,
    },
    /// A matchbox signaling room URL. Peers use `MatchInfo::players` as the GGRS peer list.
    Matchbox { room_url: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_shape_is_tagged() {
        let json = serde_json::to_string(&ClientMsg::SetReady { ready: true }).unwrap();
        assert_eq!(json, r#"{"type":"set_ready","ready":true}"#);
    }
}

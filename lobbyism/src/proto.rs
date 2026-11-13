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
    Hello {
        /// Token issued by the game's auth backend; the server's token validator
        /// turns it into a [`PlayerId`].
        token: String,
    },
    /// Create a lobby and become its host.
    CreateLobby {
        /// Maximum number of members, the host included. Bounded by the server.
        max_players: u32,
        /// Initial lobby metadata. Bounded in size by the server.
        metadata: Metadata,
    },
    /// Join an existing lobby.
    JoinLobby {
        /// The lobby to join.
        lobby: LobbyId,
    },
    /// Leave the current lobby.
    LeaveLobby,
    /// Replace the lobby metadata. Host only.
    SetMetadata {
        /// The new metadata; replaces the old map entirely.
        metadata: Metadata,
    },
    /// Set this player's ready flag in the current lobby.
    SetReady {
        /// Whether the player is ready to start.
        ready: bool,
    },
    /// Start the match. Host only; every member must be ready.
    StartMatch,
    /// Enter the matchmaking queue.
    Enqueue {
        /// Game mode, one of the modes the server is configured with.
        mode: String,
        /// Region, one of the regions the server is configured with.
        region: String,
        /// Skill rating; the server groups players into buckets of equal width.
        /// Client-reported, so not trustworthy for ranked play.
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
    Welcome {
        /// The identity the server derived from the token.
        player: PlayerId,
    },
    /// Current lobby state; sent to all members after every change.
    Lobby(LobbyInfo),
    /// The host left and `new_host` took over. Followed by a [`ServerMsg::Lobby`] update.
    HostMigrated {
        /// The lobby whose host changed.
        lobby: LobbyId,
        /// The host that left.
        old_host: PlayerId,
        /// The member that took over (the next one in join order).
        new_host: PlayerId,
    },
    /// This player is no longer in a lobby (left, or the lobby closed).
    LeftLobby {
        /// The lobby the player left.
        lobby: LobbyId,
    },
    /// This player is waiting in the matchmaking queue.
    Queued {
        /// The queue's game mode.
        mode: String,
        /// The queue's region.
        region: String,
        /// The skill bucket the player was sorted into.
        bucket: u32,
    },
    /// This player left the matchmaking queue.
    Dequeued,
    /// A match was formed. Hand `ticket` to the game transport.
    Matched(MatchInfo),
    /// A request failed.
    Error {
        /// Human-readable reason, meant for logs rather than for players.
        message: String,
    },
}

/// Snapshot of a lobby.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LobbyInfo {
    /// The lobby's identity.
    pub id: LobbyId,
    /// The current host, who may change metadata and start the match.
    pub host: PlayerId,
    /// Maximum number of members, the host included.
    pub max_players: u32,
    /// Free-form metadata set by the host.
    pub metadata: Metadata,
    /// Members in join order, with their ready flag.
    pub members: Vec<Member>,
}

/// One member of a lobby.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// The member's identity.
    pub player: PlayerId,
    /// Whether the member marked themselves ready.
    pub ready: bool,
}

/// A formed match, as seen by one of its players.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchInfo {
    /// The match's identity; also written into netcode tickets' user data.
    pub id: MatchId,
    /// All players, in a stable order. A player's index is its handle
    /// (for example its GGRS player handle).
    pub players: Vec<PlayerId>,
    /// Index of the receiving player in `players`.
    pub handle: usize,
    /// What the receiving player needs to connect to the game.
    pub ticket: Ticket,
}

/// What the game transport needs to connect. The lobby never carries gameplay packets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ticket {
    /// A netcode.io / renet `ConnectToken`, serialized with `ConnectToken::write`.
    Netcode {
        /// Game server addresses the token is valid for.
        addresses: Vec<SocketAddr>,
        /// The serialized token; read it with `ConnectToken::read`.
        connect_token: Vec<u8>,
    },
    /// A matchbox signaling room URL. Peers use `MatchInfo::players` as the GGRS peer list.
    Matchbox {
        /// Signaling room URL, including the `?next=N` group size.
        room_url: String,
    },
}

/// Largest UDP broadcast announcement, in bytes of JSON; longer ones are not sent or read.
pub const LAN_MAX_ANNOUNCEMENT_BYTES: usize = 1024;

/// mDNS / DNS-SD service type that LAN hosts register under.
pub const LAN_MDNS_SERVICE: &str = "_lobbyism._tcp.local.";

/// What a LAN host announces so players on the same network can find it.
///
/// Sent as JSON in a UDP broadcast datagram, or as mDNS TXT properties
/// ([`LanAnnouncement::to_txt`]). Anyone on the network can send one, so
/// joiners treat it as untrusted: it only says where to connect.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LanAnnouncement {
    /// Identifies the game (and ideally its version); joiners ignore other games.
    pub game: String,
    /// Display name of the host or session, for a server browser.
    pub name: String,
    /// TCP port of the host's lobby server (`ws://<host address>:<port>`).
    pub lobby_port: u16,
    /// The lobby to send in `ClientMsg::JoinLobby`.
    pub lobby: LobbyId,
    /// Whether the `Hello` token must be the host's join code.
    pub code_required: bool,
}

impl LanAnnouncement {
    /// TXT properties for mDNS; the lobby port travels in the SRV record instead.
    pub fn to_txt(&self) -> Vec<(&'static str, String)> {
        vec![
            ("game", self.game.clone()),
            ("name", self.name.clone()),
            ("lobby", self.lobby.to_string()),
            ("code", u8::from(self.code_required).to_string()),
        ]
    }

    /// The inverse of [`LanAnnouncement::to_txt`]; `None` if a property is missing or malformed.
    pub fn from_txt<'a>(get: impl Fn(&str) -> Option<&'a str>, lobby_port: u16) -> Option<Self> {
        Some(Self {
            game: get("game")?.into(),
            name: get("name")?.into(),
            lobby_port,
            lobby: get("lobby")?.parse().ok()?,
            code_required: match get("code")? {
                "0" => false,
                "1" => true,
                _ => return None,
            },
        })
    }
}

/// Tests for the JSON wire format.
#[cfg(test)]
mod tests {
    use super::*;

    /// Messages are internally tagged by `type`, with snake_case names.
    #[test]
    fn json_shape_is_tagged() {
        let json = serde_json::to_string(&ClientMsg::SetReady { ready: true }).unwrap();
        assert_eq!(json, r#"{"type":"set_ready","ready":true}"#);
    }

    /// A LAN announcement survives the TXT encoding; broken properties are refused.
    #[test]
    fn lan_announcement_txt_round_trips() {
        let ann = LanAnnouncement {
            game: "demo/1".into(),
            name: "Alice's game".into(),
            lobby_port: 7878,
            lobby: 42,
            code_required: true,
        };
        let txt: BTreeMap<_, _> = ann.to_txt().into_iter().collect();
        let get = |k: &str| txt.get(k).map(String::as_str);
        assert_eq!(LanAnnouncement::from_txt(get, 7878), Some(ann));
        let mut broken = txt.clone();
        broken.insert("code", "yes".into());
        assert_eq!(
            LanAnnouncement::from_txt(|k| broken.get(k).map(String::as_str), 7878),
            None
        );
    }
}

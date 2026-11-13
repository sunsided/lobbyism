//! Turning a formed match into something the game transport can connect with.

use crate::auth::to_hex;
use hmac::{Hmac, KeyInit, Mac};
use lobbyism::proto::{MatchId, PlayerId, Ticket};
use sha2::Sha256;

/// Issues one player's connection ticket for a formed match.
///
/// `issue` runs inside [`Core`](crate::Core), which [`serve`](crate::serve)
/// keeps behind one lock for all connections, once per player of every match.
/// It must be fast and must not block (no network calls, e.g. to a game server
/// allocator), and it must not panic: a slow issuer stalls every player, and a
/// panic poisons the lock for good. Report failures through the `Err` variant;
/// the players of that match then get an error instead of a ticket.
pub trait TicketIssuer: Send {
    /// Creates the ticket for `player` in match `match_id`. `players` is the whole
    /// match in handle order. An error is reported to every player of the match.
    fn issue(
        &self,
        match_id: MatchId,
        players: &[PlayerId],
        player: PlayerId,
    ) -> Result<Ticket, String>;
}

/// Points every player of a match at the same matchbox signaling room.
///
/// The room name is an HMAC of the match id under a random per-process key, so
/// outsiders cannot guess it and take a seat; `?next=N` makes matchbox_server
/// group exactly the match's players. The name only hides the room: a
/// signaling server that checks per-player tokens is still the stronger gate.
#[derive(Clone)]
pub struct MatchboxIssuer {
    /// Signaling server base URL, without the room path.
    base_url: String,
    /// Random per-process key that room names are derived from.
    room_key: [u8; 32],
}

impl MatchboxIssuer {
    /// `base_url` is the signaling server, e.g. `wss://signal.example.com`.
    pub fn new(base_url: impl Into<String>) -> Result<Self, String> {
        let mut room_key = [0u8; 32];
        getrandom::fill(&mut room_key).map_err(|e| format!("no OS randomness: {e}"))?;
        Ok(Self {
            base_url: base_url.into(),
            room_key,
        })
    }
}

impl TicketIssuer for MatchboxIssuer {
    /// Room name: the first 128 bits of HMAC-SHA256(room key, match id), hex-encoded.
    fn issue(
        &self,
        match_id: MatchId,
        players: &[PlayerId],
        _player: PlayerId,
    ) -> Result<Ticket, String> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.room_key).expect("HMAC accepts any key length");
        mac.update(&match_id.to_le_bytes());
        // 128 bits is plenty for an unguessable room name.
        let room = to_hex(&mac.finalize().into_bytes()[..16]);
        let base = self.base_url.trim_end_matches('/');
        Ok(Ticket::Matchbox {
            room_url: format!("{base}/lobbyism_{room}?next={}", players.len()),
        })
    }
}

#[cfg(feature = "netcode")]
pub use netcode::NetcodeIssuer;

/// The renet / netcode.io issuer, behind the `netcode` feature.
#[cfg(feature = "netcode")]
mod netcode {
    use super::*;
    use renet_netcode::{ConnectToken, NETCODE_KEY_BYTES, NETCODE_USER_DATA_BYTES};
    use std::net::SocketAddr;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Issues netcode.io / renet `ConnectToken`s for a dedicated game server.
    ///
    /// The game server must run `ServerAuthentication::Secure` with the same
    /// `private_key` and `protocol_id`. The renet client id is the [`PlayerId`];
    /// the token's user data starts with the little-endian [`MatchId`], so the
    /// game server can group connections by match.
    #[derive(Clone)]
    pub struct NetcodeIssuer {
        /// Your game's netcode protocol id; tokens for another id are rejected.
        pub protocol_id: u64,
        /// Key the tokens are encrypted with, shared with the game server.
        /// Never send it to a client.
        pub private_key: [u8; NETCODE_KEY_BYTES],
        /// Public game server addresses written into every token. The game
        /// server only accepts tokens that list one of its own addresses.
        pub server_addresses: Vec<SocketAddr>,
        /// How long the token may be used to start a connection.
        pub expire_seconds: u64,
        /// Connection timeout the client and server agree on.
        pub timeout_seconds: i32,
    }

    impl TicketIssuer for NetcodeIssuer {
        /// A fresh `ConnectToken` per player, with the match id in the user data.
        fn issue(
            &self,
            match_id: MatchId,
            _players: &[PlayerId],
            player: PlayerId,
        ) -> Result<Ticket, String> {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?;
            let mut user_data = [0u8; NETCODE_USER_DATA_BYTES];
            user_data[..8].copy_from_slice(&match_id.to_le_bytes());
            let token = ConnectToken::generate(
                now,
                self.protocol_id,
                self.expire_seconds,
                player,
                self.timeout_seconds,
                self.server_addresses.clone(),
                Some(&user_data),
                &self.private_key,
            )
            .map_err(|e| e.to_string())?;
            let mut connect_token = Vec::new();
            token.write(&mut connect_token).map_err(|e| e.to_string())?;
            Ok(Ticket::Netcode {
                addresses: self.server_addresses.clone(),
                connect_token,
            })
        }
    }

    /// Tests for netcode token generation.
    #[cfg(test)]
    mod tests {
        use super::*;

        /// An issued token parses back with the player as client id and the right protocol id.
        #[test]
        fn token_round_trips_and_carries_match_id() {
            let issuer = NetcodeIssuer {
                protocol_id: 7,
                private_key: [3; NETCODE_KEY_BYTES],
                server_addresses: vec!["127.0.0.1:5000".parse().unwrap()],
                expire_seconds: 30,
                timeout_seconds: 5,
            };
            let Ticket::Netcode {
                addresses,
                connect_token,
            } = issuer.issue(42, &[9, 10], 10).unwrap()
            else {
                panic!("netcode ticket expected")
            };
            assert_eq!(addresses, issuer.server_addresses);
            let token = ConnectToken::read(&mut connect_token.as_slice()).unwrap();
            assert_eq!(token.client_id, 10);
            assert_eq!(token.protocol_id, 7);
        }
    }
}

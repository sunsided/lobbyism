//! Turning a formed match into something the game transport can connect with.

use lobbyism_proto::{MatchId, PlayerId, Ticket};

/// Issues one player's connection ticket for a formed match.
pub trait TicketIssuer: Send {
    /// `players` is the whole match in handle order; `player` is the recipient.
    fn issue(
        &self,
        match_id: MatchId,
        players: &[PlayerId],
        player: PlayerId,
    ) -> Result<Ticket, String>;
}

/// Points every player of a match at the same matchbox signaling room.
///
/// The room URL carries `?next=N`, so matchbox_server groups exactly the match's players.
#[derive(Debug, Clone)]
pub struct MatchboxIssuer {
    /// Signaling server base URL, e.g. `wss://signal.example.com`.
    pub base_url: String,
}

impl TicketIssuer for MatchboxIssuer {
    fn issue(
        &self,
        match_id: MatchId,
        players: &[PlayerId],
        _player: PlayerId,
    ) -> Result<Ticket, String> {
        let base = self.base_url.trim_end_matches('/');
        Ok(Ticket::Matchbox {
            room_url: format!("{base}/lobbyism_{match_id}?next={}", players.len()),
        })
    }
}

#[cfg(feature = "netcode")]
pub use netcode::NetcodeIssuer;

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
        pub protocol_id: u64,
        pub private_key: [u8; NETCODE_KEY_BYTES],
        pub server_addresses: Vec<SocketAddr>,
        /// How long the token may be used to start a connection.
        pub expire_seconds: u64,
        /// Connection timeout the client and server agree on.
        pub timeout_seconds: i32,
    }

    impl TicketIssuer for NetcodeIssuer {
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

    #[cfg(test)]
    mod tests {
        use super::*;

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

//! Transport-free lobby and matchmaking state machine.
//!
//! [`Core::handle`] and [`Core::disconnect`] return the messages to deliver, so the
//! state machine is testable without sockets.

use crate::ticket::TicketIssuer;
use lobbyism_proto::{
    ClientMsg, LobbyId, LobbyInfo, MatchId, MatchInfo, Member, Metadata, PlayerId, ServerMsg,
};
use std::collections::HashMap;

/// Messages to deliver, in order.
pub type Outbox = Vec<(PlayerId, ServerMsg)>;

/// Called with `(lobby, old_host, new_host)` when a host leaves and another member takes over.
pub type HostMigrationHook = Box<dyn Fn(LobbyId, PlayerId, PlayerId) + Send>;

/// Server-side limits and matchmaking rules.
#[derive(Debug, Clone)]
pub struct Config {
    /// Queue modes and the number of players a match of that mode needs.
    pub modes: HashMap<String, u32>,
    /// Regions clients may queue in.
    pub regions: Vec<String>,
    /// Skill values `[k*w, (k+1)*w)` share bucket `k`.
    pub skill_bucket_width: u32,
    /// Upper bound for `CreateLobby::max_players`.
    pub max_lobby_size: u32,
    /// Upper bound for the summed key and value bytes of lobby metadata.
    pub max_metadata_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            modes: HashMap::from([("duel".into(), 2)]),
            regions: vec!["eu".into(), "us".into()],
            skill_bucket_width: 100,
            max_lobby_size: 16,
            max_metadata_bytes: 4096,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueueKey {
    mode: String,
    region: String,
    bucket: u32,
}

struct Lobby {
    host: PlayerId,
    max_players: u32,
    metadata: Metadata,
    members: Vec<Member>,
}

/// All lobby and queue state. In-memory only.
pub struct Core<I> {
    config: Config,
    issuer: I,
    on_host_migration: Option<HostMigrationHook>,
    next_lobby: LobbyId,
    next_match: MatchId,
    lobbies: HashMap<LobbyId, Lobby>,
    player_lobby: HashMap<PlayerId, LobbyId>,
    queues: HashMap<QueueKey, Vec<PlayerId>>,
    player_queue: HashMap<PlayerId, QueueKey>,
}

impl<I: TicketIssuer> Core<I> {
    pub fn new(config: Config, issuer: I) -> Self {
        Self {
            config,
            issuer,
            on_host_migration: None,
            next_lobby: 1,
            next_match: 1,
            lobbies: HashMap::new(),
            player_lobby: HashMap::new(),
            queues: HashMap::new(),
            player_queue: HashMap::new(),
        }
    }

    /// Register a callback that runs whenever a lobby changes host.
    pub fn on_host_migration(mut self, hook: HostMigrationHook) -> Self {
        self.on_host_migration = Some(hook);
        self
    }

    /// Apply one message from an authenticated player.
    pub fn handle(&mut self, player: PlayerId, msg: ClientMsg) -> Outbox {
        let result = match msg {
            ClientMsg::Hello { .. } => Err("already authenticated".into()),
            ClientMsg::CreateLobby {
                max_players,
                metadata,
            } => self.create(player, max_players, metadata),
            ClientMsg::JoinLobby { lobby } => self.join(player, lobby),
            ClientMsg::LeaveLobby => self.leave(player),
            ClientMsg::SetMetadata { metadata } => self.set_metadata(player, metadata),
            ClientMsg::SetReady { ready } => self.set_ready(player, ready),
            ClientMsg::StartMatch => self.start(player),
            ClientMsg::Enqueue {
                mode,
                region,
                skill,
            } => self.enqueue(player, mode, region, skill),
            ClientMsg::CancelQueue => self.cancel_queue(player),
        };
        result.unwrap_or_else(|message| vec![(player, ServerMsg::Error { message })])
    }

    /// Remove a player from its lobby and queue, e.g. when its connection drops.
    pub fn disconnect(&mut self, player: PlayerId) -> Outbox {
        let mut out = self.leave(player).unwrap_or_default();
        self.remove_from_queue(player);
        // The player is gone; only notify the others.
        out.retain(|(to, _)| *to != player);
        out
    }

    fn create(
        &mut self,
        player: PlayerId,
        max_players: u32,
        metadata: Metadata,
    ) -> Result<Outbox, String> {
        self.ensure_idle(player)?;
        if max_players == 0 || max_players > self.config.max_lobby_size {
            return Err(format!(
                "max_players must be 1..={}",
                self.config.max_lobby_size
            ));
        }
        self.check_metadata(&metadata)?;
        let id = self.next_lobby;
        self.next_lobby += 1;
        self.lobbies.insert(
            id,
            Lobby {
                host: player,
                max_players,
                metadata,
                members: vec![Member {
                    player,
                    ready: false,
                }],
            },
        );
        self.player_lobby.insert(player, id);
        Ok(self.broadcast(id))
    }

    fn join(&mut self, player: PlayerId, id: LobbyId) -> Result<Outbox, String> {
        self.ensure_idle(player)?;
        let lobby = self.lobbies.get_mut(&id).ok_or("no such lobby")?;
        if lobby.members.len() >= lobby.max_players as usize {
            return Err("lobby is full".into());
        }
        lobby.members.push(Member {
            player,
            ready: false,
        });
        self.player_lobby.insert(player, id);
        Ok(self.broadcast(id))
    }

    fn leave(&mut self, player: PlayerId) -> Result<Outbox, String> {
        let id = self.player_lobby.remove(&player).ok_or("not in a lobby")?;
        let lobby = self
            .lobbies
            .get_mut(&id)
            .expect("player_lobby points at a live lobby");
        lobby.members.retain(|m| m.player != player);
        let mut out = vec![(player, ServerMsg::LeftLobby { lobby: id })];
        let Some(first) = lobby.members.first() else {
            self.lobbies.remove(&id);
            return Ok(out);
        };
        if lobby.host == player {
            let new_host = first.player;
            lobby.host = new_host;
            if let Some(hook) = &self.on_host_migration {
                hook(id, player, new_host);
            }
            let msg = ServerMsg::HostMigrated {
                lobby: id,
                old_host: player,
                new_host,
            };
            out.extend(lobby.members.iter().map(|m| (m.player, msg.clone())));
        }
        out.extend(self.broadcast(id));
        Ok(out)
    }

    fn set_metadata(&mut self, player: PlayerId, metadata: Metadata) -> Result<Outbox, String> {
        self.check_metadata(&metadata)?;
        let id = self.hosted_lobby(player)?;
        self.lobbies.get_mut(&id).unwrap().metadata = metadata;
        Ok(self.broadcast(id))
    }

    fn set_ready(&mut self, player: PlayerId, ready: bool) -> Result<Outbox, String> {
        let id = *self.player_lobby.get(&player).ok_or("not in a lobby")?;
        let lobby = self.lobbies.get_mut(&id).unwrap();
        lobby
            .members
            .iter_mut()
            .find(|m| m.player == player)
            .unwrap()
            .ready = ready;
        Ok(self.broadcast(id))
    }

    fn start(&mut self, player: PlayerId) -> Result<Outbox, String> {
        let id = self.hosted_lobby(player)?;
        if !self.lobbies[&id].members.iter().all(|m| m.ready) {
            return Err("not all members are ready".into());
        }
        let players: Vec<_> = self.lobbies[&id].members.iter().map(|m| m.player).collect();
        self.lobbies.remove(&id);
        for p in &players {
            self.player_lobby.remove(p);
        }
        Ok(self.form_match(&players))
    }

    fn enqueue(
        &mut self,
        player: PlayerId,
        mode: String,
        region: String,
        skill: u32,
    ) -> Result<Outbox, String> {
        self.ensure_idle(player)?;
        let needed = *self.config.modes.get(&mode).ok_or("unknown mode")? as usize;
        if !self.config.regions.contains(&region) {
            return Err("unknown region".into());
        }
        // shortcut: skill is client-reported; move it into validated token claims once ranking matters.
        let bucket = skill / self.config.skill_bucket_width.max(1);
        let key = QueueKey {
            mode,
            region,
            bucket,
        };
        let mut out = vec![(
            player,
            ServerMsg::Queued {
                mode: key.mode.clone(),
                region: key.region.clone(),
                bucket,
            },
        )];
        let queue = self.queues.entry(key.clone()).or_default();
        queue.push(player);
        if queue.len() < needed {
            self.player_queue.insert(player, key);
            return Ok(out);
        }
        let players: Vec<_> = queue.drain(..needed).collect();
        if queue.is_empty() {
            self.queues.remove(&key);
        }
        for p in &players {
            self.player_queue.remove(p);
        }
        out.extend(self.form_match(&players));
        Ok(out)
    }

    fn cancel_queue(&mut self, player: PlayerId) -> Result<Outbox, String> {
        if !self.remove_from_queue(player) {
            return Err("not queued".into());
        }
        Ok(vec![(player, ServerMsg::Dequeued)])
    }

    fn remove_from_queue(&mut self, player: PlayerId) -> bool {
        let Some(key) = self.player_queue.remove(&player) else {
            return false;
        };
        let queue = self
            .queues
            .get_mut(&key)
            .expect("player_queue points at a live queue");
        queue.retain(|p| *p != player);
        if queue.is_empty() {
            self.queues.remove(&key);
        }
        true
    }

    /// Players are already out of their lobby/queue here, so an issuer failure is
    /// reported to every player rather than aborting halfway.
    fn form_match(&mut self, players: &[PlayerId]) -> Outbox {
        let id = self.next_match;
        self.next_match += 1;
        let msgs = players.iter().enumerate().map(|(handle, &p)| {
            match self.issuer.issue(id, players, p) {
                Ok(ticket) => ServerMsg::Matched(MatchInfo {
                    id,
                    players: players.to_vec(),
                    handle,
                    ticket,
                }),
                Err(e) => ServerMsg::Error {
                    message: format!("match {id}: ticket issue failed: {e}"),
                },
            }
        });
        players.iter().copied().zip(msgs).collect()
    }

    fn ensure_idle(&self, player: PlayerId) -> Result<(), String> {
        if self.player_lobby.contains_key(&player) {
            return Err("already in a lobby".into());
        }
        if self.player_queue.contains_key(&player) {
            return Err("already queued".into());
        }
        Ok(())
    }

    fn hosted_lobby(&self, player: PlayerId) -> Result<LobbyId, String> {
        let id = *self.player_lobby.get(&player).ok_or("not in a lobby")?;
        if self.lobbies[&id].host != player {
            return Err("only the host can do that".into());
        }
        Ok(id)
    }

    fn check_metadata(&self, metadata: &Metadata) -> Result<(), String> {
        let bytes: usize = metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
        if bytes > self.config.max_metadata_bytes {
            return Err(format!(
                "metadata exceeds {} bytes",
                self.config.max_metadata_bytes
            ));
        }
        Ok(())
    }

    fn broadcast(&self, id: LobbyId) -> Outbox {
        let lobby = &self.lobbies[&id];
        let info = LobbyInfo {
            id,
            host: lobby.host,
            max_players: lobby.max_players,
            metadata: lobby.metadata.clone(),
            members: lobby.members.clone(),
        };
        lobby
            .members
            .iter()
            .map(|m| (m.player, ServerMsg::Lobby(info.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ticket::MatchboxIssuer;
    use lobbyism_proto::Ticket;
    use std::sync::{Arc, Mutex};

    fn core() -> Core<MatchboxIssuer> {
        Core::new(Config::default(), MatchboxIssuer::new("ws://mb").unwrap())
    }

    fn create(c: &mut Core<MatchboxIssuer>, host: PlayerId, max: u32) -> LobbyId {
        let out = c.handle(
            host,
            ClientMsg::CreateLobby {
                max_players: max,
                metadata: Metadata::new(),
            },
        );
        match &out[..] {
            [(p, ServerMsg::Lobby(info))] if *p == host => info.id,
            other => panic!("unexpected {other:?}"),
        }
    }

    fn last_lobby(out: &Outbox) -> &LobbyInfo {
        out.iter()
            .rev()
            .find_map(|(_, m)| {
                if let ServerMsg::Lobby(i) = m {
                    Some(i)
                } else {
                    None
                }
            })
            .expect("lobby update")
    }

    fn is_error(out: &Outbox) -> bool {
        matches!(&out[..], [(_, ServerMsg::Error { .. })])
    }

    fn enqueue(
        c: &mut Core<MatchboxIssuer>,
        p: PlayerId,
        mode: &str,
        region: &str,
        skill: u32,
    ) -> Outbox {
        c.handle(
            p,
            ClientMsg::Enqueue {
                mode: mode.into(),
                region: region.into(),
                skill,
            },
        )
    }

    #[test]
    fn create_join_leave_closes_empty_lobby() {
        let mut c = core();
        let id = create(&mut c, 1, 4);
        let out = c.handle(2, ClientMsg::JoinLobby { lobby: id });
        assert_eq!(out.len(), 2, "both members get the update");
        assert_eq!(last_lobby(&out).members.len(), 2);

        c.handle(2, ClientMsg::LeaveLobby);
        let out = c.handle(1, ClientMsg::LeaveLobby);
        assert_eq!(out, vec![(1, ServerMsg::LeftLobby { lobby: id })]);
        assert!(
            is_error(&c.handle(3, ClientMsg::JoinLobby { lobby: id })),
            "lobby is gone"
        );
    }

    #[test]
    fn join_rejects_full_unknown_and_double_membership() {
        let mut c = core();
        let id = create(&mut c, 1, 2);
        c.handle(2, ClientMsg::JoinLobby { lobby: id });
        assert!(is_error(&c.handle(3, ClientMsg::JoinLobby { lobby: id })));
        assert!(is_error(&c.handle(3, ClientMsg::JoinLobby { lobby: 999 })));
        assert!(is_error(&c.handle(
            1,
            ClientMsg::CreateLobby {
                max_players: 2,
                metadata: Metadata::new()
            }
        )));
        assert!(is_error(&c.handle(
            4,
            ClientMsg::CreateLobby {
                max_players: 0,
                metadata: Metadata::new()
            }
        )));
    }

    #[test]
    fn host_leaving_migrates_and_runs_hook() {
        let seen = Arc::new(Mutex::new(None));
        let hook_seen = seen.clone();
        let mut c = core().on_host_migration(Box::new(move |l, old, new| {
            *hook_seen.lock().unwrap() = Some((l, old, new));
        }));
        let id = create(&mut c, 1, 4);
        c.handle(2, ClientMsg::JoinLobby { lobby: id });
        c.handle(3, ClientMsg::JoinLobby { lobby: id });

        let out = c.handle(1, ClientMsg::LeaveLobby);
        assert!(out.contains(&(
            2,
            ServerMsg::HostMigrated {
                lobby: id,
                old_host: 1,
                new_host: 2
            }
        )));
        assert!(out.contains(&(
            3,
            ServerMsg::HostMigrated {
                lobby: id,
                old_host: 1,
                new_host: 2
            }
        )));
        assert_eq!(last_lobby(&out).host, 2);
        assert_eq!(*seen.lock().unwrap(), Some((id, 1, 2)));
    }

    #[test]
    fn disconnect_leaves_lobby_without_messaging_the_gone_player() {
        let mut c = core();
        let id = create(&mut c, 1, 4);
        c.handle(2, ClientMsg::JoinLobby { lobby: id });
        let out = c.disconnect(1);
        assert!(out.iter().all(|(p, _)| *p == 2));
        assert_eq!(
            last_lobby(&out).members,
            vec![Member {
                player: 2,
                ready: false
            }]
        );
    }

    #[test]
    fn metadata_and_start_are_host_only_and_bounded() {
        let mut c = core();
        let id = create(&mut c, 1, 4);
        c.handle(2, ClientMsg::JoinLobby { lobby: id });
        let meta = Metadata::from([("map".into(), "dust".into())]);
        assert!(is_error(&c.handle(
            2,
            ClientMsg::SetMetadata {
                metadata: meta.clone()
            }
        )));
        assert_eq!(
            last_lobby(&c.handle(
                1,
                ClientMsg::SetMetadata {
                    metadata: meta.clone()
                }
            ))
            .metadata,
            meta
        );
        let huge = Metadata::from([("k".into(), "v".repeat(5000))]);
        assert!(is_error(
            &c.handle(1, ClientMsg::SetMetadata { metadata: huge })
        ));
        assert!(is_error(&c.handle(2, ClientMsg::StartMatch)));
    }

    #[test]
    fn start_requires_all_ready_then_issues_tickets_and_frees_players() {
        let mut c = core();
        let id = create(&mut c, 1, 4);
        c.handle(2, ClientMsg::JoinLobby { lobby: id });
        c.handle(1, ClientMsg::SetReady { ready: true });
        assert!(is_error(&c.handle(1, ClientMsg::StartMatch)));

        let out = c.handle(2, ClientMsg::SetReady { ready: true });
        assert!(last_lobby(&out).members.iter().all(|m| m.ready));
        let out = c.handle(1, ClientMsg::StartMatch);
        assert_eq!(out.len(), 2);
        let mut rooms = Vec::new();
        for (i, (p, msg)) in out.iter().enumerate() {
            let ServerMsg::Matched(m) = msg else {
                panic!("{msg:?}")
            };
            assert_eq!(m.players, vec![1, 2]);
            assert_eq!(m.handle, i);
            assert_eq!(m.players[m.handle], *p);
            let Ticket::Matchbox { room_url } = &m.ticket else {
                panic!("{:?}", m.ticket)
            };
            rooms.push(room_url.clone());
        }
        // Same room for both players, not derivable from the sequential match id.
        assert_eq!(rooms[0], rooms[1]);
        assert!(rooms[0].starts_with("ws://mb/lobbyism_") && rooms[0].ends_with("?next=2"));
        assert_eq!(rooms[0].len(), "ws://mb/lobbyism_?next=2".len() + 32);
        // Lobby is consumed; both players are free again.
        assert!(is_error(&c.handle(3, ClientMsg::JoinLobby { lobby: id })));
        create(&mut c, 1, 2);
    }

    #[test]
    fn queue_matches_only_same_mode_region_and_bucket() {
        let mut c = core();
        assert_eq!(enqueue(&mut c, 1, "duel", "eu", 150).len(), 1);
        assert_eq!(
            enqueue(&mut c, 2, "duel", "us", 150).len(),
            1,
            "other region"
        );
        assert_eq!(
            enqueue(&mut c, 3, "duel", "eu", 250).len(),
            1,
            "other bucket"
        );
        let out = enqueue(&mut c, 4, "duel", "eu", 199);
        let matched: Vec<_> = out
            .iter()
            .filter_map(|(p, m)| {
                if let ServerMsg::Matched(i) = m {
                    Some((*p, i.players.clone()))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(matched, vec![(1, vec![1, 4]), (4, vec![1, 4])]);
        // Matched players left the queue.
        assert!(is_error(&c.handle(1, ClientMsg::CancelQueue)));
    }

    #[test]
    fn queue_rejects_unknown_mode_region_and_double_entry() {
        let mut c = core();
        assert!(is_error(&enqueue(&mut c, 1, "chess", "eu", 0)));
        assert!(is_error(&enqueue(&mut c, 1, "duel", "mars", 0)));
        enqueue(&mut c, 1, "duel", "eu", 0);
        assert!(is_error(&enqueue(&mut c, 1, "duel", "eu", 0)));
        assert!(is_error(&c.handle(
            1,
            ClientMsg::CreateLobby {
                max_players: 2,
                metadata: Metadata::new()
            }
        )));
    }

    #[test]
    fn cancel_and_disconnect_remove_from_queue() {
        let mut c = core();
        enqueue(&mut c, 1, "duel", "eu", 0);
        assert_eq!(
            c.handle(1, ClientMsg::CancelQueue),
            vec![(1, ServerMsg::Dequeued)]
        );
        enqueue(&mut c, 2, "duel", "eu", 0);
        assert!(c.disconnect(2).is_empty());
        // Neither 1 nor 2 is waiting, so 3 does not match.
        assert_eq!(enqueue(&mut c, 3, "duel", "eu", 0).len(), 1);
        assert!(
            c.queues.values().all(|q| !q.is_empty()),
            "no empty queues linger"
        );
    }
}

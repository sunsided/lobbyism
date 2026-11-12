//! Random message sequences from a handful of players against the lobby and
//! queue state machine; internal indexes must stay consistent after every step.
#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use lobbyism_proto::{ClientMsg, Metadata, ServerMsg};
use lobbyism_server::{Config, Core, MatchboxIssuer};
use std::collections::HashMap;

#[derive(Arbitrary, Debug)]
enum Op {
    Create { max_players: u8, meta_len: u16 },
    Join { lobby: u8 },
    Leave,
    SetMetadata { meta_len: u16 },
    SetReady(bool),
    Start,
    Enqueue { mode: u8, region: u8, skill: u16 },
    Cancel,
    Hello,
    Disconnect,
}

fuzz_target!(|steps: Vec<(u8, Op)>| {
    let config = Config {
        modes: HashMap::from([("duel".into(), 2), ("trio".into(), 3)]),
        max_lobby_size: 4,
        ..Config::default()
    };
    let mut core = Core::new(config, MatchboxIssuer::new("ws://mb").unwrap());
    let meta = |len: u16| Metadata::from([("k".to_string(), "v".repeat(len as usize % 5000))]);
    let pick = |i: u8, xs: &[&str]| xs[i as usize % xs.len()].to_string();

    for (player, op) in steps {
        let player = u64::from(player % 8);
        let msg = match op {
            Op::Create {
                max_players,
                meta_len,
            } => ClientMsg::CreateLobby {
                max_players: u32::from(max_players % 6),
                metadata: meta(meta_len),
            },
            Op::Join { lobby } => ClientMsg::JoinLobby {
                lobby: u64::from(lobby % 8),
            },
            Op::Leave => ClientMsg::LeaveLobby,
            Op::SetMetadata { meta_len } => ClientMsg::SetMetadata {
                metadata: meta(meta_len),
            },
            Op::SetReady(ready) => ClientMsg::SetReady { ready },
            Op::Start => ClientMsg::StartMatch,
            Op::Enqueue {
                mode,
                region,
                skill,
            } => ClientMsg::Enqueue {
                mode: pick(mode, &["duel", "trio", "chess"]),
                region: pick(region, &["eu", "us", "mars"]),
                skill: u32::from(skill),
            },
            Op::Cancel => ClientMsg::CancelQueue,
            Op::Hello => ClientMsg::Hello {
                token: String::new(),
            },
            Op::Disconnect => {
                core.disconnect(player);
                core.check_invariants();
                continue;
            }
        };
        for (to, out) in core.handle(player, msg) {
            if let ServerMsg::Matched(m) = out {
                assert_eq!(m.players[m.handle], to);
            }
        }
        core.check_invariants();
    }
});

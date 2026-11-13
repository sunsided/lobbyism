//! mDNS round trip: a host announces, a browser on the same machine finds it.
//! Ignored by default, since CI runners often block multicast; run with
//! `cargo test -p lobbyism --features lan-mdns --test lan_mdns -- --ignored`.

use lobbyism::lan::discover_mdns;
use lobbyism::proto::LanAnnouncement;
use lobbyism_server::lan::announce_mdns;
use std::time::Duration;

/// A registered announcement is browsed back with all its fields; other games are filtered out.
#[test]
#[ignore = "needs multicast on a network interface"]
fn mdns_announcement_is_discovered() {
    let ann = LanAnnouncement {
        game: "lobbyism-mdns-test".into(),
        name: "test host".into(),
        lobby_port: 47_801,
        lobby: 0x5eed,
        code_required: true,
    };
    let _announcer = announce_mdns(&ann).unwrap();
    let hosts = discover_mdns(&ann.game, Duration::from_secs(3)).unwrap();
    assert!(!hosts.is_empty(), "nothing discovered");
    assert!(hosts.iter().all(|h| h.announcement == ann), "{hosts:?}");
    assert!(
        discover_mdns("another game", Duration::from_secs(2))
            .unwrap()
            .iter()
            .all(|h| h.announcement != ann),
        "another game's search returned ours"
    );
}

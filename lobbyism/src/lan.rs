//! Finding lobbies hosted on the local network (native only; browsers can't
//! send UDP or mDNS).
//!
//! Hosts announce a [`LanAnnouncement`] with `lobbyism-server`'s `lan` module;
//! [`discover_broadcast`] and [`discover_mdns`] collect them. Announcements
//! come from anyone on the network and only say where to connect.

#[cfg(target_arch = "wasm32")]
compile_error!("the lan-broadcast and lan-mdns features need UDP sockets, which wasm32 lacks");

use crate::proto::LanAnnouncement;
use std::net::{IpAddr, SocketAddr};

/// A lobby found on the local network.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LanHost {
    /// The address the announcement came from.
    pub addr: IpAddr,
    /// What the host announced.
    pub announcement: LanAnnouncement,
}

impl LanHost {
    /// The lobby server URL for `LobbyClient::connect`, e.g. `ws://192.168.1.20:7878`.
    pub fn url(&self) -> String {
        format!(
            "ws://{}",
            SocketAddr::new(self.addr, self.announcement.lobby_port)
        )
    }
}

#[cfg(feature = "lan-broadcast")]
pub use broadcast::discover_broadcast;

/// Listening for UDP broadcast announcements, behind the `lan-broadcast` feature.
#[cfg(feature = "lan-broadcast")]
mod broadcast {
    use super::LanHost;
    use crate::proto::{LAN_MAX_ANNOUNCEMENT_BYTES, LanAnnouncement};
    use socket2::{Domain, Protocol, Socket, Type};
    use std::io;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use tokio::net::UdpSocket;

    /// Listens on UDP `port` for `wait` and returns the distinct hosts of `game` it heard.
    ///
    /// Hosts announce once a second, so a `wait` of 2 to 3 seconds finds
    /// everyone who is reachable. The port is shared with other listeners on
    /// the same machine, so several game instances can search at once.
    pub async fn discover_broadcast(
        port: u16,
        game: &str,
        wait: Duration,
    ) -> io::Result<Vec<LanHost>> {
        let socket = listen(port)?;
        let deadline = tokio::time::Instant::now() + wait;
        // One byte more than allowed, so an oversized datagram shows up as too long.
        let mut buf = [0u8; LAN_MAX_ANNOUNCEMENT_BYTES + 1];
        let mut hosts = Vec::new();
        while let Ok(received) = tokio::time::timeout_at(deadline, socket.recv_from(&mut buf)).await
        {
            let (n, from) = received?;
            if n > LAN_MAX_ANNOUNCEMENT_BYTES {
                continue;
            }
            let Ok(announcement) = serde_json::from_slice::<LanAnnouncement>(&buf[..n]) else {
                continue;
            };
            let host = LanHost {
                addr: from.ip(),
                announcement,
            };
            if host.announcement.game == game && !hosts.contains(&host) {
                hosts.push(host);
            }
        }
        Ok(hosts)
    }

    /// A UDP socket on `0.0.0.0:port` that other processes may bind as well.
    fn listen(port: u16) -> io::Result<UdpSocket> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        // BSDs and macOS only share the port with SO_REUSEPORT.
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)).into())?;
        UdpSocket::from_std(socket.into())
    }
}

#[cfg(feature = "lan-mdns")]
pub use mdns::discover_mdns;

/// Browsing mDNS / DNS-SD, behind the `lan-mdns` feature.
#[cfg(feature = "lan-mdns")]
mod mdns {
    use super::LanHost;
    use crate::proto::{LAN_MDNS_SERVICE, LanAnnouncement};
    use mdns_sd::{ServiceDaemon, ServiceEvent};
    use std::time::{Duration, Instant};

    /// Browses for `wait` and returns the distinct hosts of `game` that resolved.
    ///
    /// Blocks the calling thread; from async code, run it in
    /// `tokio::task::spawn_blocking`. IPv6 link-local addresses are skipped,
    /// since a URL can't carry their interface.
    pub fn discover_mdns(game: &str, wait: Duration) -> Result<Vec<LanHost>, String> {
        let daemon = ServiceDaemon::new().map_err(|e| format!("mDNS: {e}"))?;
        let events = daemon
            .browse(LAN_MDNS_SERVICE)
            .map_err(|e| format!("mDNS: {e}"))?;
        let deadline = Instant::now() + wait;
        let mut hosts = Vec::new();
        while let Ok(event) = events.recv_deadline(deadline) {
            let ServiceEvent::ServiceResolved(service) = event else {
                continue;
            };
            let get = |key: &str| service.txt_properties.get_property_val_str(key);
            let Some(announcement) = LanAnnouncement::from_txt(get, service.port) else {
                continue;
            };
            if announcement.game != game {
                continue;
            }
            for ip in &service.addresses {
                let host = LanHost {
                    addr: ip.to_ip_addr(),
                    announcement: announcement.clone(),
                };
                let link_local_v6 =
                    matches!(host.addr, std::net::IpAddr::V6(v6) if v6.is_unicast_link_local());
                if !link_local_v6 && !hosts.contains(&host) {
                    hosts.push(host);
                }
            }
        }
        let _ = daemon.shutdown();
        Ok(hosts)
    }
}

/// Tests for LAN discovery.
#[cfg(test)]
mod tests {
    use super::*;

    /// IPv6 hosts get a bracketed URL.
    #[test]
    fn url_brackets_ipv6() {
        let host = |addr: &str| LanHost {
            addr: addr.parse().unwrap(),
            announcement: LanAnnouncement {
                game: "demo".into(),
                name: "host".into(),
                lobby_port: 7878,
                lobby: 1,
                code_required: false,
            },
        };
        assert_eq!(host("192.168.1.2").url(), "ws://192.168.1.2:7878");
        assert_eq!(host("fd00::2").url(), "ws://[fd00::2]:7878");
    }

    /// Announcements of the right game are found once each; others and garbage are ignored.
    #[cfg(feature = "lan-broadcast")]
    #[tokio::test]
    async fn broadcast_discovery_filters_and_dedupes() {
        use std::time::Duration;
        // Find a free port, then let discovery bind it.
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let ann = |game: &str| {
            serde_json::to_vec(&LanAnnouncement {
                game: game.into(),
                name: "host".into(),
                lobby_port: 7878,
                lobby: 5,
                code_required: true,
            })
            .unwrap()
        };
        let search = tokio::spawn(discover_broadcast(port, "demo", Duration::from_secs(1)));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let tx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let to = ("127.0.0.1", port);
        for payload in [
            ann("demo"),
            ann("demo"),
            ann("other"),
            b"{not json".to_vec(),
            vec![b' '; 4096],
        ] {
            tx.send_to(&payload, to).await.unwrap();
        }
        let hosts = search.await.unwrap().unwrap();
        assert_eq!(hosts.len(), 1, "{hosts:?}");
        assert_eq!(hosts[0].url(), "ws://127.0.0.1:7878");
        assert!(hosts[0].announcement.code_required);
    }
}

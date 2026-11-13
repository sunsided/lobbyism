//! LAN play: the game of one player hosts the lobby and the game server, and
//! players on the same network find it by UDP broadcast or mDNS.
//!
//! Pair these with [`LanValidator`](crate::LanValidator) for authentication and
//! a [`NetcodeIssuer`](crate::NetcodeIssuer) whose `server_addresses` come from
//! [`lan_addresses`].

use std::net::{IpAddr, SocketAddr};

/// The most addresses a netcode `ConnectToken` can carry.
const MAX_TOKEN_ADDRESSES: usize = 32;

/// This machine's network addresses with `port`, for `NetcodeIssuer::server_addresses`
/// and the renet server's `public_addresses` (both must list the same).
///
/// Interfaces that are up, without loopback and link-local addresses, IPv4
/// first, at most 32. A joiner tries them in order, so a virtual interface it
/// cannot reach (a Docker bridge, a VPN) costs it a connection timeout; filter
/// the list if the host has those.
pub fn lan_addresses(port: u16) -> std::io::Result<Vec<SocketAddr>> {
    let mut ips: Vec<IpAddr> = if_addrs::get_if_addrs()?
        .into_iter()
        .filter(|i| i.is_oper_up() && !i.is_loopback() && !i.is_link_local())
        .map(|i| i.ip())
        .collect();
    // `IpAddr`'s order puts IPv4 first; sorting also makes duplicates adjacent for `dedup`.
    ips.sort();
    ips.dedup();
    ips.truncate(MAX_TOKEN_ADDRESSES);
    Ok(ips
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect())
}

#[cfg(feature = "lan-broadcast")]
pub use broadcast::announce_broadcast;

/// Announcing by UDP broadcast, behind the `lan-broadcast` feature.
#[cfg(feature = "lan-broadcast")]
mod broadcast {
    use lobbyism::proto::{LAN_MAX_ANNOUNCEMENT_BYTES, LanAnnouncement};
    use std::io;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use tokio::net::UdpSocket;

    /// Pause between two announcements.
    const INTERVAL: Duration = Duration::from_secs(1);

    /// Broadcasts `announcement` to UDP `port` on every IPv4 network, once a second.
    ///
    /// Runs until the task is dropped; abort it when the lobby closes or fills
    /// up, and start a new one when the announcement changes. Interfaces are
    /// enumerated anew each round, so a host that switches networks keeps
    /// being found. Fails only if the socket cannot be set up or the
    /// announcement is over [`LAN_MAX_ANNOUNCEMENT_BYTES`].
    pub async fn announce_broadcast(port: u16, announcement: &LanAnnouncement) -> io::Result<()> {
        announce_to(announcement, || broadcast_targets(port)).await
    }

    /// The announce loop, sending to whatever `targets` returns each round.
    pub(super) async fn announce_to(
        announcement: &LanAnnouncement,
        targets: impl Fn() -> Vec<SocketAddr>,
    ) -> io::Result<()> {
        let payload = serde_json::to_vec(announcement).map_err(io::Error::other)?;
        if payload.len() > LAN_MAX_ANNOUNCEMENT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LAN announcement too long",
            ));
        }
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.set_broadcast(true)?;
        let mut tick = tokio::time::interval(INTERVAL);
        loop {
            tick.tick().await;
            for target in targets() {
                // One unreachable network (cable out, interface going down) must not stop the rest.
                if let Err(e) = socket.send_to(&payload, target).await {
                    tracing::debug!(%target, "LAN announcement not sent: {e}");
                }
            }
        }
    }

    /// Each IPv4 interface's broadcast address, plus the limited broadcast address.
    fn broadcast_targets(port: u16) -> Vec<SocketAddr> {
        let mut targets: Vec<SocketAddr> = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .filter(|i| i.is_oper_up() && !i.is_loopback())
            .filter_map(|i| match i.addr {
                if_addrs::IfAddr::V4(v4) => v4.broadcast,
                if_addrs::IfAddr::V6(_) => None,
            })
            .chain([Ipv4Addr::BROADCAST])
            .map(|ip| SocketAddr::from((ip, port)))
            .collect();
        targets.sort();
        targets.dedup();
        targets
    }
}

#[cfg(feature = "lan-mdns")]
pub use mdns::{MdnsAnnouncer, announce_mdns};

/// Announcing over mDNS / DNS-SD, behind the `lan-mdns` feature.
#[cfg(feature = "lan-mdns")]
mod mdns {
    use lobbyism::proto::{LAN_MDNS_SERVICE, LanAnnouncement};
    use mdns_sd::{ServiceDaemon, ServiceInfo};
    use std::time::Duration;

    /// Longest TXT property value DNS-SD allows (one length byte, key included).
    const MAX_TXT_BYTES: usize = 255;
    /// How long dropping an [`MdnsAnnouncer`] waits for its goodbye to go out.
    const GOODBYE_WAIT: Duration = Duration::from_secs(1);

    /// A registered mDNS service; dropping it withdraws the announcement.
    #[must_use = "the announcement is withdrawn as soon as this is dropped"]
    pub struct MdnsAnnouncer {
        /// The responder thread that answers queries.
        daemon: ServiceDaemon,
        /// The registered instance, for unregistering.
        fullname: String,
    }

    impl Drop for MdnsAnnouncer {
        /// Sends the goodbye packet and stops the responder.
        ///
        /// Waits up to a second for the responder to confirm the goodbye, so
        /// browsers drop the lobby right away instead of when its record
        /// expires; past that, the goodbye is best-effort.
        fn drop(&mut self) {
            if let Ok(status) = self.daemon.unregister(&self.fullname) {
                let _ = status.recv_timeout(GOODBYE_WAIT);
            }
            let _ = self.daemon.shutdown();
        }
    }

    /// Registers `announcement` as a `_lobbyism._tcp` service on all interfaces.
    ///
    /// The announcement stays up until the returned [`MdnsAnnouncer`] is
    /// dropped; register a new one when it changes. Fails if the mDNS
    /// responder cannot start, rejects the service, or a TXT value (e.g. the
    /// name) is too long. Sending the announcements happens afterwards on the
    /// responder's thread, so network errors there aren't reported here. The
    /// instance name comes from the random lobby id, so two hosts don't clash
    /// over it in practice (mdns-sd would rename one if they did).
    pub fn announce_mdns(announcement: &LanAnnouncement) -> Result<MdnsAnnouncer, String> {
        let txt = announcement.to_txt();
        if txt
            .iter()
            .any(|(k, v)| k.len() + 1 + v.len() > MAX_TXT_BYTES)
        {
            return Err("LAN announcement field too long for mDNS".into());
        }
        let daemon = ServiceDaemon::new().map_err(|e| format!("mDNS: {e}"))?;
        // Lobby ids are random, so they make unique instance and host names.
        let instance = format!("lobby-{:016x}", announcement.lobby);
        let info = ServiceInfo::new(
            LAN_MDNS_SERVICE,
            &instance,
            &format!("{instance}.local."),
            (),
            announcement.lobby_port,
            &txt[..],
        )
        .map_err(|e| format!("mDNS: {e}"))?
        .enable_addr_auto();
        let fullname = info.get_fullname().to_string();
        daemon.register(info).map_err(|e| format!("mDNS: {e}"))?;
        Ok(MdnsAnnouncer { daemon, fullname })
    }
}

/// Tests for the LAN helpers.
#[cfg(test)]
mod tests {
    use super::*;

    /// Ticket addresses never include loopback or link-local, and fit a token.
    #[test]
    fn lan_addresses_are_reachable_kinds() {
        let addrs = lan_addresses(5000).unwrap();
        assert!(addrs.len() <= MAX_TOKEN_ADDRESSES);
        let mut unique = addrs.clone();
        unique.dedup();
        assert_eq!(unique, addrs, "duplicate addresses");
        assert!(addrs.is_sorted_by_key(|a| a.is_ipv6()), "IPv6 before IPv4");
        for a in addrs {
            assert_eq!(a.port(), 5000);
            assert!(!a.ip().is_loopback(), "{a}");
        }
    }

    /// The broadcast loop sends the announcement as JSON to its targets.
    #[cfg(feature = "lan-broadcast")]
    #[tokio::test]
    async fn broadcast_sends_json_announcement() {
        use lobbyism::proto::LanAnnouncement;
        let rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let to = rx.local_addr().unwrap();
        let ann = LanAnnouncement {
            game: "demo".into(),
            name: "host".into(),
            lobby_port: 7878,
            lobby: 9,
            code_required: false,
        };
        let sender = tokio::spawn({
            let ann = ann.clone();
            async move { broadcast::announce_to(&ann, || vec![to]).await }
        });
        let mut buf = [0u8; 2048];
        let (n, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        sender.abort();
        let got: LanAnnouncement = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(got, ann);
    }

    /// Oversized announcements are refused instead of being sent truncated.
    #[cfg(feature = "lan-broadcast")]
    #[tokio::test]
    async fn broadcast_refuses_oversized_announcement() {
        use lobbyism::proto::LanAnnouncement;
        let ann = LanAnnouncement {
            game: "demo".into(),
            name: "x".repeat(2000),
            lobby_port: 7878,
            lobby: 9,
            code_required: false,
        };
        let err = broadcast::announce_to(&ann, Vec::new).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}

//! Authentication at the trust boundary: turning a client's `Hello` token into a [`PlayerId`].

use hmac::{Hmac, KeyInit, Mac};
use lobbyism::proto::PlayerId;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

/// Validates the token a client sends in `ClientMsg::Hello`.
pub trait TokenValidator: Send + Sync + 'static {
    /// Returns the player the token belongs to, or the reason it was rejected.
    /// The reason is sent back to the client, so it must not leak secrets.
    fn validate(&self, token: &str) -> Result<PlayerId, String>;
}

/// Default validator: `"<player>.<expires_unix>.<hex hmac-sha256>"`.
///
/// Your auth service signs tokens with [`HmacValidator::sign`] using the shared
/// secret; this server only checks them. The secret never leaves the backend.
#[derive(Clone)]
pub struct HmacValidator {
    /// Shared HMAC-SHA256 key, at least 32 bytes.
    secret: Vec<u8>,
}

impl HmacValidator {
    /// Rejects secrets shorter than 32 bytes.
    pub fn new(secret: impl Into<Vec<u8>>) -> Result<Self, String> {
        let secret = secret.into();
        if secret.len() < 32 {
            return Err("HMAC secret must be at least 32 bytes".into());
        }
        Ok(Self { secret })
    }

    /// Create a token for `player` that expires at `expires_unix` (seconds).
    pub fn sign(&self, player: PlayerId, expires_unix: u64) -> String {
        let payload = format!("{player}.{expires_unix}");
        let tag = to_hex(&self.mac(&payload).finalize().into_bytes());
        format!("{payload}.{tag}")
    }

    /// HMAC-SHA256 over `payload`, ready to finalize or verify.
    fn mac(&self, payload: &str) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(payload.as_bytes());
        mac
    }
}

impl TokenValidator for HmacValidator {
    /// Checks the tag in constant time first, then parses the payload and the expiry.
    fn validate(&self, token: &str) -> Result<PlayerId, String> {
        /// One message for every malformed or forged token, so failures reveal nothing.
        const INVALID: &str = "invalid token";
        let (payload, tag_hex) = token.rsplit_once('.').ok_or(INVALID)?;
        let tag = decode_hex(tag_hex).ok_or(INVALID)?;
        self.mac(payload).verify_slice(&tag).map_err(|_| INVALID)?;
        // Signature is valid, so the payload is ours; parse failures are still rejected.
        let (player, expires) = payload.split_once('.').ok_or(INVALID)?;
        let player: PlayerId = player.parse().map_err(|_| INVALID)?;
        let expires: u64 = expires.parse().map_err(|_| INVALID)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        if now >= expires {
            return Err("token expired".into());
        }
        Ok(player)
    }
}

/// Validator for LAN play, where there is no auth backend to sign tokens.
///
/// Every connection gets a fresh random [`PlayerId`], so a player who
/// reconnects comes back as somebody new. [`LanValidator::open`] lets in anyone
/// who can reach the port; [`LanValidator::with_code`] additionally requires
/// the `Hello` token to equal a join code the host shows its friends. The
/// server has no rate limits, so someone on the network can try codes as fast
/// as they can connect: use at least 8 random characters, not a short PIN.
#[derive(Clone)]
pub struct LanValidator {
    /// The required join code, if any.
    code: Option<Vec<u8>>,
}

impl LanValidator {
    /// Anyone on the network may join; the token is ignored.
    pub fn open() -> Self {
        Self { code: None }
    }

    /// Only clients whose `Hello` token equals `code` may join.
    pub fn with_code(code: impl Into<String>) -> Self {
        Self {
            code: Some(code.into().into_bytes()),
        }
    }

    /// Whether clients need the join code; goes into `LanAnnouncement::code_required`.
    pub fn code_required(&self) -> bool {
        self.code.is_some()
    }
}

impl TokenValidator for LanValidator {
    /// Checks the join code in constant time, then hands out a random player id.
    fn validate(&self, token: &str) -> Result<PlayerId, String> {
        if let Some(code) = &self.code {
            // Comparing MACs of both strings is constant-time without a new dependency.
            // (They go in as messages: as HMAC keys, "x" and "x\0" would be equal.)
            let mac = |s: &[u8]| {
                let mut m = Hmac::<Sha256>::new_from_slice(b"lobbyism-lan").expect("any key");
                m.update(s);
                m
            };
            mac(code)
                .verify(&mac(token.as_bytes()).finalize().into_bytes())
                .map_err(|_| "invalid join code")?;
        }
        let mut id = [0u8; 8];
        getrandom::fill(&mut id).map_err(|e| format!("no OS randomness: {e}"))?;
        Ok(PlayerId::from_le_bytes(id))
    }
}

/// Lowercase hex encoding, the inverse of [`decode_hex`].
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Strict lowercase hex, so every tag has exactly one accepted spelling.
/// (`from_str_radix` alone would also take `A` and a leading `+`.)
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let lower_hex = |b: &u8| matches!(b, b'0'..=b'9' | b'a'..=b'f');
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| lower_hex(&b)) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Unit and property tests for token signing and validation.
#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only HMAC secret, exactly 32 bytes.
    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    /// An expiry that never passes during a test run.
    fn far_future() -> u64 {
        u64::MAX / 2
    }

    /// A token signed with the validator's secret is accepted.
    #[test]
    fn accepts_signed_token() {
        let v = HmacValidator::new(SECRET).unwrap();
        assert_eq!(v.validate(&v.sign(42, far_future())), Ok(42));
    }

    /// Tampered, foreign-signed, expired and malformed tokens are all rejected.
    #[test]
    fn rejects_tampered_foreign_expired_and_garbage() {
        let v = HmacValidator::new(SECRET).unwrap();
        let token = v.sign(42, far_future());
        let tampered = token.replacen("42.", "43.", 1);
        assert!(v.validate(&tampered).is_err());

        let other = HmacValidator::new(b"another-secret-another-secret-xx".to_vec()).unwrap();
        assert!(other.validate(&token).is_err());

        assert_eq!(v.validate(&v.sign(42, 1)), Err("token expired".into()));
        for junk in ["", ".", "1.2.zz", "1.2.é1", "no-dots"] {
            assert!(v.validate(junk).is_err(), "{junk}");
        }
    }

    proptest::proptest! {
        /// Every signed, unexpired token validates to its player.
        #[test]
        fn signed_tokens_validate(player: u64, ttl in 60u64..1 << 40) {
            let v = HmacValidator::new(SECRET).unwrap();
            let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + ttl;
            proptest::prop_assert_eq!(v.validate(&v.sign(player, expires)), Ok(player));
        }

        /// Any single-character change to a valid token is rejected, including
        /// hex case flips and sign prefixes that decode to the same bytes.
        #[test]
        fn mutated_tokens_are_rejected(player: u64, pos: proptest::sample::Index, c: char) {
            let v = HmacValidator::new(SECRET).unwrap();
            let token = v.sign(player, far_future());
            let i = pos.index(token.len());
            proptest::prop_assume!(token.as_bytes()[i] as char != c);
            let mut mutated = token.clone();
            mutated.replace_range(i..=i, &c.to_string());
            proptest::prop_assert!(v.validate(&mutated).is_err(), "{} accepted", mutated);
        }

        /// Validation of arbitrary strings returns an error instead of panicking.
        #[test]
        fn arbitrary_input_never_panics(s in "\\PC*") {
            let v = HmacValidator::new(SECRET).unwrap();
            let _ = v.validate(&s);
        }
    }

    /// An open LAN validator lets anyone in, each with a new id.
    #[test]
    fn open_lan_validator_hands_out_fresh_ids() {
        let v = LanValidator::open();
        assert!(!v.code_required());
        let a = v.validate("").unwrap();
        let b = v.validate("anything").unwrap();
        assert_ne!(a, b);
    }

    /// With a join code, only the exact code gets in.
    #[test]
    fn lan_join_code_must_match() {
        let v = LanValidator::with_code("k7Qp2xWm");
        assert!(v.code_required());
        assert!(v.validate("k7Qp2xWm").is_ok());
        for wrong in ["", "k7Qp2xW", "k7Qp2xWm ", "k7Qp2xWm\0", "K7QP2XWM"] {
            assert_eq!(
                v.validate(wrong),
                Err("invalid join code".into()),
                "{wrong}"
            );
        }
    }

    /// Secrets shorter than 32 bytes are refused.
    #[test]
    fn rejects_short_secret() {
        assert!(HmacValidator::new(b"short".to_vec()).is_err());
    }
}

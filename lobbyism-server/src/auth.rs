//! Authentication at the trust boundary: turning a client's `Hello` token into a [`PlayerId`].

use hmac::{Hmac, KeyInit, Mac};
use lobbyism_proto::PlayerId;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

/// Validates the token a client sends in `ClientMsg::Hello`.
pub trait TokenValidator: Send + Sync + 'static {
    fn validate(&self, token: &str) -> Result<PlayerId, String>;
}

/// Default validator: `"<player>.<expires_unix>.<hex hmac-sha256>"`.
///
/// Your auth service signs tokens with [`HmacValidator::sign`] using the shared
/// secret; this server only checks them. The secret never leaves the backend.
#[derive(Clone)]
pub struct HmacValidator {
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

    fn mac(&self, payload: &str) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(payload.as_bytes());
        mac
    }
}

impl TokenValidator for HmacValidator {
    fn validate(&self, token: &str) -> Result<PlayerId, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn far_future() -> u64 {
        u64::MAX / 2
    }

    #[test]
    fn accepts_signed_token() {
        let v = HmacValidator::new(SECRET).unwrap();
        assert_eq!(v.validate(&v.sign(42, far_future())), Ok(42));
    }

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

        #[test]
        fn arbitrary_input_never_panics(s in "\\PC*") {
            let v = HmacValidator::new(SECRET).unwrap();
            let _ = v.validate(&s);
        }
    }

    #[test]
    fn rejects_short_secret() {
        assert!(HmacValidator::new(b"short".to_vec()).is_err());
    }
}

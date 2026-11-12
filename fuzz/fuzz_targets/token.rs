//! Untrusted `Hello` tokens: validation never panics, and nothing but a token
//! this server signed (in its canonical spelling) is accepted.
#![no_main]

use libfuzzer_sys::fuzz_target;
use lobbyism_server::{HmacValidator, TokenValidator};

fuzz_target!(|token: &str| {
    let v = HmacValidator::new(*b"fuzz-secret-fuzz-secret-fuzz-sec").unwrap();
    if let Ok(player) = v.validate(token) {
        let (_, expires_and_tag) = token.split_once('.').unwrap();
        let expires = expires_and_tag.split_once('.').unwrap().0.parse().unwrap();
        assert_eq!(
            v.sign(player, expires),
            token,
            "accepted a non-canonical token"
        );
    }
});

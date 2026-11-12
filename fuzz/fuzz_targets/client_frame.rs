//! Untrusted WebSocket text frames: parsing never panics, and anything that
//! parses survives a serialize/parse round trip unchanged.
#![no_main]

use libfuzzer_sys::fuzz_target;
use lobbyism_proto::ClientMsg;

fuzz_target!(|data: &[u8]| {
    let Ok(msg) = serde_json::from_slice::<ClientMsg>(data) else {
        return;
    };
    let json = serde_json::to_string(&msg).expect("parsed message serializes");
    let again: ClientMsg = serde_json::from_str(&json).expect("own output parses");
    assert_eq!(msg, again);
});

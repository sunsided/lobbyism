//! Wire-level checks of the server's authentication handshake.

use lobbyism_client::{Error, LobbyClient};
use lobbyism_server::{Config, Core, HmacValidator, MatchboxIssuer, serve};

const SECRET: &[u8] = b"handshake-test-secret-32-bytes!!";

async fn start() -> (String, HmacValidator) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let validator = HmacValidator::new(SECRET).unwrap();
    let core = Core::new(Config::default(), MatchboxIssuer::new("ws://mb").unwrap());
    tokio::spawn(serve(listener, core, validator.clone()));
    (url, validator)
}

#[tokio::test]
async fn valid_token_is_welcomed_and_bad_token_rejected() {
    let (url, validator) = start().await;
    let client = LobbyClient::connect(&url, &validator.sign(7, u64::MAX / 2))
        .await
        .unwrap();
    assert_eq!(client.player(), 7);

    let forged = HmacValidator::new(b"some-other-secret-some-other-sec".to_vec())
        .unwrap()
        .sign(8, u64::MAX / 2);
    assert!(matches!(
        LobbyClient::connect(&url, &forged).await,
        Err(Error::Rejected(_))
    ));
}

#[tokio::test]
async fn second_connection_for_same_player_is_rejected() {
    let (url, validator) = start().await;
    let token = validator.sign(7, u64::MAX / 2);
    let _first = LobbyClient::connect(&url, &token).await.unwrap();
    assert!(matches!(
        LobbyClient::connect(&url, &token).await,
        Err(Error::Rejected(_))
    ));
}

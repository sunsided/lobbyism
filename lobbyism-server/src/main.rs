//! `lobbyism-server`: lobby + matchmaking over WebSocket, issuing renet ConnectTokens.
//!
//! Configuration (environment):
//! - `LOBBYISM_BIND`          listen address, default `127.0.0.1:7878`
//! - `LOBBYISM_HMAC_SECRET`   shared secret for client tokens, >= 32 bytes (required)
//! - `LOBBYISM_NETCODE_KEY`   netcode private key, 64 hex chars (required)
//! - `LOBBYISM_PROTOCOL_ID`   netcode protocol id (required)
//! - `LOBBYISM_GAME_SERVERS`  comma-separated game server addresses (required)
//! - `LOBBYISM_MODES`         `mode=players,...`, default `duel=2`
//! - `LOBBYISM_REGIONS`       comma-separated regions, default `eu,us`
//!
//! With the `dotenv` feature (on by default), a `.env` file in the working
//! directory or one of its parents is loaded first; variables already set in
//! the environment take precedence.

use lobbyism_server::{Config, Core, HmacValidator, NetcodeIssuer, serve};
use std::env;

/// Loads the configuration from the environment (and `.env`) and serves until the listener fails.
#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt::init();

    #[cfg(feature = "dotenv")]
    match dotenvy::dotenv() {
        Ok(path) => tracing::info!("loaded {}", path.display()),
        Err(e) if e.not_found() => {}
        // A broken .env would otherwise silently fall back to defaults.
        Err(e) => return Err(format!(".env: {}", dotenv_error(&e))),
    }

    let bind = env::var("LOBBYISM_BIND").unwrap_or_else(|_| "127.0.0.1:7878".into());
    let validator = HmacValidator::new(required("LOBBYISM_HMAC_SECRET")?)?;
    let issuer = NetcodeIssuer {
        protocol_id: parse("LOBBYISM_PROTOCOL_ID", &required("LOBBYISM_PROTOCOL_ID")?)?,
        private_key: parse_key(&required("LOBBYISM_NETCODE_KEY")?)?,
        server_addresses: list(&required("LOBBYISM_GAME_SERVERS")?)
            .map(|a| parse("LOBBYISM_GAME_SERVERS", a))
            .collect::<Result<_, _>>()?,
        expire_seconds: 30,
        timeout_seconds: 15,
    };
    let modes = env::var("LOBBYISM_MODES").unwrap_or_else(|_| "duel=2".into());
    let config = Config {
        modes: list(&modes)
            .map(|m| {
                let (name, n) = m
                    .split_once('=')
                    .ok_or(format!("LOBBYISM_MODES: bad entry {m:?}"))?;
                Ok((name.to_string(), parse("LOBBYISM_MODES", n)?))
            })
            .collect::<Result<_, String>>()?,
        regions: list(&env::var("LOBBYISM_REGIONS").unwrap_or_else(|_| "eu,us".into()))
            .map(String::from)
            .collect(),
        ..Config::default()
    };

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("bind {bind}: {e}"))?;
    tracing::info!("listening on ws://{bind}");
    serve(listener, Core::new(config, issuer), validator)
        .await
        .map_err(|e| e.to_string())
}

/// The value of a required environment variable.
fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}

/// Parses a configuration value, naming the variable in the error.
fn parse<T: std::str::FromStr>(name: &str, value: &str) -> Result<T, String> {
    value
        .trim()
        .parse()
        .map_err(|_| format!("{name}: cannot parse {value:?}"))
}

/// The non-empty, trimmed items of a comma-separated list.
fn list(s: &str) -> impl Iterator<Item = &str> {
    s.split(',').map(str::trim).filter(|s| !s.is_empty())
}

/// Decodes the 32-byte netcode key from 64 hex characters.
fn parse_key(hex: &str) -> Result<[u8; 32], String> {
    let err = || "LOBBYISM_NETCODE_KEY must be 64 hex chars".to_string();
    if hex.len() != 64 || !hex.is_ascii() {
        return Err(err());
    }
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(|_| err())?;
    }
    Ok(key)
}

/// Describes a `.env` error without echoing the offending line, which may hold a secret.
#[cfg(feature = "dotenv")]
fn dotenv_error(e: &dotenvy::Error) -> String {
    match e {
        dotenvy::Error::LineParse(line, _) => {
            let name = line.split('=').next().unwrap_or("").trim();
            if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                format!("cannot parse the line for {name} (value not shown)")
            } else {
                "cannot parse a line (contents not shown)".into()
            }
        }
        other => other.to_string(),
    }
}

/// Tests for the binary's configuration handling.
#[cfg(all(test, feature = "dotenv"))]
mod tests {
    use super::dotenv_error;

    /// A malformed `.env` line is reported by variable name, never with its value.
    #[test]
    fn parse_errors_never_echo_values() {
        let e = dotenvy::Error::LineParse("LOBBYISM_HMAC_SECRET=\"s3cr3t\n".into(), 21);
        let msg = dotenv_error(&e);
        assert!(!msg.contains("s3cr3t"), "{msg}");
        assert!(msg.contains("LOBBYISM_HMAC_SECRET"), "{msg}");
        let e = dotenvy::Error::LineParse("s3cr3t garbage\n".into(), 0);
        assert!(!dotenv_error(&e).contains("s3cr3t"));
    }
}

//! Waiting for a started dependency to answer.
//!
//! A container that is running is not a dependency that is up. The official
//! Postgres image runs `initdb` first and only then restarts the postmaster on
//! its TCP port, so a run that started driving its workload the moment Docker
//! said "started" would be measuring the harness's impatience.
//!
//! # Why these probes speak the protocol and use no client library
//!
//! A TCP connect proves a socket is open, which for Postgres is true well
//! before the server will accept a session. So each probe exchanges the
//! cheapest thing the protocol defines and looks at the answer.
//!
//! Hand-written rather than reached through `tokio-postgres` or `async-nats`
//! for two reasons. The client libraries are optional, one feature per
//! protocol, and readiness is not: a build that left an adapter out still has
//! to fail at the point that says so rather than at a probe that will not
//! compile. And a client library retries, backs off and reconnects on its own
//! schedule, which would put a second, invisible timing policy in front of
//! every run.
//!
//! # This clock is not the scheduler's
//!
//! These sleeps happen before the run starts, against a dependency nothing is
//! driving yet. Nothing here is a fork, and no decision a trace describes
//! depends on how long the wait took.

use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

/// How long to leave between attempts.
///
/// Short enough that a warm container costs nothing to wait for, long enough
/// that a cold one is not answered with a thousand refused connections.
const INTERVAL: Duration = Duration::from_millis(50);

/// How long one attempt may take before it is treated as a failed one.
///
/// Separate from the overall budget, so a server that accepts a connection and
/// then says nothing is retried rather than swallowing the whole timeout in a
/// single attempt.
const ATTEMPT: Duration = Duration::from_secs(2);

/// Waits until the dependency answers its own protocol, or the budget runs out.
pub async fn wait(name: &str, address: &str, budget: Duration) -> Result<()> {
    let started = Instant::now();

    loop {
        let refused = match tokio::time::timeout(ATTEMPT, probe(name, address)).await {
            Ok(Ok(())) => {
                tracing::debug!(
                    dependency = name,
                    address,
                    waited = ?started.elapsed(),
                    "dependency answered"
                );

                return Ok(());
            }
            // A dependency with no probe is not something a retry fixes, and
            // waiting out the budget would report it as a slow container
            // rather than as the missing probe it is.
            Ok(Err(Error::Internal(message))) => return Err(Error::Internal(message)),
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("no answer within {ATTEMPT:?}"),
        };

        // The last refusal, because it is the one that describes the state the
        // dependency is actually stuck in. A message saying only that the wait
        // expired sends the reader to the timeout rather than to the cause.
        if started.elapsed() >= budget {
            return Err(Error::Environment(format!(
                "{name} on {address} did not answer within {budget:?}: {refused}"
            )));
        }

        tokio::time::sleep(INTERVAL).await;
    }
}

/// One attempt, in the dependency's own protocol.
async fn probe(name: &str, address: &str) -> Result<()> {
    match name {
        "postgres" => postgres(address).await,
        "nats" => nats(address).await,
        "redis" => redis(address).await,
        other => Err(Error::Internal(format!(
            "no readiness probe for `{other}`, which should not have been started"
        ))),
    }
}

/// `SSLRequest`, which every Postgres answers with one byte before it has
/// authenticated anything.
///
/// The cheapest question the protocol has: no user, no database, no password,
/// and a definite answer. A postmaster that is still starting refuses the
/// connection outright, which is the failure this is looking for.
async fn postgres(address: &str) -> Result<()> {
    const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

    let mut stream = connect(address).await?;

    stream.write_all(&SSL_REQUEST).await?;
    stream.flush().await?;

    let mut answer = [0u8; 1];
    stream.read_exact(&mut answer).await?;

    match answer[0] {
        b'S' | b'N' => Ok(()),
        other => Err(Error::Environment(format!(
            "postgres answered an SSLRequest with `{}`",
            other as char
        ))),
    }
}

/// NATS greets every connection with `INFO`, unasked.
async fn nats(address: &str) -> Result<()> {
    let stream = connect(address).await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    reader.read_line(&mut line).await?;

    if line.starts_with("INFO ") {
        Ok(())
    } else {
        Err(Error::Environment(
            "nats did not open with INFO".to_string(),
        ))
    }
}

/// `PING`, as an inline command, so the probe needs no RESP encoder.
async fn redis(address: &str) -> Result<()> {
    let mut stream = connect(address).await?;

    stream.write_all(b"PING\r\n").await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    reader.read_line(&mut line).await?;

    if line.starts_with("+PONG") {
        Ok(())
    } else {
        Err(Error::Environment(format!(
            "redis answered PING with `{}`",
            line.trim()
        )))
    }
}

async fn connect(address: &str) -> Result<TcpStream> {
    TcpStream::connect(address)
        .await
        .map_err(|error| Error::Environment(format!("{address}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::net::TcpListener;

    /// A server that answers one connection with `answer` and then stops.
    async fn server(answer: &'static [u8]) -> String {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let address = listener.local_addr().expect("address").to_string();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let _ = stream.write_all(answer).await;
                let _ = stream.flush().await;

                // Held open rather than dropped, so a probe reading a line is
                // answered rather than racing the close.
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });

        address
    }

    #[tokio::test]
    async fn a_postgres_that_answers_an_ssl_request_is_ready() {
        let address = server(b"N").await;

        wait("postgres", &address, Duration::from_secs(2))
            .await
            .expect("an answered SSLRequest is the signal");
    }

    /// The property the whole module exists for. A socket that is open and
    /// says nothing is the state the Postgres image is in while `initdb` runs,
    /// and treating it as ready is what drives a workload at a server that is
    /// about to restart.
    #[tokio::test]
    async fn a_socket_that_accepts_and_says_nothing_is_not_ready() {
        let address = server(b"").await;

        let error = wait("postgres", &address, Duration::from_millis(200))
            .await
            .expect_err("silence is not readiness");

        assert!(matches!(error, Error::Environment(_)), "{error}");
    }

    #[tokio::test]
    async fn nothing_listening_is_reported_against_the_dependency() {
        // Port 1 on loopback, which nothing binds.
        let error = wait("postgres", "127.0.0.1:1", Duration::from_millis(200))
            .await
            .expect_err("there is no server there");

        assert!(
            error.to_string().contains("postgres"),
            "the error names the dependency: {error}"
        );
    }

    #[tokio::test]
    async fn a_nats_greeting_is_the_signal() {
        let address = server(b"INFO {\"server_id\":\"x\"}\r\n").await;

        wait("nats", &address, Duration::from_secs(2))
            .await
            .expect("INFO is what a NATS opens with");
    }

    #[tokio::test]
    async fn a_redis_pong_is_the_signal() {
        let address = server(b"+PONG\r\n").await;

        wait("redis", &address, Duration::from_secs(2))
            .await
            .expect("PONG is the answer to PING");
    }

    /// A dependency with no probe is a dependency nothing knows how to wait
    /// for, and starting one would mean reporting it ready on a guess.
    #[tokio::test]
    async fn a_dependency_with_no_probe_is_an_internal_error() {
        let error = wait("clickhouse", "127.0.0.1:1", Duration::from_secs(30))
            .await
            .expect_err("there is no probe for it");

        assert!(
            matches!(error, Error::Internal(_)),
            "a missing probe is misorder's own gap, and reporting it as a slow \
             container would send the reader to their Docker install: {error}"
        );
    }
}

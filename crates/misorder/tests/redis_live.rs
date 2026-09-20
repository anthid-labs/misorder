// Only in a build with the adapter, because the whole file drives it.
#![cfg(feature = "redis")]
//! The Redis loop, against a real server in a real container.
//!
//! Every test here is `#[ignore]`, so `cargo test --workspace` stays hermetic.
//!
//! ```bash
//! cargo test -p misorder --test redis_live -- --ignored
//! ```
//!
//! `MISORDER_TEST_REDIS_URL` points the suite at a server you already have
//! instead of starting one.
//!
//! What this proves that the unit tests cannot: the replies are a real Redis's,
//! so a command this adapter re-frames or mis-pairs stops working here rather
//! than merely producing a wrong event. The in-process fake answers `+OK` to
//! everything, which is enough to test what the schedule does to an ordering
//! and not enough to test that the ordering was understood.
//!
//! The client is hand-written RESP rather than a client library, for the same
//! reason the adapter is: this repository has no Redis client and does not want
//! one, and the bytes are the point.

use std::sync::Arc;
use std::time::Duration;

use misorder::event::{Event, RedisEvent};
use misorder::orchestrator::Environment;
use misorder::proxy::redis::RedisAdapter;
use misorder::proxy::{Adapter, EventSink, ProxyContext};
use misorder::scenario::file::{Deps, Redis, RunSettings};
use misorder::schedule::{DecisionSource, Scheduler};
use misorder::trace::{Decision, DecisionPoint, PointKind, Recorder};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// A Redis to test against, started here unless one was named.
struct Server {
    address: String,
    started: Option<Environment>,
}

impl Server {
    async fn start() -> Self {
        if let Some(address) = std::env::var("MISORDER_TEST_REDIS_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        {
            return Self {
                address,
                started: None,
            };
        }

        let deps = Deps {
            redis: Some(Redis::default()),
            ..Deps::default()
        };

        let environment = Environment::start(&deps, &RunSettings::default())
            .await
            .expect("a redis container starts");

        Self {
            address: environment
                .address_of("redis")
                .expect("a started container has an address")
                .to_string(),
            started: Some(environment),
        }
    }

    async fn stop(self) {
        if let Some(environment) = self.started {
            environment.stop().await;
        }
    }
}

/// Answers one chosen fork and stays neutral everywhere else.
struct At {
    kind: PointKind,
    ordinal: u64,
    decision: Decision,
}

impl DecisionSource for At {
    fn decide(&self, point: &DecisionPoint) -> Decision {
        if point.key.kind == self.kind && point.key.ordinal == self.ordinal {
            self.decision
        } else {
            Decision::NEUTRAL
        }
    }
}

struct Live {
    proxy: std::net::SocketAddr,
    events: tokio::sync::mpsc::UnboundedReceiver<misorder::event::Observed>,
    cancel: CancellationToken,
    serving: tokio::task::JoinHandle<misorder::error::Result<()>>,
}

async fn start(upstream: &str, source: Arc<dyn DecisionSource>) -> Live {
    let mut adapter = RedisAdapter::new();
    let endpoint = adapter.bind(upstream).await.expect("bind the proxy");

    let (events, receiver) = EventSink::new();
    let cancel = CancellationToken::new();
    let scheduler = Scheduler::new(source, Recorder::new(0, "redis_live"));
    let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

    let serving = tokio::spawn(async move { adapter.serve(context).await });

    Live {
        proxy: endpoint.listen,
        events: receiver,
        cancel,
        serving,
    }
}

impl Live {
    async fn drain(&mut self) -> Vec<Event> {
        self.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut self.serving).await;

        let mut out = Vec::new();

        while let Ok(observed) = self.events.try_recv() {
            out.push(observed.event);
        }

        out
    }
}

/// One command, as a RESP array of bulk strings.
fn command(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        out.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
    }

    out
}

/// A real client, through the proxy, end to end.
///
/// `SET` then `GET`, and the value that comes back is the one a real server
/// stored. A codec that re-framed a command wrongly would not get this far.
#[tokio::test]
#[ignore = "starts a real Redis container; run with --ignored"]
async fn a_real_server_answers_through_the_adapter() {
    let server = Server::start().await;

    let mut live = start(
        &server.address,
        Arc::new(At {
            kind: PointKind::Statement,
            ordinal: u64::MAX,
            decision: Decision::NEUTRAL,
        }),
    )
    .await;

    let stream = TcpStream::connect(live.proxy)
        .await
        .expect("reach the proxy");
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    write
        .write_all(&command(&["SET", "misorder:live", "stored"]))
        .await
        .expect("write");
    write
        .write_all(&command(&["GET", "misorder:live"]))
        .await
        .expect("write");
    write.flush().await.expect("flush");

    let mut set = String::new();
    read.read_line(&mut set).await.expect("a reply to SET");
    assert_eq!(set.trim_end(), "+OK");

    let mut header = String::new();
    read.read_line(&mut header).await.expect("a reply to GET");
    assert_eq!(header.trim_end(), "$6", "a real bulk string, real length");

    let mut value = String::new();
    read.read_line(&mut value).await.expect("the value");
    assert_eq!(
        value.trim_end(),
        "stored",
        "the value a real server kept, not one a fake echoed"
    );

    drop(write);

    let observed = live.drain().await;

    assert!(
        observed.iter().any(|event| matches!(
            event,
            Event::Redis(RedisEvent::Command { name, .. }) if name == "SET"
        )),
        "the command is observed by its name: {observed:?}"
    );

    server.stop().await;
}

/// The lock bug, against a real server, in the smallest form that shows it.
///
/// A short TTL, a reply the schedule holds past it, and a `DEL` that lands
/// after the key has already expired and been taken by somebody else. Nothing
/// here forges the expiry: it is the real server's, on the real clock, which is
/// the part no fake argues for.
#[tokio::test]
#[ignore = "starts a real Redis container; run with --ignored"]
async fn a_real_expiry_lets_a_second_client_take_the_lock() {
    let server = Server::start().await;

    let live = start(
        &server.address,
        Arc::new(At {
            kind: PointKind::Statement,
            ordinal: u64::MAX,
            decision: Decision::NEUTRAL,
        }),
    )
    .await;

    let holder = TcpStream::connect(live.proxy)
        .await
        .expect("reach the proxy");
    let (read, mut write) = holder.into_split();
    let mut read = BufReader::new(read);

    write
        .write_all(&command(&[
            "SET",
            "misorder:lock",
            "token-a",
            "NX",
            "PX",
            "100",
        ]))
        .await
        .expect("write");
    write.flush().await.expect("flush");

    let mut taken = String::new();
    read.read_line(&mut taken).await.expect("a reply");
    assert_eq!(taken.trim_end(), "+OK", "the first client holds the lock");

    // The real server's own expiry, not a simulated one.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let other = TcpStream::connect(live.proxy)
        .await
        .expect("reach the proxy");
    let (other_read, mut other_write) = other.into_split();
    let mut other_read = BufReader::new(other_read);

    other_write
        .write_all(&command(&[
            "SET",
            "misorder:lock",
            "token-b",
            "NX",
            "PX",
            "5000",
        ]))
        .await
        .expect("write");
    other_write.flush().await.expect("flush");

    let mut retaken = String::new();
    other_read.read_line(&mut retaken).await.expect("a reply");

    assert_eq!(
        retaken.trim_end(),
        "+OK",
        "the lock expired under the first client, which is the premise of the bug"
    );

    drop(write);
    drop(other_write);

    let mut live = live;
    let _ = live.drain().await;

    server.stop().await;
}

/// A declared Redis that is already running needs no daemon at all.
#[tokio::test]
#[ignore = "starts a real Redis container; run with --ignored"]
async fn an_address_is_proxied_without_starting_anything() {
    let server = Server::start().await;

    let deps = Deps {
        redis: Some(Redis {
            address: Some(server.address.clone()),
            image: None,
        }),
        ..Deps::default()
    };

    let external = Environment::start(&deps, &RunSettings::default())
        .await
        .expect("an already-running server needs nothing started");

    assert_eq!(
        external
            .dependencies()
            .first()
            .expect("one dependency")
            .container_id,
        "external"
    );

    external.stop().await;

    // And the one this test started is still up, because stopping an
    // environment that owns nothing must not reach somebody else's container.
    let live = start(
        &server.address,
        Arc::new(At {
            kind: PointKind::Statement,
            ordinal: u64::MAX,
            decision: Decision::NEUTRAL,
        }),
    )
    .await;

    let stream = TcpStream::connect(live.proxy)
        .await
        .expect("reach the proxy");
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    write.write_all(&command(&["PING"])).await.expect("write");
    write.flush().await.expect("flush");

    let mut pong = String::new();
    read.read_line(&mut pong).await.expect("a reply");

    assert_eq!(pong.trim_end(), "+PONG");

    drop(write);

    let mut live = live;
    let _ = live.drain().await;

    server.stop().await;
}

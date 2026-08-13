//! The NATS and JetStream adapter.
//!
//! First alongside Postgres, and first among the message brokers for a reason
//! that is about the market rather than the protocol: NATS has real production
//! adoption and almost no testing tooling, so the gap between what teams run
//! and what they can test is widest here.
//!
//! # Protocol shape
//!
//! NATS core is a line protocol: `CONNECT`, `PUB`, `SUB`, `MSG`, `HMSG`, `PING`,
//! `PONG`, `+OK`, `-ERR`. Text commands with a length-prefixed payload, which
//! makes it the cheapest adapter to write and the right one to learn the seam
//! on. JetStream rides on top as request/reply to `$JS.API.>` subjects, plus
//! acks published back to a reply subject.
//!
//! # Where the forks are
//!
//! - [`PointKind::Deliver`] on every `MSG`/`HMSG` heading to the service.
//! - [`PointKind::Ack`] on every ack heading back to the server. This is the
//!   valuable one: swallowing it produces the redelivery, and delaying it past
//!   `ack_wait` produces the duplicate-processing race where the ack lands at a
//!   server that has already given up.
//! - [`PointKind::Connection`] on accept, and on each frame after.
//!
//! # What this adapter cannot do, and why Phase 3 exists
//!
//! `ack_wait` expiry fires on the server's own timer with no frame crossing the
//! wire. There is no decision here to intercept, because nothing asked. Forging
//! a redelivery does not help: the real server keeps its own `num_delivered`
//! and will redeliver again later, leaving two state machines that disagree,
//! which is implementing JetStream badly rather than avoiding implementing it.
//! That is why the simulated JetStream in Phase 3 is the first sim written, and
//! why it is diffed against this adapter's real server rather than replacing
//! it.

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::PointKind;

/// Proxies a NATS connection.
#[derive(Debug, Default)]
pub struct NatsAdapter;

impl NatsAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for NatsAdapter {
    fn protocol(&self) -> &'static str {
        "nats"
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        let _ = upstream;

        Err(Error::Unsupported(
            "the NATS adapter does not bind yet: it needs the line-protocol codec".to_string(),
        ))
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        tracing::debug!(
            upstream = %context.upstream,
            "nats adapter would forward to upstream"
        );

        // The shape the implementation takes, kept here so the seam is not
        // guessed at later: read a frame, classify it, ask before passing it on.
        let _ = PointKind::Deliver;

        Err(Error::Unsupported(
            "the NATS adapter does not serve yet".to_string(),
        ))
    }
}

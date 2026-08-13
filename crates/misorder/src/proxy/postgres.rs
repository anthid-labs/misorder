//! The Postgres adapter.
//!
//! On the critical path with NATS, and the one whose fidelity argument is
//! strongest: holding statements to force an exact interleaving against a real
//! server produces real serialization failures, real lock waits and real
//! isolation semantics. A simulator would have to reimplement the planner to
//! get any of that right, so this adapter is permanent.
//!
//! TimescaleDB is a flag on this adapter, not a separate thing: it speaks the
//! same wire protocol, and the differences that matter are in what the server
//! does with a statement rather than in how the statement arrives.
//!
//! # Protocol shape
//!
//! A length-prefixed binary protocol: a startup message, then typed messages in
//! both directions. Simple query (`Q`) and extended query
//! (`Parse`/`Bind`/`Execute`/`Sync`) both matter, because a service using
//! prepared statements has its statement boundaries in different places.
//!
//! # Where the forks are
//!
//! - [`PointKind::Statement`] before a statement goes upstream. Holding one
//!   here until another connection commits is the interleaving control, and it
//!   is the reason to write this adapter at all.
//! - [`PointKind::Response`] before a result goes back.
//! - [`PointKind::Connection`] on accept and on each message after.
//!
//! # Reading the session, not just forwarding it
//!
//! The adapter tracks enough state to emit
//! [`PostgresEvent`](crate::event::PostgresEvent): transaction boundaries,
//! statement text, and the SQLSTATE on an `ErrorResponse`. That is what the
//! built-in invariants need, and none of it requires understanding SQL: the
//! transaction state is in the `ReadyForQuery` byte the server already sends.

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::PointKind;

/// Proxies a Postgres connection.
#[derive(Debug, Default)]
pub struct PostgresAdapter;

impl PostgresAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for PostgresAdapter {
    fn protocol(&self) -> &'static str {
        "postgres"
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        let _ = upstream;

        Err(Error::Unsupported(
            "the Postgres adapter does not bind yet: it needs the message-frame codec".to_string(),
        ))
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        tracing::debug!(
            upstream = %context.upstream,
            "postgres adapter would forward to upstream"
        );

        let _ = PointKind::Statement;

        Err(Error::Unsupported(
            "the Postgres adapter does not serve yet".to_string(),
        ))
    }
}

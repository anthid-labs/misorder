//! The wire between the service under test and its dependencies.
//!
//! Every connection the service makes goes through here. The proxy speaks the
//! real protocol in both directions and is where all nondeterminism gets
//! injected: drop the connection, delay the response, reorder two in-flight
//! replies, swallow an ack, corrupt a frame, hold statement B until statement A
//! commits.
//!
//! # This layer is permanent, not a stepping stone
//!
//! It is tempting to read the proxy as scaffolding until simulators exist. It
//! is the opposite. For Postgres, holding statements to force an exact
//! interleaving gives real serialization failures and real isolation semantics
//! against the real planner. No simulator anyone writes will reproduce that,
//! and one that tried would be reimplementing Postgres.
//!
//! Phase 3 adds simulated peers only where a proxy structurally cannot reach.
//! The sorting rule: **does anything I care about happen without a client
//! asking?** JetStream's `ack_wait` fires on the server's own timer with no
//! frame crossing the wire, so a proxy cannot intercept a decision that never
//! happened in front of it. Postgres, Redis and ClickHouse are reactive and
//! stay proxied indefinitely.
//!
//! # The rule for anything added here
//!
//! Every branch that could go two ways goes through
//! [`ProxyContext::decide`]. No `Instant::now`, no `rand`, no `tokio::select!`
//! over two futures whose completion order the adapter does not control. An
//! adapter that breaks this does not fail loudly: it produces traces that
//! replay into a different run, and the tool's one promise stops being true.

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

/// Every protocol an adapter is written for, compiled in or not.
///
/// Separate from [`speaks`] because "we have not written that codec" and "your
/// build turned it off" are different problems with different fixes, and a
/// caller that could not tell them apart would report the first when it meant
/// the second.
pub const ADAPTERS: &[&str] = &["nats", "postgres", "redis", "http"];

/// Whether this build carries the adapter for a protocol.
///
/// Beside the module gates above on purpose. Two matches on the same feature
/// set, in two files, is two things to remember to change.
pub fn speaks(protocol: &str) -> bool {
    match protocol {
        "nats" => cfg!(feature = "nats"),
        "postgres" => cfg!(feature = "postgres"),
        "redis" => cfg!(feature = "redis"),
        "http" => cfg!(feature = "http"),
        // Not a protocol with an adapter at all. `matches!` would read the same
        // and lose the one-line-per-feature shape that makes a missing arm
        // visible next to the module gates above.
        #[allow(clippy::match_like_matches_macro)]
        _ => false,
    }
}

/// Why a protocol cannot be proxied, in the words that fit the actual reason.
pub fn unsupported(protocol: &str) -> crate::error::Error {
    if ADAPTERS.contains(&protocol) {
        crate::error::Error::Unsupported(format!(
            "`{protocol}` needs the `{protocol}` feature, and this build does not have it"
        ))
    } else {
        crate::error::Error::Unsupported(format!(
            "`{protocol}` cannot be proxied yet: its wire codec is not written"
        ))
    }
}

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::event::{ConnectionId, Event, Observed};
use crate::schedule::Scheduler;
use crate::trace::{Decision, DecisionPoint, PointKind};

/// Where a proxy listens, and what to tell the service so it connects there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub protocol: &'static str,
    pub listen: SocketAddr,
    /// Environment injected into the service under test.
    ///
    /// The service is pointed at the proxy through its ordinary configuration,
    /// which is the whole language stance in one mechanism: no import, no build
    /// flag, just a different address in `NATS_URL`. It also cannot be
    /// overridden from the scenario's own `env`, because a service that
    /// connected to the real dependency would produce a clean run that tested
    /// nothing.
    pub env: Vec<(String, String)>,
}

/// Somewhere events go.
///
/// Unbounded on purpose. A bounded channel would make a slow invariant apply
/// backpressure to the proxy, which changes the timing of the run being
/// observed: the measurement would alter the thing measured, and the failure
/// would be misorder's.
#[derive(Debug, Clone)]
pub struct EventSink {
    sender: mpsc::UnboundedSender<Observed>,
}

impl EventSink {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Observed>) {
        let (sender, receiver) = mpsc::unbounded_channel();

        (Self { sender }, receiver)
    }

    /// Emits an observation from a proxied connection.
    ///
    /// A closed receiver means the run is already over, so the event is
    /// dropped rather than propagated: an adapter unwinding on the way out
    /// would turn a completed run into an error.
    pub fn emit(&self, at: std::time::Duration, connection: ConnectionId, event: Event) {
        let _ = self.sender.send(Observed::on(at, connection, event));
    }

    /// Emits a harness observation, belonging to no connection.
    pub fn emit_lifecycle(&self, at: std::time::Duration, event: Event) {
        let _ = self.sender.send(Observed::new(at, event));
    }
}

/// Hands out fork ordinals.
///
/// Shared machinery rather than something each adapter counts for itself,
/// because the ordinal is what a decision is looked up by on replay. An adapter
/// that numbered its forks differently on the second run would replay the wrong
/// decisions at the right-looking places, and nothing would report it.
#[derive(Debug, Default)]
pub struct Forks {
    counters: Mutex<HashMap<(PointKind, u64), u64>>,
}

impl Forks {
    pub fn next(&self, kind: PointKind, connection: ConnectionId) -> u64 {
        let mut counters = self.counters.lock().expect("forks mutex poisoned");
        let counter = counters.entry((kind, connection.0)).or_insert(0);
        let ordinal = *counter;

        *counter = counter.saturating_add(1);

        ordinal
    }
}

/// Everything an adapter needs, and deliberately nothing else.
///
/// No clock, no RNG, no direct access to the trace. An adapter that wants to
/// know the time asks [`ProxyContext::decide`] what to do instead.
pub struct ProxyContext {
    scheduler: Scheduler,
    forks: Forks,
    connections: AtomicU64,
    /// Where the real dependency is, as `host:port`.
    pub upstream: String,
    pub events: EventSink,
    pub cancel: CancellationToken,
}

impl ProxyContext {
    pub fn new(
        scheduler: Scheduler,
        upstream: impl Into<String>,
        events: EventSink,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            scheduler,
            forks: Forks::default(),
            connections: AtomicU64::new(0),
            upstream: upstream.into(),
            events,
            cancel,
        }
    }

    /// The next connection, numbered in the order it was accepted.
    ///
    /// Shared machinery rather than a counter in each adapter, because the
    /// number is half of what a fork is looked up by on replay. Not a race
    /// despite the atomic: an adapter numbers connections in its accept loop,
    /// which is one task, and an adapter that numbered them anywhere else would
    /// have made the ordering the OS's to decide.
    pub fn next_connection(&self) -> ConnectionId {
        ConnectionId(self.connections.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Reaches a fork and takes the answer.
    ///
    /// The only way an adapter is allowed to branch on anything that is not
    /// determined by the bytes it just read. `detail` is for the reproducer and
    /// never participates in matching a decision on replay, so it is free to
    /// carry an order id that will differ next run.
    pub fn decide(
        &self,
        kind: PointKind,
        connection: ConnectionId,
        detail: impl Into<String>,
    ) -> Decision {
        let ordinal = self.forks.next(kind, connection);
        let point = DecisionPoint::new(kind, connection, ordinal).with_detail(detail);

        self.scheduler.decide(point)
    }

    /// Since the run started. The one clock an adapter may read.
    pub fn elapsed(&self) -> std::time::Duration {
        self.scheduler.elapsed()
    }

    /// Emits an observation, timestamped from the run clock.
    pub fn observe(&self, connection: ConnectionId, event: Event) {
        self.events.emit(self.elapsed(), connection, event);
    }

    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }
}

/// One protocol misorder can sit in front of.
///
/// Adding one is the intended contribution path, and it is deliberately a small
/// surface: bind, accept, speak the protocol, ask before branching. Every
/// adapter stays open and unconditional, because the long tail of vendors is
/// only ever covered by people who needed one.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// `"nats"`, `"postgres"`, `"http"`.
    fn protocol(&self) -> &'static str;

    /// Binds a listener and reports where the service should be pointed.
    async fn bind(&mut self, upstream: &str) -> Result<Endpoint>;

    /// Serves until the token is cancelled.
    async fn serve(&mut self, context: ProxyContext) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Lifecycle;

    #[test]
    fn ordinals_are_per_kind_and_per_connection() {
        let forks = Forks::default();

        assert_eq!(forks.next(PointKind::Ack, ConnectionId(1)), 0);
        assert_eq!(forks.next(PointKind::Ack, ConnectionId(1)), 1);
        assert_eq!(
            forks.next(PointKind::Ack, ConnectionId(2)),
            0,
            "a second connection starts its own sequence"
        );
        assert_eq!(
            forks.next(PointKind::Deliver, ConnectionId(1)),
            0,
            "a different kind starts its own sequence"
        );
    }

    #[tokio::test]
    async fn emitted_events_carry_their_connection() {
        let (sink, mut receiver) = EventSink::new();

        sink.emit(
            std::time::Duration::from_millis(4),
            ConnectionId(7),
            Event::Lifecycle(Lifecycle::SystemReady),
        );

        let observed = receiver.recv().await.expect("event");

        assert_eq!(observed.connection, Some(ConnectionId(7)));
    }

    #[tokio::test]
    async fn emitting_after_the_run_ends_does_not_unwind_the_adapter() {
        let (sink, receiver) = EventSink::new();
        drop(receiver);

        sink.emit(
            std::time::Duration::ZERO,
            ConnectionId(1),
            Event::Lifecycle(Lifecycle::SystemReady),
        );
    }

    #[test]
    fn deciding_through_the_context_records_and_numbers_the_fork() {
        let scheduler = Scheduler::seeded(1, vec![], Default::default(), "s");
        let (events, _receiver) = EventSink::new();
        let context = ProxyContext::new(
            scheduler,
            "127.0.0.1:4222",
            events,
            CancellationToken::new(),
        );

        context.decide(PointKind::Ack, ConnectionId(1), "ledger.order");
        context.decide(PointKind::Ack, ConnectionId(1), "ledger.order");

        let trace = context.scheduler().trace();

        assert_eq!(
            trace
                .records
                .iter()
                .map(|r| r.point.key.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}

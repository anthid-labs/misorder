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

/// What the proxies have seen, for readiness.
///
/// `ready_when = "first_connection"` and `"nats_subscription_active"` are
/// detected from traffic crossing a proxy, and a proxy is the only thing that
/// sees it. The alternative would be polling the service's port, which says
/// nothing: a process that is listening has not necessarily attached its
/// durable, and publishing the workload at one that has not is a failure that
/// is entirely the harness's fault.
///
/// A `watch` rather than a `Notify` because the signal usually arrives *before*
/// anything waits for it: the service connects while the run loop is still
/// starting it. A notification with no waiter is lost, and the run would then
/// wait out its whole `ready_timeout` for something that already happened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Signals {
    /// A connection was accepted by any proxy.
    pub connected: bool,
    /// A NATS `SUB` crossed a proxy.
    pub subscribed: bool,
}

/// Shared between every proxy and the run loop.
#[derive(Debug, Clone)]
pub struct Readiness {
    sender: std::sync::Arc<tokio::sync::watch::Sender<Signals>>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new()
    }
}

impl Readiness {
    pub fn new() -> Self {
        let (sender, _receiver) = tokio::sync::watch::channel(Signals::default());

        Self {
            sender: std::sync::Arc::new(sender),
        }
    }

    /// A connection reached a proxy.
    pub fn connected(&self) {
        self.sender.send_if_modified(|signals| {
            let changed = !signals.connected;
            signals.connected = true;
            changed
        });
    }

    /// A subscription crossed a proxy.
    ///
    /// Implies a connection, because one had to be accepted to carry it. Set
    /// together so a scenario asking for the weaker signal is never left
    /// waiting by an adapter that only reported the stronger one.
    pub fn subscribed(&self) {
        self.sender.send_if_modified(|signals| {
            let changed = !signals.subscribed || !signals.connected;
            signals.subscribed = true;
            signals.connected = true;
            changed
        });
    }

    pub fn signals(&self) -> Signals {
        *self.sender.borrow()
    }

    /// Waits for the signal a readiness mode is defined by.
    ///
    /// An expiry is [`crate::error::Error::Environment`] rather than a finding: a service
    /// that never came up is the run's own fault, and reporting it as an
    /// invariant violation would be an invented failure. Those cost more trust
    /// than several missed real ones.
    pub async fn wait(
        &self,
        ready: crate::scenario::file::Ready,
        timeout: std::time::Duration,
    ) -> crate::error::Result<()> {
        use crate::scenario::file::Ready;

        let mut receiver = self.sender.subscribe();

        let waiting = async {
            let matched = match ready {
                Ready::FirstConnection => receiver.wait_for(|signals| signals.connected).await,
                Ready::NatsSubscriptionActive => {
                    receiver.wait_for(|signals| signals.subscribed).await
                }
                // No adapter reports it, because the Postgres codec is not
                // written. Named rather than left to time out, so the reader is
                // sent to the gap instead of to their own service.
                Ready::PostgresConnected => {
                    return Err(crate::error::Error::Unsupported(
                        "`ready_when = \"postgres_connected\"` needs the Postgres adapter, and \
                         its wire codec is not written yet"
                            .to_string(),
                    ));
                }
                other => {
                    return Err(crate::error::Error::Internal(format!(
                        "`{other}` is not detected from proxy traffic and should not have \
                         reached here"
                    )));
                }
            };

            matched.map(|_| ()).map_err(|_| {
                crate::error::Error::Internal(
                    "every proxy stopped before the service was ready".to_string(),
                )
            })
        };

        match tokio::time::timeout(timeout, waiting).await {
            Ok(result) => result,
            Err(_) => Err(crate::error::Error::Environment(format!(
                "the service was not ready within {timeout:?}: nothing matching \
                 `ready_when = \"{ready}\"` crossed a proxy"
            ))),
        }
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
    /// Where readiness is reported. Default is a handle nobody waits on, so an
    /// adapter under test needs no extra wiring.
    readiness: Readiness,
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
            readiness: Readiness::new(),
        }
    }

    /// Reports readiness into the run loop's handle rather than a private one.
    pub fn with_readiness(mut self, readiness: Readiness) -> Self {
        self.readiness = readiness;
        self
    }

    /// What this proxy has seen, for the run loop to wait on.
    pub fn readiness(&self) -> &Readiness {
        &self.readiness
    }

    /// The next connection, numbered in the order it was accepted.
    ///
    /// Shared machinery rather than a counter in each adapter, because the
    /// number is half of what a fork is looked up by on replay. Not a race
    /// despite the atomic: an adapter numbers connections in its accept loop,
    /// which is one task, and an adapter that numbered them anywhere else would
    /// have made the ordering the OS's to decide.
    pub fn next_connection(&self) -> ConnectionId {
        // Every adapter numbers connections here, so reporting from this one
        // place is what makes `first_connection` work for all of them without
        // a line in each.
        self.readiness.connected();

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

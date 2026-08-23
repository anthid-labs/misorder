//! What the proxies saw.
//!
//! One vocabulary, shared by everything downstream of [`proxy`](crate::proxy).
//! Adapters emit these; invariants consume them; the reproducer is rendered
//! from them. An adapter that kept its observations to itself would force every
//! invariant to know which adapter it was talking to, and the built-in
//! invariants exist precisely so a first-time user gets a caught bug without
//! knowing that.
//!
//! Events are an *observation* stream, not the decision stream. Decisions are
//! in [`trace`](crate::trace) and are the thing that replays. Events are what
//! resulted, and are not replayed: they are re-derived on every run, which is
//! how a replay proves it reproduced the same failure rather than asserting it.

use std::time::Duration;

use bytes::Bytes;

/// One proxied connection, numbered in the order it was accepted.
///
/// Stable across a replay: connections are accepted in a fixed order because
/// the workload that opens them is itself scheduled. If that ever stops being
/// true, replay breaks, and this is the type whose comment should say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn:{}", self.0)
    }
}

/// An observation, with when it happened and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    /// Since the run started. Wall clock in Phase 1; the virtual clock will
    /// replace the source without changing this type.
    pub at: Duration,

    /// Which proxied connection this came from, or `None` for harness
    /// lifecycle.
    ///
    /// On the wrapper rather than repeated in every variant, because every
    /// invariant that tracks per-connection state needs it and none of them
    /// should have to match on the event kind to find it. `no_commit_after_error`
    /// is meaningless without it: an error on one connection says nothing about
    /// a commit on another, and conflating the two would report a violation
    /// every time a pool had two connections.
    pub connection: Option<ConnectionId>,

    pub event: Event,
}

impl Observed {
    /// A harness observation, belonging to no connection.
    pub fn new(at: Duration, event: Event) -> Self {
        Self {
            at,
            connection: None,
            event,
        }
    }

    /// An observation from a proxied connection.
    pub fn on(at: Duration, connection: ConnectionId, event: Event) -> Self {
        Self {
            at,
            connection: Some(connection),
            event,
        }
    }
}

/// Anything a proxy or the harness itself saw happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Nats(NatsEvent),
    Postgres(PostgresEvent),
    Redis(RedisEvent),
    Http(HttpEvent),
    Lifecycle(Lifecycle),
}

impl Event {
    /// Which dependency this came from, or `None` for harness lifecycle.
    ///
    /// Used by the reproducer to say "Postgres was not involved", which is
    /// worth as much as the six events that were: it tells the reader which
    /// half of their system they can stop reading.
    pub fn dependency(&self) -> Option<&'static str> {
        match self {
            Event::Nats(_) => Some("nats"),
            Event::Postgres(_) => Some("postgres"),
            Event::Redis(_) => Some("redis"),
            Event::Http(_) => Some("http"),
            Event::Lifecycle(_) => None,
        }
    }
}

/// Redis, over RESP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisEvent {
    /// A command that actually reached the server.
    ///
    /// Only once it has been written upstream. A command the schedule dropped
    /// is never observed at all, for the same reason a dropped HTTP request is
    /// not: `every_command_gets_a_reply` would otherwise report the harness's
    /// own fault as the service's.
    Command {
        /// Upper-cased, so an invariant matches `SET` without caring that the
        /// client sent `set`. Redis command names are case-insensitive and
        /// clients disagree about which case they use.
        name: String,
        /// The arguments as sent, first one usually the key.
        args: Vec<Bytes>,
        /// Which command this was in the order the client sent them on this
        /// connection, counting from zero.
        ///
        /// Emission order is the order the server saw them, so this and that
        /// are the two halves of a reordering. Without it a pipeline of six
        /// `GET`s says nothing about which one moved.
        order: u64,
    },
    /// A reply the server actually produced.
    Reply {
        /// A `-ERR` style error reply. Kept as a flag rather than a variant
        /// because everything else about an error reply is the same shape.
        error: bool,
        /// The scalar payload for the kinds that have one - simple string,
        /// error, integer, bulk string. `None` for arrays, maps and nulls,
        /// which no built-in reads.
        value: Option<Bytes>,
    },
    ConnectionClosed,
}

/// NATS and JetStream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NatsEvent {
    Published {
        subject: String,
        payload: Bytes,
    },
    /// A message handed to a consumer.
    ///
    /// `num_delivered` is the server's own count and not one misorder keeps.
    /// The distinction matters: `max_deliver` is enforced by the server, so an
    /// invariant that counted deliveries itself would be checking misorder's
    /// bookkeeping against misorder's bookkeeping.
    Delivered {
        subject: String,
        consumer: String,
        num_delivered: u32,
        payload: Bytes,
    },
    Acked {
        consumer: String,
        subject: String,
    },
    Nacked {
        consumer: String,
        subject: String,
    },
    /// The consumer gave up on this message: `+TERM`, or `max_deliver` reached.
    Terminated {
        consumer: String,
        subject: String,
        reason: TerminalReason,
    },
    /// Advisory: a message went to the dead letter subject.
    DeadLettered {
        subject: String,
        origin_subject: String,
    },
    /// Recorded at startup from the applied topology, so an invariant can
    /// compare a consumer's filter against the stream's dead letter subject
    /// without re-reading the scenario.
    ConsumerConfigured {
        consumer: String,
        filter_subject: String,
        max_deliver: u32,
        ack_wait: Duration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalReason {
    MaxDeliverReached,
    Terminated,
}

/// Postgres, at statement granularity.
///
/// Statements carry their text and not a hash. A reproducer that says
/// "statement 7f3a2c" is unreadable, and the text is what the reader needs to
/// recognise their own code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresEvent {
    Connected {
        database: String,
    },
    Begin,
    Statement {
        sql: String,
    },
    Commit,
    Rollback,
    /// `code` is the five-character SQLSTATE. `40001` is a serialization
    /// failure, which is the one this whole adapter exists to provoke.
    Error {
        code: String,
        message: String,
    },
    Disconnected,
}

/// HTTP, which is one adapter covering Stripe, Plaid and a thousand REST
/// vendors at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpEvent {
    Request {
        method: String,
        path: String,
        /// Whatever the vendor calls it. Carried separately because the
        /// built-in idempotency invariant is the reason this adapter pays for
        /// itself on day one.
        idempotency_key: Option<String>,
        body: Bytes,
        /// Which request this was in the order the client *sent* them,
        /// counting from zero on this connection.
        ///
        /// Events are emitted in the order requests reached the service, so
        /// this and the emission order are the two halves of "what the client
        /// sent" versus "what the service saw". A reordering is the two
        /// disagreeing, and without this there is nothing to disagree with:
        /// six identical `POST /webhooks/stripe` lines say nothing about
        /// which one moved.
        order: u64,
    },
    Response {
        status: u16,
        body: Bytes,
    },
    ConnectionClosed,
}

/// The harness, rather than any dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lifecycle {
    SystemStarted {
        command: String,
    },
    SystemReady,
    SystemExited {
        code: Option<i32>,
    },
    WorkloadComplete,
    /// Nothing in flight and nothing scheduled.
    ///
    /// Phase 1 infers this from an idle window with no proxied traffic. That is
    /// a heuristic, and it is deliberately conservative: calling quiescence
    /// early manufactures a failure that never happened, which costs more trust
    /// than missing a real one.
    Quiescent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_report_the_dependency_they_came_from() {
        let event = Event::Postgres(PostgresEvent::Commit);

        assert_eq!(event.dependency(), Some("postgres"));
    }

    #[test]
    fn lifecycle_belongs_to_no_dependency() {
        let event = Event::Lifecycle(Lifecycle::Quiescent);

        assert_eq!(event.dependency(), None);
    }

    #[test]
    fn connection_ids_render_for_the_reproducer() {
        assert_eq!(ConnectionId(3).to_string(), "conn:3");
    }
}

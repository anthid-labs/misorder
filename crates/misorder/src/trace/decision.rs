//! What could have gone another way, and which way it went.
//!
//! A [`DecisionPoint`] is a fork the proxy reached. A [`Decision`] is the
//! answer it got. Together they are the entire nondeterministic content of a
//! run: reproduce the answers and you reproduce the run.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::event::ConnectionId;
use crate::schedule::FaultKind;

/// The class of fork. Also what a decision is looked up by on replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PointKind {
    /// A message is ready to hand on to its recipient.
    Deliver,
    /// An acknowledgement is crossing the proxy.
    Ack,
    /// A connection is open, and could stop being open.
    Connection,
    /// A statement is ready to go upstream. Holding it here is what forces an
    /// exact transaction interleaving.
    Statement,
    /// A reply is ready to go back.
    Response,
}

impl PointKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PointKind::Deliver => "deliver",
            PointKind::Ack => "ack",
            PointKind::Connection => "connection",
            PointKind::Statement => "statement",
            PointKind::Response => "response",
        }
    }

    /// What the thing at this fork is called in a sentence.
    ///
    /// Separate from [`PointKind::as_str`], which is the wire name. A
    /// reproducer line reads "drop ack", not "drop Ack", and the two happen to
    /// differ for `Deliver`: the fork is a delivery, the verb is to deliver.
    pub fn noun(self) -> &'static str {
        match self {
            PointKind::Deliver => "delivery",
            PointKind::Ack => "ack",
            PointKind::Connection => "connection",
            PointKind::Statement => "statement",
            PointKind::Response => "response",
        }
    }
}

/// The identity of a fork, stable across runs.
///
/// This is the part that makes shrinking sound, and it is worth being precise
/// about why. Shrinking replaces decision *N* with the neutral choice and
/// replays. That changes the run: a connection that was dropped now survives,
/// so later forks arrive that did not arrive before, and forks that did arrive
/// may not. If a decision were looked up by its position in the trace, every
/// decision after the removed one would be misapplied to a different fork, and
/// the replay would reproduce something unrelated while claiming success.
///
/// So the key is `(kind, connection, ordinal)`, where `ordinal` counts prior
/// forks of the same kind on the same connection. Removing a fault elsewhere
/// leaves those numbers alone. A fork with no recorded decision takes
/// [`Decision::NEUTRAL`], which is exactly what "this fault was not needed"
/// should mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PointKey {
    pub kind: PointKind,
    pub connection: u64,
    pub ordinal: u64,
}

/// A fork, with the context needed to describe it to a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPoint {
    #[serde(flatten)]
    pub key: PointKey,

    /// What this fork is about: a subject, a statement, a request path.
    ///
    /// Never consulted when matching a decision on replay, only when printing
    /// one. Matching on it would make a reproducer break the moment an order id
    /// changed, and the reproducer is the artifact that has to survive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl DecisionPoint {
    pub fn new(kind: PointKind, connection: ConnectionId, ordinal: u64) -> Self {
        Self {
            key: PointKey {
                kind,
                connection: connection.0,
                ordinal,
            },
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn connection(&self) -> ConnectionId {
        ConnectionId(self.key.connection)
    }
}

/// The answer to a fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "do", rename_all = "snake_case")]
pub enum Decision {
    /// Pass it on, after `delay`. A zero delay is the neutral choice.
    Deliver {
        #[serde(rename = "delay_ms", with = "crate::trace::duration_ms")]
        delay: Duration,
    },
    /// Do not pass it on at all. The recipient never learns it existed.
    Drop,
    /// Let the fork at `ahead_of` on this connection go first.
    Reorder { ahead_of: u64 },
    /// Close the connection now, mid-conversation.
    CloseConnection,
    /// Flip a byte. Rarely useful, occasionally the only thing that reproduces
    /// a vendor's parser bug.
    Corrupt { offset: usize },
    /// Hold this until the fork at `until` on this connection completes.
    ///
    /// The Postgres case: hold statement B until statement A commits. This is
    /// what a proxy can do that a simulator would have to reimplement isolation
    /// semantics to fake.
    Hold { until: u64 },
}

impl Decision {
    /// The choice that adds nothing: deliver immediately.
    ///
    /// Every fork has one, and that is not an accident of the design, it is the
    /// requirement that makes shrinking possible. Removing a decision has to
    /// mean something, and what it means is "this fork took the boring path".
    pub const NEUTRAL: Decision = Decision::Deliver {
        delay: Duration::ZERO,
    };

    /// Whether this decision perturbs the run at all.
    pub fn is_neutral(&self) -> bool {
        matches!(self, Decision::Deliver { delay } if delay.is_zero())
    }

    /// Which permitted fault produced this, for the "faults not required" line
    /// of a reproducer. `None` for the neutral choice.
    pub fn fault_kind(&self) -> Option<FaultKind> {
        match self {
            Decision::Deliver { delay } if delay.is_zero() => None,
            Decision::Deliver { .. } => Some(FaultKind::Delay),
            Decision::Drop => Some(FaultKind::SwallowAck),
            Decision::Reorder { .. } => Some(FaultKind::Reorder),
            Decision::CloseConnection => Some(FaultKind::ConnectionDrop),
            Decision::Corrupt { .. } => Some(FaultKind::CorruptFrame),
            Decision::Hold { .. } => Some(FaultKind::HoldStatement),
        }
    }
}

impl Decision {
    /// This decision as a phrase, given what it was applied to.
    ///
    /// The reproducer is the product, so its lines have to read as English at
    /// 3am. Composing `Display` with the fork kind gives "deliver deliver" and
    /// "drop ack", one of which is nonsense; this picks the verb that matches.
    pub fn describe(&self, kind: PointKind) -> String {
        let noun = kind.noun();

        match self {
            Decision::Deliver { delay } if delay.is_zero() => format!("allow {noun}"),
            Decision::Deliver { delay } => {
                format!("delay {noun} by {}ms", delay.as_millis())
            }
            Decision::Drop => format!("drop {noun}"),
            Decision::Reorder { ahead_of } => format!("reorder {noun} behind #{ahead_of}"),
            Decision::CloseConnection => "close connection".to_string(),
            Decision::Corrupt { offset } => format!("corrupt {noun} at byte {offset}"),
            Decision::Hold { until } => format!("hold {noun} until #{until}"),
        }
    }
}

impl std::fmt::Display for Decision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decision::Deliver { delay } if delay.is_zero() => write!(f, "deliver"),
            Decision::Deliver { delay } => write!(f, "deliver after {}ms", delay.as_millis()),
            Decision::Drop => write!(f, "drop"),
            Decision::Reorder { ahead_of } => write!(f, "reorder behind #{ahead_of}"),
            Decision::CloseConnection => write!(f, "close connection"),
            Decision::Corrupt { offset } => write!(f, "corrupt byte {offset}"),
            Decision::Hold { until } => write!(f, "hold until #{until}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_is_an_immediate_delivery() {
        assert!(Decision::NEUTRAL.is_neutral());
        assert!(Decision::NEUTRAL.fault_kind().is_none());
    }

    #[test]
    fn a_delayed_delivery_is_not_neutral() {
        let decision = Decision::Deliver {
            delay: Duration::from_millis(40),
        };

        assert!(!decision.is_neutral());
        assert_eq!(decision.fault_kind(), Some(FaultKind::Delay));
    }

    #[test]
    fn detail_never_participates_in_identity() {
        let bare = DecisionPoint::new(PointKind::Deliver, ConnectionId(1), 0);
        let annotated = bare.clone().with_detail("ledger.order");

        assert_eq!(bare.key, annotated.key);
    }

    #[test]
    fn a_decision_reads_as_english_against_its_fork() {
        assert_eq!(Decision::Drop.describe(PointKind::Ack), "drop ack");
        assert_eq!(Decision::Drop.describe(PointKind::Deliver), "drop delivery");
        assert_eq!(
            Decision::Deliver {
                delay: Duration::from_millis(40)
            }
            .describe(PointKind::Deliver),
            "delay delivery by 40ms"
        );
        assert_eq!(
            Decision::Hold { until: 3 }.describe(PointKind::Statement),
            "hold statement until #3"
        );
        assert_eq!(
            Decision::CloseConnection.describe(PointKind::Connection),
            "close connection"
        );
    }

    #[test]
    fn decisions_round_trip_through_json() {
        for decision in [
            Decision::NEUTRAL,
            Decision::Drop,
            Decision::Reorder { ahead_of: 4 },
            Decision::CloseConnection,
            Decision::Corrupt { offset: 12 },
            Decision::Hold { until: 2 },
            Decision::Deliver {
                delay: Duration::from_millis(40),
            },
        ] {
            let encoded = serde_json::to_string(&decision).expect("serialize");
            let decoded: Decision = serde_json::from_str(&encoded).expect("deserialize");

            assert_eq!(decision, decoded, "round trip failed for {decision}");
        }
    }
}

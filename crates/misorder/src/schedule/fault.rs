//! The faults a scenario may permit.
//!
//! Named in the scenario file, so these are the vocabulary a user learns:
//!
//! ```toml
//! [faults]
//! enabled = ["ack_timeout", "redelivery", "connection_drop", "reorder"]
//! ```
//!
//! Nothing happens that is not listed. A scenario that permits no faults still
//! runs, and still checks its invariants, which is the honest baseline: if it
//! fails with an empty `enabled`, the bug was never about timing.

use serde::{Deserialize, Serialize};

use crate::trace::PointKind;

/// One class of thing the proxy is allowed to do to a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    /// Hold an acknowledgement long enough that the server's own `ack_wait`
    /// expires first.
    ///
    /// Distinct from [`FaultKind::SwallowAck`], and the difference is the whole
    /// point: the ack eventually arrives, at a server that has already given up
    /// on the message and redelivered it. That race is where the duplicate
    /// processing lives.
    AckTimeout,

    /// Drop a delivery so the server sends it again.
    ///
    /// The counterpart to `AckTimeout`: this loses the message on the way out,
    /// that one loses the receipt on the way back.
    Redelivery,

    /// Drop an acknowledgement outright. The server never learns the message
    /// was handled.
    SwallowAck,

    /// Close a connection mid-conversation, in either direction.
    ConnectionDrop,

    /// Let a later in-flight message overtake an earlier one.
    Reorder,

    /// Delay a message without losing it.
    Delay,

    /// Flip a byte in a frame.
    ///
    /// Rarely the cause of anything, and kept because when a vendor's framing
    /// is wrong it is the only fault that finds it.
    CorruptFrame,

    /// Hold one statement until another completes.
    ///
    /// Postgres-shaped, and the reason the proxy is permanent rather than a
    /// stepping stone to a simulator: forcing an exact interleaving against a
    /// real server gives real serialization failures and real isolation
    /// semantics, which no simulator reproduces by accident.
    HoldStatement,
}

impl FaultKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FaultKind::AckTimeout => "ack_timeout",
            FaultKind::Redelivery => "redelivery",
            FaultKind::SwallowAck => "swallow_ack",
            FaultKind::ConnectionDrop => "connection_drop",
            FaultKind::Reorder => "reorder",
            FaultKind::Delay => "delay",
            FaultKind::CorruptFrame => "corrupt_frame",
            FaultKind::HoldStatement => "hold_statement",
        }
    }

    /// Every fault, for `mis check` to print and for an unrecognised name in a
    /// scenario to be reported against.
    pub const ALL: [FaultKind; 8] = [
        FaultKind::AckTimeout,
        FaultKind::Redelivery,
        FaultKind::SwallowAck,
        FaultKind::ConnectionDrop,
        FaultKind::Reorder,
        FaultKind::Delay,
        FaultKind::CorruptFrame,
        FaultKind::HoldStatement,
    ];

    /// Whether this fault can apply at this kind of fork.
    ///
    /// A table rather than a guess at each call site. Getting it wrong in the
    /// permissive direction produces a decision the adapter cannot carry out,
    /// which is a recorded fault that did not happen: the worst possible
    /// outcome, because the trace then describes a run nobody had.
    pub fn applies_at(self, point: PointKind) -> bool {
        match self {
            FaultKind::AckTimeout | FaultKind::SwallowAck => point == PointKind::Ack,
            FaultKind::Redelivery => point == PointKind::Deliver,
            FaultKind::ConnectionDrop => matches!(
                point,
                PointKind::Connection
                    | PointKind::Deliver
                    | PointKind::Statement
                    | PointKind::Response
            ),
            FaultKind::Reorder => matches!(point, PointKind::Deliver | PointKind::Response),
            FaultKind::Delay => point != PointKind::Connection,
            FaultKind::CorruptFrame => matches!(point, PointKind::Deliver | PointKind::Response),
            FaultKind::HoldStatement => point == PointKind::Statement,
        }
    }
}

impl std::fmt::Display for FaultKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_faults_only_apply_to_acks() {
        assert!(FaultKind::SwallowAck.applies_at(PointKind::Ack));
        assert!(!FaultKind::SwallowAck.applies_at(PointKind::Deliver));
    }

    #[test]
    fn holding_a_statement_is_postgres_shaped_only() {
        assert!(FaultKind::HoldStatement.applies_at(PointKind::Statement));
        assert!(!FaultKind::HoldStatement.applies_at(PointKind::Deliver));
    }

    #[test]
    fn every_fault_applies_somewhere() {
        let points = [
            PointKind::Deliver,
            PointKind::Ack,
            PointKind::Connection,
            PointKind::Statement,
            PointKind::Response,
        ];

        for fault in FaultKind::ALL {
            assert!(
                points.iter().any(|point| fault.applies_at(*point)),
                "{fault} can never fire"
            );
        }
    }

    #[test]
    fn names_round_trip_through_toml() {
        for fault in FaultKind::ALL {
            let encoded = format!("v = \"{}\"", fault.as_str());
            let decoded: toml::Value = toml::from_str(&encoded).expect("parse");
            let parsed: FaultKind =
                decoded["v"].clone().try_into().expect("as_str matches serde");

            assert_eq!(fault, parsed);
        }
    }
}

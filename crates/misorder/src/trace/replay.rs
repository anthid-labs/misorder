//! Answering forks from a recorded trace instead of from a PRNG.
//!
//! Replay is not a separate execution mode. It is the same run with a different
//! [`DecisionSource`](crate::schedule::DecisionSource) plugged in, which is
//! what makes it trustworthy: if replay had its own code path, the thing it
//! reproduced would be that path and not the original run.
//!
//! The shrinker uses this too. "Remove decision N" is "replay this trace with
//! N neutralised", so shrinking needs no machinery of its own beyond the
//! decision to remove.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::schedule::DecisionSource;
use crate::trace::{Decision, DecisionPoint, PointKey, Trace};

/// Replays the decisions in a trace, and reports where the run diverged from
/// it.
///
/// Divergence is expected during shrinking and suspicious during a plain
/// replay, so it is reported rather than judged:
///
/// - [`Replay::unmatched`] is forks the run reached that the trace has nothing
///   for. They took [`Decision::NEUTRAL`]. During shrinking this is normal: a
///   removed fault means a connection survives and reaches forks the original
///   run never got to.
/// - [`Replay::unused`] is decisions in the trace the run never reached. A
///   plain replay with unused decisions did not follow the recorded path, and
///   whatever it proved is about a different run.
#[derive(Debug)]
pub struct Replay {
    decisions: HashMap<PointKey, Decision>,
    seen: Mutex<Divergence>,
}

#[derive(Debug, Default)]
struct Divergence {
    unmatched: Vec<PointKey>,
    used: HashSet<PointKey>,
}

impl Replay {
    pub fn new(trace: &Trace) -> Self {
        let decisions = trace
            .records
            .iter()
            .map(|record| (record.point.key, record.decision))
            .collect();

        Self {
            decisions,
            seen: Mutex::new(Divergence::default()),
        }
    }

    /// Forks the run reached that the trace does not describe.
    pub fn unmatched(&self) -> Vec<PointKey> {
        self.seen.lock().expect("replay mutex poisoned").unmatched.clone()
    }

    /// Decisions in the trace the run never reached.
    pub fn unused(&self) -> Vec<PointKey> {
        let seen = self.seen.lock().expect("replay mutex poisoned");

        self.decisions
            .keys()
            .filter(|key| !seen.used.contains(*key))
            .copied()
            .collect()
    }

    /// Whether the run followed the trace exactly.
    pub fn is_faithful(&self) -> bool {
        self.unmatched().is_empty() && self.unused().is_empty()
    }
}

impl DecisionSource for Replay {
    fn decide(&self, point: &DecisionPoint) -> Decision {
        let mut seen = self.seen.lock().expect("replay mutex poisoned");

        match self.decisions.get(&point.key) {
            Some(decision) => {
                seen.used.insert(point.key);
                *decision
            }
            None => {
                seen.unmatched.push(point.key);
                Decision::NEUTRAL
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionId;
    use crate::trace::{PointKind, Record};
    use std::time::Duration;

    fn trace_with(decisions: &[(u64, Decision)]) -> Trace {
        let mut trace = Trace::new(1, "s");

        for (seq, (ordinal, decision)) in decisions.iter().enumerate() {
            trace.records.push(Record {
                seq: seq as u64,
                at: Duration::from_millis(seq as u64),
                point: DecisionPoint::new(PointKind::Ack, ConnectionId(1), *ordinal),
                decision: *decision,
            });
        }

        trace
    }

    #[test]
    fn a_recorded_fork_gets_its_recorded_decision() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop)]));
        let point = DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0);

        assert_eq!(replay.decide(&point), Decision::Drop);
        assert!(replay.is_faithful());
    }

    #[test]
    fn an_unrecorded_fork_takes_the_neutral_choice_and_is_reported() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop)]));

        replay.decide(&DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0));
        let extra = DecisionPoint::new(PointKind::Ack, ConnectionId(1), 9);

        assert_eq!(replay.decide(&extra), Decision::NEUTRAL);
        assert_eq!(replay.unmatched(), vec![extra.key]);
        assert!(!replay.is_faithful());
    }

    #[test]
    fn a_decision_the_run_never_reached_is_reported_as_unused() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop), (1, Decision::Drop)]));

        replay.decide(&DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0));

        assert_eq!(replay.unused().len(), 1);
        assert!(!replay.is_faithful());
    }

    #[test]
    fn identity_ignores_detail_so_a_reproducer_survives_changed_ids() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop)]));

        let point = DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0)
            .with_detail("ledger.org.org_9.account.acct_9.order");

        assert_eq!(replay.decide(&point), Decision::Drop);
    }
}

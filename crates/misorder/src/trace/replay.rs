//! Answering forks from a recorded trace instead of from a PRNG.
//!
//! Replay is not a separate execution mode. It is the same run with a different
//! [`DecisionSource`] plugged in, which is
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

/// Where a run and the trace it was replaying disagree.
///
/// Divergence is expected during shrinking and suspicious during a plain
/// replay, so it is reported rather than judged. Who acts on it is the caller's
/// decision, and the two callers want opposite things: the shrinker replays
/// candidates it has deliberately altered, and `mis replay` replays a trace
/// that is supposed to describe the run exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Divergence {
    /// Forks the run reached that the trace has nothing for.
    ///
    /// They took [`Decision::NEUTRAL`]. During shrinking this is normal and is
    /// most of what shrinking does: a removed fault means a connection is no
    /// longer closed, survives, and reaches forks the original run never got
    /// to.
    pub unmatched: Vec<PointKey>,

    /// Decisions the trace held that the run never reached.
    ///
    /// The stronger of the two signals, and the one that means the same thing
    /// wherever it turns up: the run went somewhere the recording did not, so
    /// whatever it proved is about a different run.
    pub unused: Vec<PointKey>,
}

impl Divergence {
    /// Whether the run followed the trace exactly.
    pub fn is_empty(&self) -> bool {
        self.unmatched.is_empty() && self.unused.is_empty()
    }

    /// How many forks disagree, either way.
    pub fn len(&self) -> usize {
        self.unmatched.len() + self.unused.len()
    }
}

/// Replays the decisions in a trace, and reports where the run diverged from
/// it.
#[derive(Debug)]
pub struct Replay {
    decisions: HashMap<PointKey, Decision>,
    seen: Mutex<Seen>,
}

#[derive(Debug, Default)]
struct Seen {
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
            seen: Mutex::new(Seen::default()),
        }
    }

    /// Forks the run reached that the trace does not describe.
    ///
    /// In the order they were reached, which is the order they happened in.
    pub fn unmatched(&self) -> Vec<PointKey> {
        self.seen
            .lock()
            .expect("replay mutex poisoned")
            .unmatched
            .clone()
    }

    /// Decisions in the trace the run never reached.
    ///
    /// Sorted, because this comes out of a `HashMap` and a report that listed
    /// the same divergence in a different order on every run could not be
    /// diffed against itself.
    pub fn unused(&self) -> Vec<PointKey> {
        let seen = self.seen.lock().expect("replay mutex poisoned");

        let mut unused: Vec<PointKey> = self
            .decisions
            .keys()
            .filter(|key| !seen.used.contains(*key))
            .copied()
            .collect();

        unused.sort();

        unused
    }

    /// Whether the run followed the trace exactly.
    pub fn is_faithful(&self) -> bool {
        self.unmatched().is_empty() && self.unused().is_empty()
    }
}

impl DecisionSource for Replay {
    fn divergence(&self) -> Option<Divergence> {
        Some(Divergence {
            unmatched: self.unmatched(),
            unused: self.unused(),
        })
    }

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

    /// The two halves reach a caller through one value, because acting on one
    /// without the other is how a replay gets called faithful for having
    /// reached no forks at all.
    #[test]
    fn divergence_reports_both_halves_through_the_source() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop), (1, Decision::Drop)]));

        replay.decide(&DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0));
        replay.decide(&DecisionPoint::new(PointKind::Ack, ConnectionId(1), 9));

        let divergence = DecisionSource::divergence(&replay).expect("a trace can be departed from");

        assert_eq!(divergence.unused.len(), 1, "ordinal 1 was never reached");
        assert_eq!(divergence.unmatched.len(), 1, "ordinal 9 was not recorded");
        assert_eq!(divergence.len(), 2);
        assert!(!divergence.is_empty());
    }

    #[test]
    fn a_faithful_replay_has_nothing_to_report() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop)]));

        replay.decide(&DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0));

        assert!(
            DecisionSource::divergence(&replay)
                .expect("still Some")
                .is_empty(),
            "a replay that followed its trace still reports, and reports nothing"
        );
    }

    /// `unused` comes out of a `HashMap`, so without sorting the same
    /// divergence would print in a different order on every run and two
    /// identical reports could not be diffed.
    #[test]
    fn unused_decisions_are_reported_in_a_stable_order() {
        let replay = Replay::new(&trace_with(&[
            (7, Decision::Drop),
            (2, Decision::Drop),
            (5, Decision::Drop),
        ]));

        let ordinals: Vec<u64> = replay.unused().iter().map(|key| key.ordinal).collect();

        assert_eq!(ordinals, vec![2, 5, 7]);
    }

    #[test]
    fn identity_ignores_detail_so_a_reproducer_survives_changed_ids() {
        let replay = Replay::new(&trace_with(&[(0, Decision::Drop)]));

        let point = DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0)
            .with_detail("ledger.org.org_9.account.acct_9.order");

        assert_eq!(replay.decide(&point), Decision::Drop);
    }
}

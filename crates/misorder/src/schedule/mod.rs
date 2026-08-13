//! Where every nondeterministic choice comes from.
//!
//! One integer in, one schedule out. Seed 8837291 produces the same sequence of
//! delays, drops, reorders and disconnections every time, on any machine, in
//! any thread order. One scenario file plus 10,000 seeds is 10,000 scenarios,
//! which is why Phase 1 needs no separate generation engine to reach that
//! number.
//!
//! # Decisions are a pure function of (seed, fork), not a stream
//!
//! The obvious implementation is one PRNG advanced once per decision. It is
//! also wrong here, and the reason is worth stating because it is the kind of
//! bug that only shows up under load.
//!
//! A run has several proxied connections being served concurrently. If they
//! draw from a shared sequential PRNG, the schedule depends on the order the
//! tasks reach it, which is decided by the OS. Same seed, different machine,
//! different run. Determinism would be a claim rather than a property, and the
//! first time someone's reproducer failed to reproduce, the tool would be over.
//!
//! So there is no stream. Each fork derives its own generator from
//! `(seed, kind, connection, ordinal)`, and ChaCha8 turns that into an
//! independent draw. Concurrency stops mattering, because nothing is shared to
//! race over: [`DecisionSource::decide`] takes `&self` and is genuinely pure.
//!
//! # The rule for adapters
//!
//! Every branch a proxy adapter could take two ways goes through
//! [`Scheduler::decide`]. Not most of them. An adapter that reads the clock,
//! calls `rand`, or lets two futures race in an order it does not control has
//! made the trace an incomplete description of the run, and replay silently
//! becomes a different run wearing the same seed.

pub mod fault;
pub mod seeded;

pub use fault::FaultKind;
pub use seeded::{Profile, Seeded};

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::trace::{Decision, DecisionPoint, Recorder, Trace};

/// Answers forks.
///
/// Two implementations, and they are the two ways a run can happen:
/// [`Seeded`] draws from a seed, [`Replay`](crate::trace::Replay) reads from a
/// recorded trace. Both are `&self`, so neither can smuggle in state that would
/// make the answer depend on when it was asked.
pub trait DecisionSource: Send + Sync {
    fn decide(&self, point: &DecisionPoint) -> Decision;
}

/// The proxies' single entry point for anything that could go two ways.
///
/// Wraps a [`DecisionSource`] and a [`Recorder`], so asking and recording
/// cannot come apart. There is deliberately no way to consult the source
/// without recording the answer: a decision that happened but was not written
/// down is a trace that does not replay, discovered days later by whoever
/// trusted it.
#[derive(Clone)]
pub struct Scheduler {
    source: Arc<dyn DecisionSource>,
    recorder: Recorder,
    started: Instant,
}

impl Scheduler {
    pub fn new(source: Arc<dyn DecisionSource>, recorder: Recorder) -> Self {
        Self {
            source,
            recorder,
            started: Instant::now(),
        }
    }

    /// Draws from a seed.
    pub fn seeded(seed: u64, faults: Vec<FaultKind>, profile: Profile, scenario: &str) -> Self {
        Self::new(
            Arc::new(Seeded::new(seed, faults, profile)),
            Recorder::new(seed, scenario),
        )
    }

    /// Replays a recorded trace.
    ///
    /// The recorder is still attached, so a replay produces a trace of its own.
    /// Comparing it against the original is how a replay proves it followed the
    /// recorded path rather than asserting it did.
    pub fn replaying(trace: &Trace) -> Self {
        Self::new(
            Arc::new(crate::trace::Replay::new(trace)),
            Recorder::new(trace.seed, trace.scenario.clone()),
        )
    }

    /// Answers one fork and records the answer.
    pub fn decide(&self, point: DecisionPoint) -> Decision {
        let decision = self.source.decide(&point);

        self.recorder.record(self.elapsed(), point, decision);

        decision
    }

    /// Since the run started.
    ///
    /// The one clock the proxies read, so that swapping it for a virtual clock
    /// in Phase 3 is a change here and nowhere else. An adapter calling
    /// `Instant::now` itself is the same bug as an adapter calling `rand`.
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Everything decided so far.
    pub fn trace(&self) -> Trace {
        self.recorder.snapshot()
    }

    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("decisions", &self.recorder.snapshot().records.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionId;
    use crate::trace::PointKind;

    #[test]
    fn deciding_records_the_decision() {
        let scheduler = Scheduler::seeded(1, vec![], Profile::default(), "s");
        let point = DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0);

        let decision = scheduler.decide(point);
        let trace = scheduler.trace();

        assert_eq!(trace.records.len(), 1);
        assert_eq!(trace.records[0].decision, decision);
    }

    #[test]
    fn a_replay_reproduces_the_trace_it_was_given() {
        let faults = vec![FaultKind::SwallowAck, FaultKind::ConnectionDrop];
        let points: Vec<_> = (0..24)
            .map(|ordinal| DecisionPoint::new(PointKind::Ack, ConnectionId(ordinal % 3), ordinal))
            .collect();

        let original = Scheduler::seeded(8_837_291, faults, Profile::default(), "s");
        for point in &points {
            original.decide(point.clone());
        }
        let recorded = original.trace();

        let replayed = Scheduler::replaying(&recorded);
        for point in &points {
            replayed.decide(point.clone());
        }

        assert_eq!(replayed.trace().records, recorded.records);
    }
}

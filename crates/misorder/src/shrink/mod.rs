//! Collapsing a failing trace to the decisions that caused it.
//!
//! A run that fails has 847 decisions in it. Six of them mattered. Delta
//! debugging finds which six: neutralise a decision, re-run, and keep the
//! change if the run still fails.
//!
//! This is not an add-on, and that is worth writing down where it will be read.
//! A version of this tool that found failures and did not reduce them would
//! hand you an 847 line trace nobody can act on - less useful than the incident
//! it predicted. The thing that makes someone adopt the tool is the six lines.
//!
//! # You cannot shrink the seed
//!
//! Seeds 8837291 and 8837292 produce unrelated schedules. There is no gradient
//! to descend, no sense in which a smaller seed is a simpler failure, and no
//! meaning to a halfway point between them. What shrinks is the trace.
//!
//! # What "removing" a decision means
//!
//! Not deleting the line. A removed decision becomes
//! [`Decision::NEUTRAL`](crate::trace::Decision::NEUTRAL): the fork still
//! happens, and takes the boring path. That is what makes the result readable
//! as "this fault was available and was not needed", and it is why every
//! decision has a neutral counterpart by construction.
//!
//! # Why ddmin and not one pass of removals
//!
//! A single pass removing one decision at a time is O(n) re-runs and gets stuck
//! whenever two decisions are only redundant together: neither can go alone, so
//! neither goes. ddmin tries subsets at increasing granularity, so it removes
//! them as a pair. On a trace where most decisions are irrelevant, which is the
//! usual case, it also converges much faster, because the first thing it tries
//! is throwing away half.

use async_trait::async_trait;

use crate::error::Result;
use crate::trace::{PointKey, Trace};

/// Decides whether a candidate trace still reproduces the failure.
///
/// Re-running is the expensive part of shrinking, so this is the interface the
/// cost lives behind. In production it starts containers; in the tests below it
/// is a closure, which is the point: the search is pure logic and is tested
/// without any of that.
#[async_trait]
pub trait Oracle: Send {
    /// `true` if this trace still produces the failure being shrunk.
    ///
    /// An oracle that answers `true` for a trace which fails for a *different*
    /// reason will shrink towards that other reason instead. The runner's
    /// implementation therefore matches on the specific invariant that fired,
    /// not on "the run failed".
    async fn still_fails(&mut self, trace: &Trace) -> Result<bool>;
}

/// How much shrinking is allowed to cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Ceiling on oracle calls.
    ///
    /// Shrinking is best-effort by nature: a partly shrunk trace is still worth
    /// far more than the original, so running out of budget returns the best
    /// result so far rather than an error.
    pub max_attempts: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_attempts: 2_000,
        }
    }
}

/// What shrinking achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The shrunk trace. This is the artifact that gets committed.
    pub trace: Trace,
    /// Perturbing decisions before.
    pub before: usize,
    /// Perturbing decisions after.
    pub after: usize,
    /// Oracle calls spent.
    pub attempts: usize,
    /// Whether the budget ran out before the search finished.
    pub exhausted: bool,
}

impl Report {
    /// Whether shrinking removed anything.
    pub fn is_reduced(&self) -> bool {
        self.after < self.before
    }
}

/// Shrinks a failing trace to a 1-minimal set of decisions.
///
/// "1-minimal" is the honest claim, and it is what delta debugging gives:
/// removing any single remaining decision makes the failure go away. It is not
/// the globally smallest set, which is exponential to find and not worth it.
pub async fn shrink(trace: &Trace, oracle: &mut dyn Oracle, limits: Limits) -> Result<Report> {
    let active: Vec<PointKey> = trace.active().map(|record| record.point.key).collect();
    let before = active.len();

    let mut state = Search {
        trace,
        oracle,
        limits,
        attempts: 0,
        exhausted: false,
    };

    let minimal = state.ddmin(active).await?;
    let (attempts, exhausted) = (state.attempts, state.exhausted);

    Ok(Report {
        trace: keep_only(trace, &minimal),
        before,
        after: minimal.len(),
        attempts,
        exhausted,
    })
}

/// The trace with everything outside `keep` neutralised.
fn keep_only(trace: &Trace, keep: &[PointKey]) -> Trace {
    let drop: Vec<PointKey> = trace
        .active()
        .map(|record| record.point.key)
        .filter(|key| !keep.contains(key))
        .collect();

    trace.without(&drop)
}

struct Search<'a> {
    trace: &'a Trace,
    oracle: &'a mut dyn Oracle,
    limits: Limits,
    attempts: usize,
    exhausted: bool,
}

impl Search<'_> {
    /// Whether the failure survives with only `keep` active.
    ///
    /// Returns `false` once the budget is spent, which stops the search and
    /// leaves the best candidate found so far in place.
    async fn fails_with(&mut self, keep: &[PointKey]) -> Result<bool> {
        if self.attempts >= self.limits.max_attempts {
            self.exhausted = true;
            return Ok(false);
        }

        self.attempts += 1;

        self.oracle.still_fails(&keep_only(self.trace, keep)).await
    }

    /// Zeller and Hildebrandt's ddmin.
    async fn ddmin(&mut self, mut candidate: Vec<PointKey>) -> Result<Vec<PointKey>> {
        let mut granularity = 2;

        while candidate.len() >= 2 && !self.exhausted {
            let chunks = partition(&candidate, granularity);

            // Does any single chunk reproduce it on its own? The big win: a
            // trace whose failure needs two decisions out of 847 gets to 424
            // in one call.
            let mut reduced = None;
            for chunk in &chunks {
                if self.fails_with(chunk).await? {
                    reduced = Some(chunk.clone());
                    break;
                }
            }

            if let Some(chunk) = reduced {
                candidate = chunk;
                granularity = 2;
                continue;
            }

            // Otherwise, can any single chunk be thrown away?
            let mut reduced = None;
            for index in 0..chunks.len() {
                let complement: Vec<PointKey> = chunks
                    .iter()
                    .enumerate()
                    .filter(|(other, _)| *other != index)
                    .flat_map(|(_, chunk)| chunk.iter().copied())
                    .collect();

                if self.fails_with(&complement).await? {
                    reduced = Some(complement);
                    break;
                }
            }

            if let Some(complement) = reduced {
                granularity = granularity.saturating_sub(1).max(2);
                candidate = complement;
                continue;
            }

            if granularity >= candidate.len() {
                break;
            }

            granularity = (granularity * 2).min(candidate.len());
        }

        Ok(candidate)
    }
}

/// Splits into `count` chunks as evenly as possible, dropping empty ones.
fn partition(items: &[PointKey], count: usize) -> Vec<Vec<PointKey>> {
    let count = count.clamp(1, items.len().max(1));
    let size = items.len().div_ceil(count);

    items
        .chunks(size.max(1))
        .map(<[PointKey]>::to_vec)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionId;
    use crate::trace::{Decision, DecisionPoint, PointKind, Record};
    use std::collections::HashSet;
    use std::time::Duration;

    /// A trace of `count` dropped acks, all on one connection.
    fn trace_of(count: u64) -> Trace {
        let mut trace = Trace::new(8_837_291, "synthetic");

        for ordinal in 0..count {
            trace.records.push(Record {
                seq: ordinal,
                at: Duration::from_millis(ordinal),
                point: DecisionPoint::new(PointKind::Ack, ConnectionId(1), ordinal),
                decision: Decision::Drop,
            });
        }

        trace
    }

    fn key(ordinal: u64) -> PointKey {
        PointKey {
            kind: PointKind::Ack,
            connection: 1,
            ordinal,
        }
    }

    /// Fails only when every key in `required` is still active.
    struct NeedsAllOf {
        required: HashSet<PointKey>,
        calls: usize,
    }

    #[async_trait]
    impl Oracle for NeedsAllOf {
        async fn still_fails(&mut self, trace: &Trace) -> Result<bool> {
            self.calls += 1;

            let active: HashSet<PointKey> = trace.active().map(|record| record.point.key).collect();

            Ok(self.required.is_subset(&active))
        }
    }

    fn needs(ordinals: &[u64]) -> NeedsAllOf {
        NeedsAllOf {
            required: ordinals.iter().copied().map(key).collect(),
            calls: 0,
        }
    }

    #[tokio::test]
    async fn eight_hundred_decisions_collapse_to_the_three_that_mattered() {
        let trace = trace_of(847);
        let mut oracle = needs(&[3, 17, 42]);

        let report = shrink(&trace, &mut oracle, Limits::default())
            .await
            .expect("shrink");

        let remaining: HashSet<PointKey> = report
            .trace
            .active()
            .map(|record| record.point.key)
            .collect();

        assert_eq!(remaining, [key(3), key(17), key(42)].into_iter().collect());
        assert_eq!(report.before, 847);
        assert_eq!(report.after, 3);
        assert!(report.is_reduced());
        assert!(!report.exhausted);
    }

    #[tokio::test]
    async fn shrinking_neutralises_rather_than_deletes() {
        let trace = trace_of(20);
        let mut oracle = needs(&[5]);

        let report = shrink(&trace, &mut oracle, Limits::default())
            .await
            .expect("shrink");

        assert_eq!(
            report.trace.records.len(),
            20,
            "every fork the run reached stays in the file"
        );
        assert_eq!(report.after, 1);
    }

    #[tokio::test]
    async fn decisions_that_are_only_redundant_together_are_still_removed() {
        // The case a one-at-a-time pass cannot solve: 8 and 9 are both
        // irrelevant, but removing either alone leaves a trace that still
        // fails, so a greedy pass keeps neither and reports both.
        let trace = trace_of(64);
        let mut oracle = needs(&[1]);

        let report = shrink(&trace, &mut oracle, Limits::default())
            .await
            .expect("shrink");

        assert_eq!(report.after, 1);
    }

    #[tokio::test]
    async fn a_failure_needing_everything_shrinks_to_nothing_smaller() {
        let trace = trace_of(8);
        let mut oracle = needs(&[0, 1, 2, 3, 4, 5, 6, 7]);

        let report = shrink(&trace, &mut oracle, Limits::default())
            .await
            .expect("shrink");

        assert_eq!(report.after, 8);
        assert!(!report.is_reduced());
    }

    #[tokio::test]
    async fn running_out_of_budget_returns_the_best_result_so_far() {
        let trace = trace_of(500);
        let mut oracle = needs(&[7, 200, 411]);

        let report = shrink(&trace, &mut oracle, Limits { max_attempts: 3 })
            .await
            .expect("shrink");

        assert!(report.exhausted);
        assert!(report.attempts <= 3);
        assert!(
            report.after <= report.before,
            "a partial shrink never grows the trace"
        );
    }

    #[tokio::test]
    async fn shrinking_costs_far_less_than_one_run_per_decision() {
        let trace = trace_of(847);
        let mut oracle = needs(&[3, 17, 42]);

        let report = shrink(&trace, &mut oracle, Limits::default())
            .await
            .expect("shrink");

        assert!(
            report.attempts < 847,
            "ddmin used {} calls for 847 decisions",
            report.attempts
        );
    }
}

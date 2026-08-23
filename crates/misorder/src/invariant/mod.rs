//! What must always be true.
//!
//! Two kinds, and the split is the whole adoption argument.
//!
//! [`builtin`] invariants ship with each adapter and need zero user input. They
//! encode the semantics of the dependency itself: a NATS consumer never sees
//! more than `max_deliver` deliveries, a Postgres connection that errored does
//! not then commit. A first-time user gets a caught bug before they have
//! learned what this tool is, which is the only order in which anyone adopts
//! anything.
//!
//! [`user`] invariants are the ones only the user can write. No protocol
//! invariant can know that fills never exceed order quantity. That part is
//! irreducibly theirs, and the leverage is the pitch: five invariants against
//! ten thousand orderings.
//!
//! # Streaming and terminal checks
//!
//! [`Invariant::observe`] runs on every event, and is where anything about
//! *sequence* belongs: a delivery after an ack is only wrong because of what
//! came before it. [`Invariant::finish`] runs once the system is quiescent, and
//! is where anything about *final state* belongs: a SQL query, or an accounting
//! of requests that never reached a terminal state.
//!
//! An invariant that could be either should be streaming. A violation caught at
//! the moment it happens carries the event that caused it, and that event is
//! what the reproducer prints.

pub mod builtin;
pub mod user;

use async_trait::async_trait;

use crate::error::Result;
use crate::event::Observed;

/// Something that was supposed to be true and was not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The invariant's name, as written in the scenario or as the built-in is
    /// called. This is what a user greps for.
    pub invariant: String,

    /// What actually happened, in the user's vocabulary.
    ///
    /// Names subjects, statements and counts, not internal types. Someone reads
    /// this at 3am with no context, and "delivered 6 times, max_deliver is 5"
    /// tells them everything while "assertion failed: n <= max" tells them
    /// nothing.
    pub detail: String,

    /// When, relative to the start of the run.
    pub at: std::time::Duration,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.invariant, self.detail)
    }
}

/// What a terminal check gets to look at.
///
/// Addresses rather than open connections: a check that held a pooled
/// connection through the run would be one more participant in the interleaving
/// it is supposed to be observing.
#[derive(Debug, Clone, Default)]
pub struct CheckContext {
    /// Connection string for the scenario's Postgres, if it declared one.
    pub postgres_url: Option<String>,

    /// Where the service under test is listening, for `check = "http"`.
    ///
    /// The service's own address rather than the proxy's, and deliberately: a
    /// terminal check asks the service what its state ended up as, and routing
    /// that question through the fault injector could drop it, delay it past
    /// the report, or answer it out of order. The check would then be measuring
    /// the harness.
    pub service_url: Option<String>,

    /// Total length of the run.
    pub elapsed: std::time::Duration,
}

/// An assertion checked against every generated ordering.
///
/// `Debug` is a supertrait rather than a nicety: an invariant that fires needs
/// to be identifiable in a log without the reader knowing which concrete type
/// it was, and a `Box<dyn Invariant>` with no `Debug` makes every enclosing
/// type undebuggable too.
#[async_trait]
pub trait Invariant: std::fmt::Debug + Send + Sync {
    /// As it appears in a report.
    fn name(&self) -> &str;

    /// One line, for `mis check`.
    fn describe(&self) -> &str;

    /// Called for every event, in order.
    ///
    /// Returns at most one violation, and the first one wins: after an
    /// invariant has fired, everything downstream is a consequence, and
    /// reporting the cascade buries the cause.
    fn observe(&mut self, observed: &Observed) -> Option<Violation>;

    /// Called once, after the system goes quiescent.
    async fn finish(&mut self, context: &CheckContext) -> Result<Option<Violation>> {
        let _ = context;
        Ok(None)
    }
}

/// Every invariant a scenario declared, checked together.
pub struct Checker {
    invariants: Vec<Box<dyn Invariant>>,
    violations: Vec<Violation>,
    fired: Vec<bool>,
}

impl Checker {
    pub fn new(invariants: Vec<Box<dyn Invariant>>) -> Self {
        let fired = vec![false; invariants.len()];

        Self {
            invariants,
            violations: Vec::new(),
            fired,
        }
    }

    /// Feeds one event to every invariant that has not already fired.
    ///
    /// A run is not stopped by the first violation. Two independent invariants
    /// breaking in one ordering is a real and useful signal, and stopping early
    /// would hide the second one behind however many seeds it takes to
    /// separate them.
    pub fn observe(&mut self, observed: &Observed) {
        for (index, invariant) in self.invariants.iter_mut().enumerate() {
            if self.fired[index] {
                continue;
            }

            if let Some(violation) = invariant.observe(observed) {
                self.fired[index] = true;
                self.violations.push(violation);
            }
        }
    }

    /// Runs every terminal check.
    pub async fn finish(&mut self, context: &CheckContext) -> Result<()> {
        for (index, invariant) in self.invariants.iter_mut().enumerate() {
            if self.fired[index] {
                continue;
            }

            if let Some(violation) = invariant.finish(context).await? {
                self.fired[index] = true;
                self.violations.push(violation);
            }
        }

        Ok(())
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.invariants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.invariants.is_empty()
    }
}

impl std::fmt::Debug for Checker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Checker")
            .field("invariants", &self.invariants.len())
            .field("violations", &self.violations.len())
            .finish()
    }
}

/// Builds every invariant a resolved scenario declared.
///
/// Dispatches on which key the block used, which the scenario parser has
/// already validated, so anything reaching here with neither is an internal
/// error rather than a user mistake.
pub fn build_all(
    scenario: &crate::scenario::file::Resolved,
) -> crate::error::Result<Vec<Box<dyn Invariant>>> {
    scenario
        .invariants
        .iter()
        .map(|spec| {
            if spec.builtin.is_some() {
                builtin::build(spec)
            } else {
                user::build(spec)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Lifecycle};
    use std::time::Duration;

    #[derive(Debug)]
    struct AlwaysFires;

    #[async_trait]
    impl Invariant for AlwaysFires {
        fn name(&self) -> &str {
            "always_fires"
        }

        fn describe(&self) -> &str {
            "fires on the first event"
        }

        fn observe(&mut self, observed: &Observed) -> Option<Violation> {
            Some(Violation {
                invariant: self.name().to_string(),
                detail: "as advertised".to_string(),
                at: observed.at,
            })
        }
    }

    fn quiescent() -> Observed {
        Observed::new(
            Duration::from_millis(1),
            Event::Lifecycle(Lifecycle::Quiescent),
        )
    }

    #[test]
    fn an_invariant_fires_at_most_once() {
        let mut checker = Checker::new(vec![Box::new(AlwaysFires)]);

        checker.observe(&quiescent());
        checker.observe(&quiescent());
        checker.observe(&quiescent());

        assert_eq!(checker.violations().len(), 1);
    }

    #[test]
    fn independent_invariants_both_report() {
        let mut checker = Checker::new(vec![Box::new(AlwaysFires), Box::new(AlwaysFires)]);

        checker.observe(&quiescent());

        assert_eq!(checker.violations().len(), 2);
        assert!(!checker.is_clean());
    }
}

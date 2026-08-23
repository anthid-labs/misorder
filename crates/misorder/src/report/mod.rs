//! Turning a failure into something a person can act on.
//!
//! The reproducer is the product. A tool that reports "seed 8837291 failed" has
//! handed back the incident it was supposed to explain; one that reports the
//! six events that caused it has done the work.

pub mod run;

pub use run::{RunReport, SweepReport};

use std::time::Duration;

use crate::event::Observed;
use crate::invariant::Violation;
use crate::schedule::FaultKind;
use crate::trace::Trace;

/// A minimal, replayable description of one failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reproducer {
    pub scenario: String,
    pub seed: u64,
    pub violation: Violation,

    /// The decisions that survived shrinking.
    pub steps: Vec<Step>,

    /// Decisions in the original run, for the "6 of 847" line.
    pub original_decisions: usize,

    /// Dependencies the scenario declared that never appeared in the failing
    /// run.
    pub uninvolved: Vec<String>,

    /// Faults the scenario permitted that the failure did not need.
    pub unused_faults: Vec<FaultKind>,
}

/// One line of a reproducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub at: Duration,
    pub connection: u64,
    pub what: String,
}

impl Reproducer {
    /// Builds a reproducer from a shrunk trace and the run that produced it.
    pub fn build(
        trace: &Trace,
        violation: Violation,
        events: &[Observed],
        declared_deps: &[&str],
        declared_faults: &[FaultKind],
        original_decisions: usize,
    ) -> Self {
        let steps = trace
            .active()
            .map(|record| Step {
                at: record.at,
                connection: record.point.key.connection,
                what: match &record.point.detail {
                    Some(detail) => {
                        format!(
                            "{} ({detail})",
                            record.decision.describe(record.point.key.kind)
                        )
                    }
                    None => record.decision.describe(record.point.key.kind),
                },
            })
            .collect();

        let involved: Vec<&str> = events
            .iter()
            .filter_map(|observed| observed.event.dependency())
            .collect();

        let uninvolved = declared_deps
            .iter()
            .filter(|name| !involved.contains(*name))
            .map(|name| (*name).to_string())
            .collect();

        let used: Vec<FaultKind> = trace
            .active()
            .filter_map(|record| record.decision.fault_kind())
            .collect();

        let unused_faults = declared_faults
            .iter()
            .filter(|fault| !used.contains(fault))
            .copied()
            .collect();

        Self {
            scenario: trace.scenario.clone(),
            seed: trace.seed,
            violation,
            steps,
            original_decisions,
            uninvolved,
            unused_faults,
        }
    }

    /// The reproducer as it appears on a terminal.
    ///
    /// The negative space is as valuable as the steps. "Postgres was not
    /// involved" tells a reader which half of their system to stop reading, and
    /// naming the faults that were available and not needed is what stops
    /// someone concluding the bug needs a network partition when it needs one
    /// dropped ack.
    pub fn render(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!("MINIMAL REPRODUCER: {}\n", self.scenario));
        out.push_str(&format!(
            "seed {}, {} of {} decisions\n\n",
            self.seed,
            self.steps.len(),
            self.original_decisions
        ));

        for (index, step) in self.steps.iter().enumerate() {
            out.push_str(&format!(
                "  {}. [{:>6}ms] conn:{} {}\n",
                index + 1,
                step.at.as_millis(),
                step.connection,
                step.what
            ));
        }

        out.push_str(&format!(
            "\n  {}: {}\n",
            self.violation.invariant, self.violation.detail
        ));

        let mut notes = Vec::new();

        if !self.uninvolved.is_empty() {
            notes.push(format!(
                "{} {} not involved.",
                capitalise(&join_and(&self.uninvolved)),
                was_or_were(self.uninvolved.len())
            ));
        }

        if !self.unused_faults.is_empty() {
            let names: Vec<String> = self
                .unused_faults
                .iter()
                .map(|fault| format!("'{fault}'"))
                .collect();

            notes.push(format!(
                "Fault{} {} {} not required.",
                plural(names.len()),
                join_and(&names),
                was_or_were(names.len())
            ));
        }

        if !notes.is_empty() {
            out.push_str(&format!("\n  {}\n", notes.join(" ")));
        }

        out
    }
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn was_or_were(count: usize) -> &'static str {
    if count == 1 { "was" } else { "were" }
}

/// `a`, `a and b`, `a, b and c`.
///
/// Worth the eight lines. This sentence is the last thing a reader sees before
/// they go and look at their own code, and "nats, postgres was not involved"
/// makes them stop and reparse it.
fn join_and(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn capitalise(text: &str) -> String {
    let mut chars = text.chars();

    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event::{ConnectionId, Event, NatsEvent};
    use crate::trace::{Decision, DecisionPoint, PointKind, Record};
    use bytes::Bytes;

    pub(crate) fn sample_reproducer() -> Reproducer {
        let mut trace = Trace::new(8_837_291, "dead_letter_no_redelivery");

        trace.records.push(Record {
            seq: 0,
            at: Duration::from_millis(12),
            point: DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0).with_detail("ledger.>"),
            decision: Decision::Drop,
        });
        trace.records.push(Record {
            seq: 1,
            at: Duration::from_millis(40),
            point: DecisionPoint::new(PointKind::Deliver, ConnectionId(1), 0),
            decision: Decision::NEUTRAL,
        });

        let events = vec![Observed::on(
            Duration::from_millis(1),
            ConnectionId(1),
            Event::Nats(NatsEvent::Published {
                subject: "ledger.order".to_string(),
                payload: Bytes::new(),
            }),
        )];

        Reproducer::build(
            &trace,
            Violation {
                invariant: "no_infinite_redelivery".to_string(),
                detail: "the payload on ledger.dead_letter was delivered 11 times".to_string(),
                at: Duration::from_millis(90),
            },
            &events,
            &["nats", "postgres"],
            &[
                FaultKind::SwallowAck,
                FaultKind::Reorder,
                FaultKind::ConnectionDrop,
            ],
            847,
        )
    }

    #[test]
    fn only_surviving_decisions_become_steps() {
        let reproducer = sample_reproducer();

        assert_eq!(reproducer.steps.len(), 1);
        assert_eq!(reproducer.original_decisions, 847);
    }

    #[test]
    fn a_dependency_that_never_appeared_is_called_out() {
        let reproducer = sample_reproducer();

        assert_eq!(reproducer.uninvolved, vec!["postgres".to_string()]);
    }

    #[test]
    fn faults_the_failure_did_not_need_are_called_out() {
        let reproducer = sample_reproducer();

        assert_eq!(
            reproducer.unused_faults,
            vec![FaultKind::Reorder, FaultKind::ConnectionDrop]
        );
    }

    #[test]
    fn the_rendered_report_says_what_was_not_involved() {
        let rendered = sample_reproducer().render();

        assert!(rendered.contains("MINIMAL REPRODUCER: dead_letter_no_redelivery"));
        assert!(rendered.contains("1 of 847 decisions"), "{rendered}");
        assert!(
            rendered.contains("Postgres was not involved."),
            "{rendered}"
        );
        assert!(
            rendered.contains("Faults 'reorder' and 'connection_drop' were not required."),
            "{rendered}"
        );
        assert!(rendered.contains("no_infinite_redelivery:"), "{rendered}");
        assert!(
            rendered.contains("1. [    12ms] conn:1 drop ack (ledger.>)"),
            "the step line is the thing a reader acts on, so its exact shape is \
             pinned here: {rendered}"
        );
    }

    #[test]
    fn lists_of_two_or_more_read_as_english() {
        assert_eq!(join_and(&[]), "");
        assert_eq!(join_and(&["a".to_string()]), "a");
        assert_eq!(join_and(&["a".to_string(), "b".to_string()]), "a and b");
        assert_eq!(
            join_and(&["a".to_string(), "b".to_string(), "c".to_string()]),
            "a, b and c"
        );
    }

    #[test]
    fn several_uninvolved_dependencies_read_as_plural() {
        let mut reproducer = sample_reproducer();
        reproducer.uninvolved = vec!["postgres".to_string(), "redis".to_string()];

        assert!(
            reproducer
                .render()
                .contains("Postgres and redis were not involved."),
            "{}",
            reproducer.render()
        );
    }

    #[test]
    fn a_single_unused_fault_reads_as_singular() {
        let mut reproducer = sample_reproducer();
        reproducer.unused_faults = vec![FaultKind::Reorder];

        assert!(
            reproducer
                .render()
                .contains("Fault 'reorder' was not required."),
            "{}",
            reproducer.render()
        );
    }
}

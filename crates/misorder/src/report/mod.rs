//! Turning a failure into something a person can act on.
//!
//! The reproducer is the product. A tool that reports "seed 8837291 failed" has
//! handed back the incident it was supposed to explain; one that reports the
//! six events that caused it has done the work.

pub mod run;

pub use run::{RunReport, SweepReport};

use std::time::Duration;

use crate::event::{Event, HttpEvent, Observed};
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

    /// What the service actually received, as send-order positions in the
    /// order they arrived.
    ///
    /// `[1, 2, 3, 4, 6, 5]` is five deliveries in the order they were sent and
    /// a sixth that overtook the fifth. Empty for a protocol that does not
    /// report delivery order, and for a failure that had nothing delivered.
    ///
    /// Positions rather than payloads, deliberately. A reproducer is something
    /// you attach to a public issue, and the rule that makes that safe is that
    /// it records decisions rather than messages. The order two requests
    /// arrived in is a decision; what was in them is not.
    pub arrivals: Vec<u64>,
}

/// One line of a reproducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub at: Duration,
    pub connection: u64,
    pub what: String,
}

/// How terminal output is coloured.
///
/// Codes rather than a colour library, because this crate's dependency list is
/// something buyers read and four escape sequences do not justify a line in it.
///
/// The engine holds the palette but never decides whether to use it. Whether a
/// terminal is attached, whether `NO_COLOR` is set, and whether the user asked
/// for plain output are all questions about the process the CLI is running in,
/// and a library that answered them itself would put escape codes into the
/// report field of somebody's JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Style {
    /// An invariant that held, a run that passed.
    pub good: &'static str,
    /// A violation. The finding itself, not the trouble around it.
    pub bad: &'static str,
    /// Something that needs attention and is not a finding: a run that could
    /// not complete, an invariant that is named but not implemented.
    pub warn: &'static str,
    /// Structure rather than content - timings, ordinals, the negative space.
    pub dim: &'static str,
    pub reset: &'static str,
}

impl Style {
    /// No colour at all. Every field is empty, so a styled render and a plain
    /// one produce identical bytes.
    pub const fn plain() -> Self {
        Self {
            good: "",
            bad: "",
            warn: "",
            dim: "",
            reset: "",
        }
    }

    /// The eight-colour set, which every terminal worth supporting has had
    /// since the 1980s. Bright variants and 256-colour codes buy nothing here
    /// and are the ones that come out unreadable on a light background.
    pub const fn colour() -> Self {
        Self {
            good: "\x1b[32m",
            bad: "\x1b[31m",
            warn: "\x1b[33m",
            dim: "\x1b[2m",
            reset: "\x1b[0m",
        }
    }

    /// Wraps `text`, and does nothing when the palette is plain.
    pub fn paint(&self, colour: &str, text: impl std::fmt::Display) -> String {
        if colour.is_empty() {
            return text.to_string();
        }

        format!("{colour}{text}{}", self.reset)
    }
}

impl Default for Style {
    fn default() -> Self {
        Self::plain()
    }
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

        // Emitted in the order requests reached the service, each carrying the
        // position the client sent it at. The two disagreeing is a reordering.
        let arrivals: Vec<u64> = events
            .iter()
            .filter_map(|observed| match &observed.event {
                Event::Http(HttpEvent::Request { order, .. }) => Some(order + 1),
                _ => None,
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
            arrivals,
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
        self.render_with(&Style::plain())
    }

    /// The order the service received things, against the order they were
    /// sent.
    ///
    /// Nothing at all when the two agree, which is the common case even in a
    /// failing run: most seeds perturb something that turns out not to matter.
    /// A section that appeared on every reproducer whether or not it said
    /// anything would stop being read.
    ///
    /// The out-of-order positions are the failure colour, because on a
    /// reordering bug they *are* the failure - the invariant underneath names
    /// what broke, and this line is where it broke.
    fn render_arrivals(&self, style: &Style) -> String {
        if self.arrivals.len() < 2 {
            return String::new();
        }

        // Bounded by the highest position seen rather than by how many
        // arrived, because a dropped delivery leaves a gap: five arrivals
        // numbered up to six is six sent and one lost, not five of anything.
        //
        // A delivery dropped from the *end* is invisible here and always will
        // be - nothing observed it, and inventing it would be the harness
        // reporting traffic that never existed.
        let total = self
            .arrivals
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(self.arrivals.len() as u64);

        let sent: Vec<u64> = (1..=total).collect();

        // A position that arrived after something sent later than it. Computed
        // against the whole prefix rather than the previous entry, so that one
        // delivery overtaking three is three out-of-order positions and not
        // one.
        let mut highest = 0;
        let mut late = vec![false; self.arrivals.len()];

        for (index, position) in self.arrivals.iter().enumerate() {
            if *position < highest {
                late[index] = true;
            }

            highest = highest.max(*position);
        }

        let dropped: Vec<u64> = sent
            .iter()
            .filter(|position| !self.arrivals.contains(position))
            .copied()
            .collect();

        if !late.contains(&true) && dropped.is_empty() {
            return String::new();
        }

        let mut out = String::from("\n  delivery order\n");

        out.push_str(&format!(
            "    sent      {}\n",
            style.paint(
                style.dim,
                sent.iter()
                    .map(|position| format!("#{position}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        ));

        let received: Vec<String> = self
            .arrivals
            .iter()
            .enumerate()
            .map(|(index, position)| {
                let cell = format!("#{position}");

                if late[index] {
                    style.paint(style.bad, cell)
                } else {
                    cell
                }
            })
            .collect();

        out.push_str(&format!("    received  {}\n", received.join(" ")));

        if !dropped.is_empty() {
            let names: Vec<String> = dropped
                .iter()
                .map(|position| format!("#{position}"))
                .collect();

            out.push_str(&format!(
                "    {}\n",
                style.paint(style.warn, format!("never arrived  {}", names.join(" ")))
            ));
        }

        out
    }

    /// The reproducer, coloured.
    ///
    /// One implementation rather than two, because a second copy of this
    /// formatting would drift and the plain one is what ends up in the report
    /// document that other tools read.
    pub fn render_with(&self, style: &Style) -> String {
        let mut out = String::new();

        out.push_str(&format!(
            "{}\n",
            style.paint(style.bad, format!("MINIMAL REPRODUCER: {}", self.scenario))
        ));
        out.push_str(&format!(
            "seed {}, {} of {} decisions\n\n",
            self.seed,
            self.steps.len(),
            self.original_decisions
        ));

        for (index, step) in self.steps.iter().enumerate() {
            out.push_str(&format!(
                "  {}. {} {}\n",
                index + 1,
                style.paint(
                    style.dim,
                    format!("[{:>6}ms] conn:{}", step.at.as_millis(), step.connection)
                ),
                style.paint(style.warn, &step.what)
            ));
        }

        out.push_str(&self.render_arrivals(style));

        out.push_str(&format!(
            "\n  {}: {}\n",
            style.paint(style.bad, &self.violation.invariant),
            self.violation.detail
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
            out.push_str(&format!(
                "\n  {}\n",
                style.paint(style.dim, notes.join(" "))
            ));
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

    /// Builds a reproducer with a given arrival order and nothing else
    /// interesting, so the delivery-order rendering can be exercised on its
    /// own.
    fn with_arrivals(arrivals: &[u64]) -> Reproducer {
        Reproducer {
            arrivals: arrivals.to_vec(),
            ..sample_reproducer()
        }
    }

    /// Silence when the two orders agree. Most failing seeds perturb something
    /// that turned out not to matter, and a section that appeared on every
    /// reproducer whether or not it said anything would stop being read.
    #[test]
    fn deliveries_that_arrived_in_order_say_nothing() {
        let rendered = with_arrivals(&[1, 2, 3, 4, 5, 6]).render();

        assert!(
            !rendered.contains("delivery order"),
            "an unperturbed order should not be reported:\n{rendered}"
        );
    }

    #[test]
    fn a_single_delivery_says_nothing() {
        assert!(!with_arrivals(&[1]).render().contains("delivery order"));
    }

    /// The case the whole section exists for.
    #[test]
    fn a_late_delivery_is_named_and_painted_as_the_failure() {
        let rendered = with_arrivals(&[1, 2, 3, 4, 6, 5]).render_with(&Style::colour());

        assert!(rendered.contains("delivery order"), "{rendered}");
        assert!(
            rendered.contains(&format!(
                "{}#5{}",
                Style::colour().bad,
                Style::colour().reset
            )),
            "the delivery that arrived late is not in the failure colour:\n{rendered}"
        );
        assert!(
            !rendered.contains(&format!(
                "{}#6{}",
                Style::colour().bad,
                Style::colour().reset
            )),
            "the delivery that overtook is not itself the problem:\n{rendered}"
        );
    }

    /// One delivery overtaking three is three positions out of order, not one.
    /// Comparing against the previous entry rather than the whole prefix would
    /// report only the first.
    #[test]
    fn one_delivery_overtaking_several_marks_all_of_them() {
        let rendered = with_arrivals(&[6, 1, 2, 3, 4, 5]).render_with(&Style::colour());

        for position in ["#1", "#2", "#3", "#4", "#5"] {
            assert!(
                rendered.contains(&format!(
                    "{}{position}{}",
                    Style::colour().bad,
                    Style::colour().reset
                )),
                "{position} arrived after #6 and is not marked:\n{rendered}"
            );
        }
    }

    /// A gap in the positions is a delivery the schedule dropped. Bounding the
    /// sent row by how many arrived instead of by the highest position seen
    /// would hide it.
    #[test]
    fn a_delivery_that_never_arrived_is_reported_as_missing() {
        let rendered = with_arrivals(&[1, 2, 4, 5, 6]).render();

        assert!(rendered.contains("never arrived  #3"), "{rendered}");
        assert!(
            rendered.contains("#1 #2 #3 #4 #5 #6"),
            "the sent row should span the whole range, gap included:\n{rendered}"
        );
    }

    /// Nothing was delivered, so there is no order to report. A protocol with
    /// no delivery events at all lands here too.
    #[test]
    fn no_deliveries_report_no_order() {
        assert!(!with_arrivals(&[]).render().contains("delivery order"));
    }

    /// A plain palette must produce byte-identical output to the unstyled
    /// render. If it does not, the report document and the terminal disagree
    /// about what happened, and the document is the one other tools read.
    #[test]
    fn a_plain_style_renders_exactly_what_render_does() {
        let reproducer = sample_reproducer();

        assert_eq!(
            reproducer.render(),
            reproducer.render_with(&Style::plain()),
            "the default render is the plain-styled one"
        );
    }

    /// Every escape sequence opened is closed. An unterminated colour bleeds
    /// into whatever the terminal prints next, including the user's prompt.
    #[test]
    fn every_colour_is_reset() {
        let rendered = sample_reproducer().render_with(&Style::colour());

        let opens = rendered.matches("\u{1b}[").count();
        let resets = rendered.matches("\u{1b}[0m").count();

        assert!(opens > 0, "the coloured render painted nothing");
        assert_eq!(
            opens - resets,
            resets,
            "each painted span is one open and one reset; got {opens} open(s), {resets} reset(s)"
        );
    }

    /// The colours carry meaning, so the invariant that fired has to be the
    /// red one rather than merely some colour.
    #[test]
    fn the_violation_is_painted_as_a_failure() {
        let reproducer = sample_reproducer();
        let rendered = reproducer.render_with(&Style::colour());

        assert!(
            rendered.contains(&format!(
                "{}{}{}",
                Style::colour().bad,
                reproducer.violation.invariant,
                Style::colour().reset
            )),
            "the invariant that fired is not in the failure colour:\n{rendered}"
        );
    }

    #[test]
    fn painting_with_an_empty_colour_changes_nothing() {
        assert_eq!(Style::plain().paint("", "text"), "text");
    }

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

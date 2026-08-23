//! The machine-readable result of a run.
//!
//! One versioned JSON document per run, and one per sweep. This is the single
//! seam between the engine and anything that wants to do more with a result
//! than print it: store it, compare it against last week's, comment on a pull
//! request, group ten failing seeds into two bugs, or hand an auditor evidence
//! that a given scenario ran against a given build.
//!
//! # Why a document and not a plugin interface
//!
//! Everything downstream of a run consumes this and nothing else. No hooks into
//! engine internals, no trait a consumer implements, no dynamic loading. That is
//! deliberate in both directions:
//!
//! - The engine stays free to change its internals without breaking anything
//!   built on top, because the only contract is [`FORMAT_VERSION`].
//! - Nothing outside can quietly become load-bearing for the engine. An open
//!   core stops being open the moment the interesting path runs through an
//!   interface only one implementation satisfies.
//!
//! A run writes this to a file or to stdout and exits. Whatever reads it is a
//! separate process with its own lifecycle, which is also what keeps the engine
//! stateless and offline: it has no database, no account, and nothing to send
//! anywhere.
//!
//! # What belongs here and what does not
//!
//! Here: everything true about *one* run or *one* sweep on *one* machine.
//! Counts, outcomes, the failure signature, what was permitted and what was
//! used, which build produced it.
//!
//! Not here: anything needing history or a second machine. Trends over time,
//! which pull request introduced a signature, a team's shared reproducer
//! library, cross-run clustering. Those need persistence, and persistence is
//! not something a stateless CLI should grow.
//!
//! Grouping the failures *within one sweep* is in, because it needs no state
//! and it is what makes the local output honest: ten failing seeds are usually
//! two bugs, and a tool that reports ten teaches you to ignore it.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::invariant::Violation;
use crate::report::Reproducer;
use crate::schedule::FaultKind;

/// Bumped whenever a consumer written against an older version would misread a
/// newer document.
///
/// Additive fields do not bump it, so a consumer must ignore fields it does not
/// know. A report is read by things on their own release cycle, and requiring
/// them to upgrade in step with the engine would make every engine release a
/// coordinated one.
pub const FORMAT_VERSION: u32 = 1;

/// Which build produced a result.
///
/// Part of the document rather than assumed, because a result outlives the
/// binary. An evidence bundle that cannot say which engine version produced it
/// is worth very little to the person checking it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Engine {
    pub name: String,
    pub version: String,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            name: "misorder".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Which scenario a result attests to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioRef {
    pub name: String,

    /// BLAKE3 of the scenario file. Absent when the scenario came from memory
    /// rather than a file, which is honest: there is no artifact to attest to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// How a run ended.
///
/// Three-valued, matching the exit codes, and for the same reason: a consumer
/// counting `Incomplete` as `Violated` would report a broken Docker socket as a
/// caught bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Every invariant held.
    Passed,
    /// An invariant was violated. A finding.
    Violated,
    /// The run could not complete. Not a finding.
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViolationRecord {
    pub invariant: String,
    pub detail: String,
    pub at_ms: u64,
}

impl From<&Violation> for ViolationRecord {
    fn from(violation: &Violation) -> Self {
        Self {
            invariant: violation.invariant.clone(),
            detail: violation.detail.clone(),
            at_ms: violation.at.as_millis() as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decisions {
    /// Every fork the run reached.
    pub recorded: usize,
    /// The ones that perturbed it.
    pub active: usize,
}

/// What was allowed, and what the failure actually used.
///
/// The difference is the useful part. A scenario permitting four faults whose
/// failure needed one is a narrower finding than the permission list suggests,
/// and this is where that becomes countable rather than a sentence in a report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Faults {
    pub permitted: Vec<FaultKind>,
    pub used: Vec<FaultKind>,
}

/// A dependency the run stood up.
///
/// The image digest is what makes a result comparable across time: "this passed
/// against nats:2.10-alpine" is much weaker than the same sentence with the
/// digest, because the tag moved. It is `None` until the orchestrator reports
/// one, and stated as absent rather than filled with the tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyRecord {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// Which slice of a seed space a process was responsible for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRef {
    pub index: u64,
    pub count: u64,
}

/// The result of one run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReport {
    pub format: u32,
    pub engine: Engine,
    pub scenario: ScenarioRef,

    pub seed: u64,
    pub verdict: Verdict,

    /// Identifies the *shape* of the failure, so two runs that found the same
    /// bug agree and two that found different bugs do not.
    ///
    /// Only meaningful on a shrunk trace, and absent on a pass. Computing it
    /// here rather than downstream means the person at the terminal and
    /// anything built on top agree on the identity of a bug.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,

    pub violations: Vec<ViolationRecord>,
    pub decisions: Decisions,
    pub faults: Faults,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyRecord>,

    /// The reproducer as it was printed. Carried so a consumer does not have to
    /// re-render it, and so the text a human saw is the text that was stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reproducer: Option<String>,

    /// RFC3339, UTC.
    pub started_at: String,
    pub elapsed_ms: u64,
}

impl RunReport {
    pub fn passed(&self) -> bool {
        self.verdict == Verdict::Passed
    }

    /// Renders as pretty JSON with a trailing newline.
    ///
    /// Pretty rather than compact: these get committed, attached to tickets, and
    /// diffed, and a one-line document makes every change look like the whole
    /// file changed.
    pub fn to_json(&self) -> String {
        let mut out = serde_json::to_string_pretty(self)
            .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"));
        out.push('\n');

        out
    }
}

/// Distinct failures found in one sweep, with the seeds that found each.
///
/// The whole of dedup that a stateless single-machine run can honestly do.
/// Grouping *across* sweeps, tracking when a signature first appeared, and
/// attributing it to a change are history, and history needs a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureGroup {
    pub signature: String,
    /// The invariant that fired. One signature can only have one, since the
    /// failing decisions are what produce it.
    pub invariant: String,
    /// Ascending. The first is the cheapest one to reproduce from.
    pub seeds: Vec<u64>,
}

/// The result of a sweep of seeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepReport {
    pub format: u32,
    pub engine: Engine,
    pub scenario: ScenarioRef,

    /// Which slice this process ran, when the sweep was split across machines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardRef>,

    /// Seeds this process was asked for, before sharding.
    pub seed_start: u64,
    pub seed_count: u64,

    /// Seeds this process actually ran. Differs from `seed_count` under
    /// sharding, and an attestation that conflated the two would claim coverage
    /// the machine never had.
    pub seeds_run: u64,

    pub passed: u64,
    pub violated: u64,
    /// Runs that could not complete. Not findings, and counted separately so a
    /// sweep where half the runs failed to start cannot read as a clean pass.
    pub incomplete: u64,

    /// Distinct failures, most-found first.
    pub distinct_failures: Vec<SignatureGroup>,

    /// One report per failing seed.
    pub failures: Vec<RunReport>,

    pub started_at: String,
    pub elapsed_ms: u64,
}

impl SweepReport {
    pub fn passed(&self) -> bool {
        self.violated == 0
    }

    /// Whether the sweep covered what it was asked to.
    ///
    /// A sweep with incomplete runs has a coverage hole, and saying so is the
    /// difference between evidence and a number. An auditor reading "10,000
    /// seeds passed" when 4,000 never started is being misled by a true
    /// sentence.
    pub fn is_complete(&self) -> bool {
        self.incomplete == 0 && self.passed + self.violated == self.seeds_run
    }

    pub fn to_json(&self) -> String {
        let mut out = serde_json::to_string_pretty(self)
            .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"));
        out.push('\n');

        out
    }

    /// Groups failing runs by signature, most-found first.
    pub fn group(failures: &[RunReport]) -> Vec<SignatureGroup> {
        let mut groups: Vec<SignatureGroup> = Vec::new();

        for report in failures {
            let Some(signature) = &report.signature else {
                continue;
            };

            let invariant = report
                .violations
                .first()
                .map(|violation| violation.invariant.clone())
                .unwrap_or_default();

            match groups
                .iter_mut()
                .find(|group| group.signature == *signature)
            {
                Some(group) => group.seeds.push(report.seed),
                None => groups.push(SignatureGroup {
                    signature: signature.clone(),
                    invariant,
                    seeds: vec![report.seed],
                }),
            }
        }

        for group in &mut groups {
            group.seeds.sort_unstable();
        }

        // Most-found first, then by first seed so the order is stable between
        // two sweeps that found the same thing.
        groups.sort_by(|a, b| {
            b.seeds
                .len()
                .cmp(&a.seeds.len())
                .then_with(|| a.seeds.first().cmp(&b.seeds.first()))
        });

        groups
    }
}

/// The wall-clock start of a run, as RFC3339 in UTC.
pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Fills the parts of a report that come from the reproducer.
pub(crate) fn from_reproducer(report: &mut RunReport, reproducer: &Reproducer) {
    report.reproducer = Some(reproducer.render());
}

/// Milliseconds, saturating rather than wrapping.
pub(crate) fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failing(seed: u64, signature: &str, invariant: &str) -> RunReport {
        RunReport {
            format: FORMAT_VERSION,
            engine: Engine::default(),
            scenario: ScenarioRef {
                name: "s".to_string(),
                digest: Some("abc".to_string()),
            },
            seed,
            verdict: Verdict::Violated,
            signature: Some(signature.to_string()),
            violations: vec![ViolationRecord {
                invariant: invariant.to_string(),
                detail: "detail".to_string(),
                at_ms: 10,
            }],
            decisions: Decisions {
                recorded: 847,
                active: 6,
            },
            faults: Faults::default(),
            dependencies: Vec::new(),
            reproducer: None,
            started_at: now_rfc3339(),
            elapsed_ms: 1200,
        }
    }

    #[test]
    fn ten_failing_seeds_group_into_the_bugs_they_actually_are() {
        let failures = vec![
            failing(3, "aaaa", "no_infinite_redelivery"),
            failing(71, "bbbb", "no_commit_after_error"),
            failing(9, "aaaa", "no_infinite_redelivery"),
            failing(4, "aaaa", "no_infinite_redelivery"),
        ];

        let groups = SweepReport::group(&failures);

        assert_eq!(groups.len(), 2, "four seeds, two bugs");
        assert_eq!(groups[0].signature, "aaaa", "most-found first");
        assert_eq!(groups[0].seeds, vec![3, 4, 9], "ascending, cheapest first");
        assert_eq!(groups[1].seeds, vec![71]);
    }

    #[test]
    fn grouping_is_stable_between_two_sweeps_that_found_the_same_thing() {
        let one = SweepReport::group(&[failing(1, "aaaa", "x"), failing(2, "bbbb", "y")]);
        let other = SweepReport::group(&[failing(2, "bbbb", "y"), failing(1, "aaaa", "x")]);

        assert_eq!(one, other);
    }

    #[test]
    fn a_sweep_with_runs_that_never_started_is_not_complete_coverage() {
        let sweep = SweepReport {
            format: FORMAT_VERSION,
            engine: Engine::default(),
            scenario: ScenarioRef {
                name: "s".to_string(),
                digest: None,
            },
            shard: None,
            seed_start: 0,
            seed_count: 10_000,
            seeds_run: 10_000,
            passed: 6_000,
            violated: 0,
            incomplete: 4_000,
            distinct_failures: Vec::new(),
            failures: Vec::new(),
            started_at: now_rfc3339(),
            elapsed_ms: 5,
        };

        assert!(sweep.passed(), "nothing was violated");
        assert!(
            !sweep.is_complete(),
            "and 4000 seeds never ran, which a report claiming a clean pass has to say"
        );
    }

    #[test]
    fn a_report_round_trips_through_json() {
        let report = failing(8_837_291, "aaaa", "no_infinite_redelivery");

        let decoded: RunReport = serde_json::from_str(&report.to_json()).expect("decode");

        assert_eq!(decoded, report);
    }

    #[test]
    fn a_consumer_written_against_this_version_can_ignore_unknown_fields() {
        let mut value: serde_json::Value =
            serde_json::from_str(&failing(1, "aaaa", "x").to_json()).expect("decode");

        value["something_added_later"] = serde_json::json!({ "nested": true });

        serde_json::from_value::<RunReport>(value)
            .expect("additive fields must not break an older consumer");
    }

    #[test]
    fn a_passing_report_carries_no_signature() {
        let mut report = failing(1, "aaaa", "x");
        report.verdict = Verdict::Passed;
        report.signature = None;
        report.violations.clear();

        assert!(report.passed());
        assert!(!report.to_json().contains("signature"));
    }
}

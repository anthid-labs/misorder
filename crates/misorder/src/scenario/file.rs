//! The scenario file.
//!
//! One TOML file is the entire interface. It declares what to run, what
//! dependencies it needs, what workload to drive at it, which faults are
//! permitted, and what must always be true. Nothing else is written: no SDK, no
//! imports, no build flags, so a Go service and a Rust service adopt this
//! identically.
//!
//! ```toml
//! name = "dead_letter_no_redelivery"
//!
//! [[system]]
//! run = "./target/debug/ledger"
//! ready_when = "nats_subscription_active"
//!
//! [[deps.nats.streams]]
//! name = "LEDGER"
//! subjects = ["ledger.>"]
//! max_deliver = 5
//! ack_wait = "30s"
//! discard = "old"
//!
//! [deps.postgres]
//! migrations = "./migrations"
//!
//! [[workload]]
//! publish = "ledger.org.org_1.account.acct_1.order"
//! payload = { order_id = "ord_1", kind = "fill", qty = 100 }
//!
//! [faults]
//! enabled = ["ack_timeout", "redelivery", "connection_drop", "reorder"]
//!
//! [[invariants]]
//! builtin = "no_infinite_redelivery"
//! window = "5m"
//! same_payload_max = 10
//! ```
//!
//! # Two constraints this format is built to
//!
//! **It has to be readable by someone who has never heard of deterministic
//! simulation**, because the file is the onboarding. Every key names a thing
//! from the user's own system: a stream, a subject, a migration directory. None
//! of them name a concept from this crate.
//!
//! **It is something a generator emits, not just something a human types.**
//! Phase 2 produces these programmatically from recorded vendor sessions, and
//! a format migration at that point would invalidate every committed
//! reproducer. So: no positional meaning, no shorthand that only reads well by
//! hand, and every optional key genuinely optional.
//!
//! # Why unknown keys are refused
//!
//! `deny_unknown_fields` throughout. A misspelled `max_delivers` is a startup
//! error and not a setting that silently never applied. The failure mode this
//! prevents is specific and nasty: a scenario that quietly permits no faults
//! passes 10,000 seeds and reports that the service is fine.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::schedule::FaultKind;

/// A parsed scenario file.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Identifies this scenario in traces, reports and CI output. Must be
    /// stable: it is what a committed reproducer is filed under.
    pub name: String,

    /// The service under test. A list because a scenario may need more than one
    /// process, and the interesting failures in a distributed system need at
    /// least two.
    #[serde(default)]
    pub system: Vec<System>,

    #[serde(default)]
    pub deps: Deps,

    #[serde(default)]
    pub workload: Vec<WorkloadStep>,

    #[serde(default)]
    pub faults: Faults,

    #[serde(default)]
    pub invariants: Vec<InvariantSpec>,

    #[serde(default)]
    pub run: RunSettings,

    /// Recorded vendor behaviours this scenario wants applied.
    ///
    /// Names only. What they mean comes from a
    /// [`CorpusSource`](crate::corpus::CorpusSource), so a scenario is portable
    /// between a team's own corpus and any other, and naming a behaviour that
    /// the corpus in use does not have is an error rather than a silent no-op.
    #[serde(default)]
    pub vendors: std::collections::BTreeMap<String, VendorSpec>,

    /// BLAKE3 of the file this was read from, set by [`Scenario::load`].
    ///
    /// Not a key: `#[serde(skip)]`, so it cannot be written or forged in the
    /// file itself. It exists so a result can say which scenario it attests to,
    /// which is the difference between a report and an assertion when the
    /// reader is an auditor rather than the person who ran it.
    #[serde(skip)]
    pub digest: Option<String>,
}

/// The behaviours a scenario wants from one vendor.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VendorSpec {
    #[serde(default)]
    pub behaviors: Vec<String>,
}

/// A process to start and proxy.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct System {
    /// The command line, as it would be typed.
    pub run: String,

    /// How to tell it is ready to receive work.
    ///
    /// Starting the workload before the service is listening produces a failure
    /// that is entirely the harness's fault, and one false failure costs more
    /// trust than several missed bugs.
    #[serde(default)]
    pub ready_when: Ready,

    /// Working directory. Relative paths in `run` resolve against it.
    #[serde(default)]
    pub cwd: Option<PathBuf>,

    /// Extra environment. The dependency addresses are injected on top of this
    /// and cannot be overridden here: pointing the service at the real
    /// dependency instead of the proxy would leave the run looking healthy and
    /// testing nothing.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// How readiness is detected.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Ready {
    /// The first connection through any proxy. The default, and right for most
    /// services: a process that has connected to its database is up.
    #[default]
    FirstConnection,
    /// A NATS `SUB` has crossed the proxy.
    NatsSubscriptionActive,
    /// A Postgres startup message has been answered.
    PostgresConnected,
    /// Assume ready immediately. For a service whose first act is to receive.
    Immediate,
}

impl Ready {
    /// The name as written in the scenario file.
    ///
    /// `mis check` echoes this back, so it has to be the string a user would
    /// type. Printing the `Debug` form instead would show `NatsSubscriptionActive`
    /// for a key they wrote as `nats_subscription_active`, and leave them
    /// wondering which one is real.
    pub fn as_str(self) -> &'static str {
        match self {
            Ready::FirstConnection => "first_connection",
            Ready::NatsSubscriptionActive => "nats_subscription_active",
            Ready::PostgresConnected => "postgres_connected",
            Ready::Immediate => "immediate",
        }
    }
}

impl std::fmt::Display for Ready {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The dependencies to start.
///
/// Every one of them is real in Phase 1: real Postgres, real NATS. Fidelity is
/// free and unarguable at this stage, and it matters more than speed. Phase 3
/// adds simulated peers for the cases a proxy structurally cannot reach, and
/// even then the real container stays, because that is what the simulator is
/// diffed against.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Deps {
    #[serde(default)]
    pub nats: Option<Nats>,

    #[serde(default)]
    pub postgres: Option<Postgres>,
}

impl Deps {
    /// Which dependencies this scenario declares, in a stable order.
    pub fn declared(&self) -> Vec<&'static str> {
        let mut names = Vec::new();

        if self.nats.is_some() {
            names.push("nats");
        }
        if self.postgres.is_some() {
            names.push("postgres");
        }

        names
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Nats {
    /// Overrides the pinned default. Pin it in the scenario when a vendor's
    /// behaviour is version-specific, which is most of the time it matters.
    #[serde(default)]
    pub image: Option<String>,

    #[serde(default)]
    pub streams: Vec<Stream>,
}

/// A JetStream stream and the consumer semantics that go with it.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    pub name: String,
    pub subjects: Vec<String>,

    /// Deliveries before the server gives up. The built-in
    /// `max_deliver_respected` invariant checks the server honours it, and
    /// `no_infinite_redelivery` checks the service does not defeat it by
    /// republishing.
    #[serde(default = "default_max_deliver")]
    pub max_deliver: u32,

    #[serde(default = "default_ack_wait", with = "humantime_serde")]
    pub ack_wait: Duration,

    #[serde(default)]
    pub discard: Discard,

    /// Where `discard = "old"` starts silently dropping. Unset means no bound,
    /// which is the wrong default for a test: a stream that cannot fill cannot
    /// reproduce the failure that happens when it does.
    #[serde(default)]
    pub max_bytes: Option<u64>,

    /// The consumer to create. Defaults to `<name>_WORKER`.
    #[serde(default)]
    pub consumer: Option<String>,

    /// The consumer's filter. Defaults to the first subject.
    ///
    /// Worth setting explicitly: a filter of `ledger.>` matches the stream's
    /// own dead letter subject, which is the redelivery loop the built-in
    /// `consumer_filter_excludes_dead_letter` invariant exists to catch.
    #[serde(default)]
    pub filter_subject: Option<String>,
}

fn default_max_deliver() -> u32 {
    5
}

fn default_ack_wait() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Discard {
    /// Drop the oldest message to make room, silently. The one that surprises
    /// people.
    Old,
    /// Refuse the write.
    #[default]
    New,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Postgres {
    #[serde(default)]
    pub image: Option<String>,

    /// Directory of `.sql` files, applied in filename order before the system
    /// starts.
    #[serde(default)]
    pub migrations: Option<PathBuf>,

    #[serde(default = "default_database")]
    pub database: String,
}

fn default_database() -> String {
    "misorder".to_string()
}

/// One step of the workload driven at the service.
///
/// Deliberately not an untagged enum. Serde reports an untagged mismatch as
/// "data did not match any variant of untagged enum WorkloadStep", pointing at
/// the whole table, which is useless when the actual mistake is a typo in one
/// key. The file is the onboarding, so its error messages are part of the
/// product: these fields are all optional and [`WorkloadStep::resolve`] says
/// exactly what is wrong.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkloadStep {
    /// NATS subject to publish to.
    #[serde(default)]
    pub publish: Option<String>,

    /// Body for `publish`, or for `post`. Encoded as JSON on the wire.
    #[serde(default)]
    pub payload: Option<toml::Value>,

    /// HTTP path to POST to.
    #[serde(default)]
    pub post: Option<String>,

    /// Do nothing for this long.
    ///
    /// Wall clock in Phase 1, virtual in Phase 3. A scenario using this to
    /// order two events is relying on timing and will be flaky; use it to let
    /// the system settle, not to sequence.
    #[serde(default, with = "humantime_serde")]
    pub wait: Option<Duration>,

    /// Repeat this step. Each repetition is a separate set of forks, so ten
    /// publishes give the scheduler ten independent chances.
    #[serde(default = "one")]
    pub repeat: usize,
}

fn one() -> usize {
    1
}

/// What a workload step turned out to mean.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Publish { subject: String, payload: Vec<u8> },
    Post { path: String, body: Vec<u8> },
    Wait(Duration),
}

impl WorkloadStep {
    /// Works out which kind of step this is, and says so precisely when it is
    /// none of them.
    pub fn resolve(&self) -> Result<Step> {
        let payload = || -> Result<Vec<u8>> {
            match &self.payload {
                Some(value) => serde_json::to_vec(value)
                    .map_err(|error| Error::Scenario(format!("payload is not encodable: {error}"))),
                None => Ok(Vec::new()),
            }
        };

        match (&self.publish, &self.post, self.wait) {
            (Some(subject), None, None) => Ok(Step::Publish {
                subject: subject.clone(),
                payload: payload()?,
            }),
            (None, Some(path), None) => Ok(Step::Post {
                path: path.clone(),
                body: payload()?,
            }),
            (None, None, Some(wait)) => Ok(Step::Wait(wait)),
            (None, None, None) => Err(Error::Scenario(
                "a [[workload]] step needs one of `publish`, `post` or `wait`".to_string(),
            )),
            _ => Err(Error::Scenario(
                "a [[workload]] step sets more than one of `publish`, `post` and `wait`; \
                 split it into separate steps"
                    .to_string(),
            )),
        }
    }
}

/// Which faults the scheduler may inject.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Faults {
    /// Empty means none, and a run with none is a useful baseline rather than a
    /// misconfiguration: if the scenario fails with nothing enabled, the bug was
    /// never about ordering.
    #[serde(default)]
    pub enabled: Vec<FaultKind>,
}

/// An assertion, either one that ships with an adapter or one the user wrote.
///
/// Same shape decision as [`WorkloadStep`], for the same reason.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvariantSpec {
    /// Names a built-in. See [`invariant::builtin`](crate::invariant::builtin).
    #[serde(default)]
    pub builtin: Option<String>,

    /// Names a user invariant. Appears in the report, so make it a sentence
    /// about the domain: `fills_never_exceed_order_qty`.
    #[serde(default)]
    pub name: Option<String>,

    /// How a user invariant is checked. Currently only `sql`.
    #[serde(default)]
    pub check: Option<CheckKind>,

    #[serde(default)]
    pub query: Option<String>,

    #[serde(default)]
    pub expect: Option<Expect>,

    /// Window for the built-ins that need one, such as
    /// `no_infinite_redelivery`.
    #[serde(default, with = "humantime_serde")]
    pub window: Option<Duration>,

    /// Threshold for `no_infinite_redelivery`: identical payloads within
    /// `window` before it is a loop rather than a retry.
    #[serde(default)]
    pub same_payload_max: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckKind {
    /// Run a query against the scenario's Postgres once the system is
    /// quiescent.
    Sql,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Expect {
    /// The query must return no rows. The natural shape for "find me a
    /// violation": the query looks for the bad state, and finding any is the
    /// failure.
    #[default]
    Empty,
    NonEmpty,
}

/// Run-wide limits.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunSettings {
    /// Hard ceiling on one run. A scenario that hits this is reported as a
    /// harness timeout, never as an invariant violation.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,

    /// Time for the dependencies and the system to come up.
    #[serde(default = "default_ready_timeout", with = "humantime_serde")]
    pub ready_timeout: Duration,

    /// Idle time with no proxied traffic before the system counts as
    /// quiescent.
    ///
    /// A heuristic, and a conservative one. Calling quiescence during a 40ms
    /// CPU burst manufactures a failure that never happened. Phase 3 replaces
    /// this with real idleness detection; until then, too long is the safe
    /// direction to be wrong in.
    #[serde(default = "default_quiesce_after", with = "humantime_serde")]
    pub quiesce_after: Duration,
}

fn default_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_ready_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_quiesce_after() -> Duration {
    Duration::from_secs(2)
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            timeout: default_timeout(),
            ready_timeout: default_ready_timeout(),
            quiesce_after: default_quiesce_after(),
        }
    }
}

/// A scenario that has been validated and had its defaults applied.
///
/// Separate from [`Scenario`] so nothing downstream has to re-check what the
/// parser already checked, and so the components take a type that cannot
/// represent a contradiction.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved {
    pub name: String,

    /// BLAKE3 of the scenario file, when it came from one.
    pub digest: Option<String>,

    /// Vendor to behaviour names, still unresolved: binding them to a corpus
    /// happens at run time, because the same scenario runs against different
    /// corpora.
    pub vendors: std::collections::BTreeMap<String, Vec<String>>,

    pub system: Vec<System>,
    pub deps: Deps,
    pub workload: Vec<Step>,
    pub faults: Vec<FaultKind>,
    pub invariants: Vec<InvariantSpec>,
    pub run: RunSettings,
}

impl Scenario {
    /// Reads and parses a scenario file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let text = std::fs::read_to_string(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => {
                Error::NotFound(format!("no scenario file at {}", path.display()))
            }
            _ => Error::Io(error),
        })?;

        let mut scenario = Self::parse(&text).map_err(|error| match error {
            Error::Scenario(message) => Error::Scenario(format!("{}: {message}", path.display())),
            other => other,
        })?;

        // Over the bytes on disk, not over the parsed structure. Two files that
        // parse identically but differ in a comment are different artifacts,
        // and a comment is exactly where someone records why a value is what it
        // is. An attestation that ignored that would attest to less than the
        // reader thinks.
        scenario.digest = Some(blake3::hash(text.as_bytes()).to_hex()[..32].to_string());

        Ok(scenario)
    }

    pub fn parse(text: &str) -> Result<Self> {
        toml::from_str(text).map_err(|error| Error::Scenario(error.to_string()))
    }

    /// Validates and applies defaults.
    ///
    /// Everything that can be caught without starting a container is caught
    /// here, because `mis check` is the fast feedback loop and a mistake found
    /// after a 30 second container start is a mistake found too late.
    pub fn resolve(&self) -> Result<Resolved> {
        if self.name.trim().is_empty() {
            return Err(Error::Scenario("`name` is empty".to_string()));
        }

        if self.system.is_empty() {
            return Err(Error::Scenario(
                "no [[system]] block: nothing to test".to_string(),
            ));
        }

        if self.invariants.is_empty() {
            return Err(Error::Scenario(
                "no [[invariants]]: a run that asserts nothing cannot fail".to_string(),
            ));
        }

        let mut seen = HashSet::new();
        for stream in self.deps.nats.iter().flat_map(|nats| &nats.streams) {
            if !seen.insert(&stream.name) {
                return Err(Error::Scenario(format!(
                    "two streams named `{}`",
                    stream.name
                )));
            }

            if stream.subjects.is_empty() {
                return Err(Error::Scenario(format!(
                    "stream `{}` has no subjects",
                    stream.name
                )));
            }

            if stream.max_deliver == 0 {
                return Err(Error::Scenario(format!(
                    "stream `{}` has max_deliver = 0, so nothing is ever delivered",
                    stream.name
                )));
            }
        }

        // Faults that can never fire are refused rather than ignored. A
        // scenario permitting `hold_statement` with no Postgres is not a
        // harmless no-op: it reads as covering an interleaving it never
        // explores.
        for fault in &self.faults.enabled {
            let needs = match fault {
                FaultKind::AckTimeout | FaultKind::SwallowAck | FaultKind::Redelivery => {
                    Some("nats")
                }
                FaultKind::HoldStatement => Some("postgres"),
                _ => None,
            };

            if let Some(dependency) = needs
                && !self.deps.declared().contains(&dependency)
            {
                return Err(Error::Scenario(format!(
                    "fault `{fault}` needs a [deps.{dependency}] block, which this scenario \
                     does not declare"
                )));
            }
        }

        let workload = self
            .workload
            .iter()
            .flat_map(|step| std::iter::repeat_n(step, step.repeat.max(1)))
            .map(WorkloadStep::resolve)
            .collect::<Result<Vec<_>>>()?;

        for invariant in &self.invariants {
            validate_invariant(invariant)?;
        }

        Ok(Resolved {
            name: self.name.clone(),
            digest: self.digest.clone(),
            vendors: self
                .vendors
                .iter()
                .map(|(vendor, spec)| (vendor.clone(), spec.behaviors.clone()))
                .collect(),
            system: self.system.clone(),
            deps: self.deps.clone(),
            workload,
            faults: self.faults.enabled.clone(),
            invariants: self.invariants.clone(),
            run: self.run.clone(),
        })
    }
}

fn validate_invariant(spec: &InvariantSpec) -> Result<()> {
    match (&spec.builtin, &spec.name) {
        (Some(builtin), None) => {
            if crate::invariant::builtin::is_known(builtin) {
                Ok(())
            } else {
                Err(Error::Scenario(format!(
                    "unknown builtin invariant `{builtin}`; available: {}",
                    crate::invariant::builtin::names().join(", ")
                )))
            }
        }
        (None, Some(name)) => match spec.check {
            Some(CheckKind::Sql) if spec.query.is_some() => Ok(()),
            Some(CheckKind::Sql) => Err(Error::Scenario(format!(
                "invariant `{name}` has `check = \"sql\"` but no `query`"
            ))),
            None => Err(Error::Scenario(format!(
                "invariant `{name}` has no `check`; use `check = \"sql\"`"
            ))),
        },
        (Some(builtin), Some(name)) => Err(Error::Scenario(format!(
            "invariant sets both `builtin = \"{builtin}\"` and `name = \"{name}\"`; \
             a built-in is named by its `builtin` key"
        ))),
        (None, None) => Err(Error::Scenario(
            "an [[invariants]] block needs either `builtin` or `name`".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
name = "minimal"

[[system]]
run = "./service"

[[invariants]]
builtin = "eventually_quiescent"
"#;

    fn parse(text: &str) -> Result<Resolved> {
        Scenario::parse(text)?.resolve()
    }

    #[test]
    fn the_readme_scenario_parses_and_resolves() {
        let text = r#"
name = "dead_letter_no_redelivery"

[[system]]
run = "./target/debug/ledger"
ready_when = "nats_subscription_active"

[[deps.nats.streams]]
name = "LEDGER"
subjects = ["ledger.>"]
max_deliver = 5
ack_wait = "30s"
discard = "old"

[deps.postgres]
migrations = "./migrations"

[[workload]]
publish = "ledger.org.org_1.account.acct_1.order"
payload = { order_id = "ord_1", kind = "fill", qty = 100 }

[faults]
enabled = ["ack_timeout", "redelivery", "connection_drop", "reorder"]

[[invariants]]
builtin = "no_infinite_redelivery"
window = "5m"
same_payload_max = 10
"#;

        let resolved = parse(text).expect("resolve");

        assert_eq!(resolved.name, "dead_letter_no_redelivery");
        assert_eq!(resolved.faults.len(), 4);
        assert_eq!(resolved.deps.declared(), vec!["nats", "postgres"]);
        assert!(matches!(resolved.workload[0], Step::Publish { .. }));
    }

    #[test]
    fn a_misspelled_key_is_a_startup_error() {
        let text = MINIMAL.replace("run = \"./service\"", "runn = \"./service\"");

        let error = parse(&text).expect_err("should refuse");

        assert!(matches!(error, Error::Scenario(_)), "got {error:?}");
    }

    #[test]
    fn a_scenario_that_asserts_nothing_is_refused() {
        let text = r#"
name = "empty"

[[system]]
run = "./service"
"#;

        let error = parse(text).expect_err("should refuse");

        assert!(error.to_string().contains("cannot fail"), "got {error}");
    }

    #[test]
    fn a_scenario_with_nothing_to_test_is_refused() {
        let text = r#"
name = "empty"

[[invariants]]
builtin = "eventually_quiescent"
"#;

        let error = parse(text).expect_err("should refuse");

        assert!(error.to_string().contains("nothing to test"), "got {error}");
    }

    #[test]
    fn a_fault_with_no_dependency_to_apply_to_is_refused() {
        let text = format!("{MINIMAL}\n[faults]\nenabled = [\"hold_statement\"]\n");

        let error = parse(&text).expect_err("should refuse");

        assert!(
            error.to_string().contains("[deps.postgres]"),
            "the message should name the missing block, got {error}"
        );
    }

    #[test]
    fn an_unknown_builtin_lists_the_known_ones() {
        let text = MINIMAL.replace("eventually_quiescent", "no_such_invariant");

        let error = parse(&text).expect_err("should refuse");

        assert!(
            error.to_string().contains("eventually_quiescent"),
            "the message should list what is available, got {error}"
        );
    }

    #[test]
    fn a_user_invariant_without_a_query_says_so() {
        let text = format!(
            "{MINIMAL}\n[[invariants]]\nname = \"fills_never_exceed_qty\"\ncheck = \"sql\"\n"
        );

        let error = parse(&text).expect_err("should refuse");

        assert!(error.to_string().contains("no `query`"), "got {error}");
    }

    #[test]
    fn a_workload_step_with_two_actions_is_refused() {
        let text = format!("{MINIMAL}\n[[workload]]\npublish = \"a.b\"\npost = \"/orders\"\n");

        let error = parse(&text).expect_err("should refuse");

        assert!(error.to_string().contains("split it"), "got {error}");
    }

    #[test]
    fn repeat_expands_into_separate_steps() {
        let text = format!("{MINIMAL}\n[[workload]]\npublish = \"a.b\"\nrepeat = 3\n");

        let resolved = parse(&text).expect("resolve");

        assert_eq!(resolved.workload.len(), 3);
    }

    #[test]
    fn ready_names_round_trip_through_toml() {
        for ready in [
            Ready::FirstConnection,
            Ready::NatsSubscriptionActive,
            Ready::PostgresConnected,
            Ready::Immediate,
        ] {
            let text = MINIMAL.replace(
                "run = \"./service\"",
                &format!("run = \"./service\"\nready_when = \"{ready}\""),
            );

            assert_eq!(parse(&text).expect("resolve").system[0].ready_when, ready);
        }
    }

    #[test]
    fn defaults_are_applied_where_the_file_is_silent() {
        let resolved = parse(MINIMAL).expect("resolve");

        assert_eq!(resolved.run.timeout, Duration::from_secs(60));
        assert_eq!(resolved.system[0].ready_when, Ready::FirstConnection);
        assert!(resolved.faults.is_empty());
    }

    #[test]
    fn a_stream_that_delivers_nothing_is_refused() {
        let text = format!(
            "{MINIMAL}\n[[deps.nats.streams]]\nname = \"S\"\nsubjects = [\"s.>\"]\n\
             max_deliver = 0\n"
        );

        let error = parse(&text).expect_err("should refuse");

        assert!(error.to_string().contains("max_deliver"), "got {error}");
    }

    #[test]
    fn a_scenario_names_the_vendor_behaviours_it_wants() {
        let text = format!(
            "{MINIMAL}\n[vendors.lightspeed]\nbehaviors = [\"no_ack_on_second_replace\"]\n"
        );

        let resolved = parse(&text).expect("resolve");

        assert_eq!(
            resolved.vendors.get("lightspeed").map(Vec::as_slice),
            Some(["no_ack_on_second_replace".to_string()].as_slice())
        );
    }

    #[test]
    fn a_scenario_parsed_from_a_string_has_no_digest_to_attest_to() {
        assert!(Scenario::parse(MINIMAL).expect("parse").digest.is_none());
    }

    #[test]
    fn loading_a_file_digests_its_bytes_including_comments() {
        let directory = tempfile::tempdir().expect("tempdir");

        let plain = directory.path().join("plain.toml");
        let commented = directory.path().join("commented.toml");

        std::fs::write(&plain, MINIMAL).expect("write");
        std::fs::write(&commented, format!("# why this value\n{MINIMAL}")).expect("write");

        let plain = Scenario::load(&plain).expect("load").digest;
        let commented = Scenario::load(&commented).expect("load").digest;

        assert!(plain.is_some());
        assert_ne!(plain, commented, "a comment is part of the artifact");
    }

    #[test]
    fn a_missing_file_reports_where_it_looked() {
        let error = Scenario::load("/nonexistent/scenario.toml").expect_err("should fail");

        assert!(
            error.to_string().contains("/nonexistent/scenario.toml"),
            "got {error}"
        );
    }
}

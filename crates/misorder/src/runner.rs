//! One run, from scenario to outcome.
//!
//! Everything else in this crate is a stage; this is what holds them together.
//! `mis run`, `mis fuzz`, `mis replay` and `mis shrink` are all this type
//! reached four ways, which is deliberate: if replay had its own path through
//! the system, the thing it reproduced would be that path.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::event::{Event, Lifecycle, Observed};
use crate::invariant::{CheckContext, Checker, Violation};
use crate::orchestrator::Environment;
use crate::orchestrator::service::{self, Service};
use crate::proxy::{Adapter, EventSink, Fleet};
use crate::report::Reproducer;
use crate::report::run::{
    self, Decisions, Engine, Faults, RunReport, ScenarioRef, ShardRef, SweepReport, Verdict,
};
use crate::scenario::file::{Resolved, Step};
use crate::schedule::{Profile, Scheduler};
use crate::shrink::{self, Oracle};
use crate::trace::Trace;
use crate::workload::Driver;

/// Where a run's decisions come from.
#[derive(Debug, Clone)]
pub enum Run {
    /// Draw a fresh schedule.
    Seed(u64),
    /// Follow a recorded one.
    Replay(Trace),
}

impl Run {
    pub fn seed(&self) -> u64 {
        match self {
            Run::Seed(seed) => *seed,
            Run::Replay(trace) => trace.seed,
        }
    }
}

/// A contiguous span of seeds.
///
/// Stated as start and count rather than passed as an iterator, because a sweep
/// has to be able to say afterwards what it was asked to cover. "Seeds 0 to
/// 10000" must mean the same set next week, and a report that could only say
/// "some seeds" is not evidence of anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seeds {
    pub start: u64,
    pub count: u64,
}

impl Seeds {
    pub fn new(start: u64, count: u64) -> Self {
        Self { start, count }
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + use<> {
        let start = self.start;

        (0..self.count).map(move |offset| start.saturating_add(offset))
    }
}

/// One slice of a seed space, for splitting a sweep across machines.
///
/// Selection is `seed % count == index`, not a contiguous range, and the
/// difference matters. Contiguous ranges give every machine a block that may be
/// entirely uninteresting or entirely failing, so one worker finishes in a
/// second and another runs for an hour. Modulo spreads the work, and it needs
/// no coordination: a machine can compute its own slice from two integers with
/// nothing to ask anybody.
///
/// That is the whole seam for running a sweep across many machines. Deciding
/// *how many* machines, starting them, collecting their reports and merging
/// them is orchestration, and orchestration is not something a stateless CLI
/// should grow. What the CLI owes an orchestrator is the ability to be told
/// "you are 7 of 64" and to do exactly that, offline, and say so in its report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    index: u64,
    count: u64,
}

impl Shard {
    pub fn new(index: u64, count: u64) -> Result<Self> {
        if count == 0 {
            return Err(Error::Scenario(
                "a shard count of 0 runs nothing".to_string(),
            ));
        }

        if index >= count {
            return Err(Error::Scenario(format!(
                "shard {index} of {count} does not exist; shards are numbered 0 to {}",
                count - 1
            )));
        }

        Ok(Self { index, count })
    }

    /// Parses `7/64`.
    pub fn parse(text: &str) -> Result<Self> {
        let (index, count) = text.split_once('/').ok_or_else(|| {
            Error::Scenario(format!("`{text}` is not a shard; write it as `7/64`"))
        })?;

        let parse = |value: &str, what: &str| -> Result<u64> {
            value
                .trim()
                .parse::<u64>()
                .map_err(|_| Error::Scenario(format!("shard {what} `{value}` is not a number")))
        };

        Shard::new(parse(index, "index")?, parse(count, "count")?)
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn contains(&self, seed: u64) -> bool {
        seed % self.count == self.index
    }

    fn as_ref(&self) -> ShardRef {
        ShardRef {
            index: self.index,
            count: self.count,
        }
    }
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.index, self.count)
    }
}

/// What one run produced.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub scenario: String,
    pub seed: u64,

    /// Everything decided, whether or not anything failed. A passing run's
    /// trace is worth keeping: it is what a later failure gets diffed against.
    pub trace: Trace,

    pub violations: Vec<Violation>,
    pub events: Vec<Observed>,
    pub elapsed: Duration,

    /// What the scenario declared, carried so a reproducer can name what was
    /// *not* involved.
    ///
    /// On the outcome rather than looked up from the scenario at render time,
    /// because a reproducer is also rendered from a trace loaded off disk in a
    /// later process, and the negative space is the half of the report that
    /// tells a reader which of their systems to stop reading.
    pub declared_deps: Vec<String>,
    pub declared_faults: Vec<crate::schedule::FaultKind>,

    /// BLAKE3 of the scenario file, when it came from one.
    pub scenario_digest: Option<String>,

    /// RFC3339, UTC.
    pub started_at: String,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// The failure, rendered.
    ///
    /// The first violation, not all of them. Two invariants firing in one
    /// ordering usually means one caused the other, and a report that led with
    /// the consequence would send the reader to the wrong place.
    pub fn failure(&self) -> Option<Reproducer> {
        self.reproducer(self.trace.active_count())
    }

    /// The machine-readable form.
    ///
    /// The one document anything downstream reads. Never [`Verdict::Incomplete`]:
    /// an `Outcome` exists only for a run that finished, and a run that could
    /// not start is an `Err` that never becomes one of these.
    ///
    /// The signature is only meaningful on a shrunk trace, so it is taken from
    /// whatever trace this outcome holds and is the caller's job to have shrunk
    /// first. Signing an unshrunk trace would give every seed its own identity
    /// and defeat the grouping it exists for.
    pub fn report(&self) -> RunReport {
        let violations = self.violations.iter().map(Into::into).collect();

        let used: Vec<crate::schedule::FaultKind> = {
            let mut used: Vec<_> = self
                .trace
                .active()
                .filter_map(|record| record.decision.fault_kind())
                .collect();
            used.sort_unstable();
            used.dedup();
            used
        };

        let mut report = RunReport {
            format: run::FORMAT_VERSION,
            engine: Engine::default(),
            scenario: ScenarioRef {
                name: self.scenario.clone(),
                digest: self.scenario_digest.clone(),
            },
            seed: self.seed,
            verdict: if self.passed() {
                Verdict::Passed
            } else {
                Verdict::Violated
            },
            signature: (!self.passed()).then(|| self.trace.signature()),
            violations,
            decisions: Decisions {
                recorded: self.trace.records.len(),
                active: self.trace.active_count(),
            },
            faults: Faults {
                permitted: self.declared_faults.clone(),
                used,
            },
            dependencies: self
                .declared_deps
                .iter()
                .map(|name| crate::report::run::DependencyRecord {
                    name: name.clone(),
                    image: None,
                    digest: None,
                })
                .collect(),
            reproducer: None,
            started_at: self.started_at.clone(),
            elapsed_ms: run::millis(self.elapsed),
        };

        if let Some(reproducer) = self.failure() {
            run::from_reproducer(&mut report, &reproducer);
        }

        report
    }

    /// The failure, rendered against a stated original decision count.
    ///
    /// `mis run --shrink` replays a shrunk trace to render its reproducer, and
    /// that replay's own decision count is the shrunk one. Passing the count
    /// from before shrinking is what makes the report say "6 of 847" rather
    /// than "6 of 6", which is the line that shows the shrinker did anything.
    pub fn reproducer(&self, original_decisions: usize) -> Option<Reproducer> {
        let violation = self.violations.first()?.clone();
        let declared: Vec<&str> = self.declared_deps.iter().map(String::as_str).collect();

        Some(Reproducer::build(
            &self.trace,
            violation,
            &self.events,
            &declared,
            &self.declared_faults,
            original_decisions,
        ))
    }
}

/// What a sweep of seeds found.
#[derive(Debug, Clone)]
pub struct FuzzReport {
    pub scenario: String,
    pub scenario_digest: Option<String>,
    pub shard: Option<Shard>,

    /// What this process was asked for, before sharding.
    pub seeds: Seeds,
    /// What it actually ran. Lower than `seeds.count` under sharding.
    pub seeds_run: u64,

    pub passed: u64,
    /// Runs that could not complete. Counted separately from passes, because a
    /// sweep where half the runs never started must not read as a clean one.
    pub incomplete: u64,

    /// Failing runs, by ascending seed.
    pub failures: Vec<Outcome>,

    pub started_at: String,
    pub elapsed: Duration,
}

impl FuzzReport {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    /// Whether every seed this process was responsible for actually ran.
    pub fn is_complete(&self) -> bool {
        self.incomplete == 0
    }

    /// The machine-readable form, with failures grouped by signature.
    ///
    /// `reports` is passed in rather than derived from `failures`, because the
    /// useful signature comes from a *shrunk* trace and shrinking happens after
    /// the sweep. A caller that has not shrunk passes the unshrunk reports and
    /// gets grouping that is honest about being per-seed.
    pub fn to_report(&self, reports: Vec<RunReport>) -> SweepReport {
        SweepReport {
            format: run::FORMAT_VERSION,
            engine: Engine::default(),
            scenario: ScenarioRef {
                name: self.scenario.clone(),
                digest: self.scenario_digest.clone(),
            },
            shard: self.shard.map(|shard| shard.as_ref()),
            seed_start: self.seeds.start,
            seed_count: self.seeds.count,
            seeds_run: self.seeds_run,
            passed: self.passed,
            violated: reports.len() as u64,
            incomplete: self.incomplete,
            distinct_failures: SweepReport::group(&reports),
            failures: reports,
            started_at: self.started_at.clone(),
            elapsed_ms: run::millis(self.elapsed),
        }
    }
}

/// Runs a scenario.
#[derive(Debug, Clone)]
pub struct Runner {
    scenario: Resolved,
    profile: Profile,
}

impl Runner {
    pub fn new(scenario: Resolved) -> Self {
        Self {
            scenario,
            profile: Profile::default(),
        }
    }

    pub fn with_profile(mut self, profile: Profile) -> Self {
        self.profile = profile;
        self
    }

    pub fn scenario(&self) -> &Resolved {
        &self.scenario
    }

    /// Executes one run.
    ///
    /// The order of the stages is load-bearing and is the same on every path
    /// through the tool: dependencies, topology, proxies, service, workload,
    /// quiescence, checks. Anything that started earlier than it should would
    /// see a half-built world, and the timing of that is not something the
    /// scheduler controls.
    pub async fn execute(&self, run: Run) -> Result<Outcome> {
        let started = Instant::now();
        let started_at = run::now_rfc3339();
        let cancel = CancellationToken::new();
        let (events, mut receiver) = EventSink::new();

        let scheduler = match &run {
            Run::Seed(seed) => Scheduler::seeded(
                *seed,
                self.scenario.faults.clone(),
                self.profile,
                &self.scenario.name,
            ),
            Run::Replay(trace) => Scheduler::replaying(trace),
        };

        let mut checker = Checker::new(crate::invariant::build_all(&self.scenario)?);

        let environment = Environment::start(&self.scenario.deps, &self.scenario.run).await?;

        // From here on the environment must be torn down whatever happens, so
        // the rest is one fallible block and the teardown is unconditional. A
        // run that failed and leaked a Postgres container makes the next run
        // fail too, and the second failure is the one the user reports.
        let outcome = self
            .drive(&environment, &scheduler, &events, &cancel, started)
            .await;

        cancel.cancel();
        drop(events);

        let mut collected = Vec::new();
        while let Some(observed) = receiver.recv().await {
            checker.observe(&observed);
            collected.push(observed);
        }

        let context = CheckContext {
            postgres_url: self
                .scenario
                .deps
                .postgres
                .as_ref()
                .and_then(|postgres| environment.postgres_url(&postgres.database)),
            elapsed: started.elapsed(),
        };

        let finished = checker.finish(&context).await;

        environment.stop().await;

        outcome?;
        finished?;

        Ok(Outcome {
            scenario: self.scenario.name.clone(),
            seed: run.seed(),
            trace: scheduler.trace(),
            violations: checker.violations().to_vec(),
            events: collected,
            elapsed: started.elapsed(),
            declared_deps: self
                .scenario
                .deps
                .declared()
                .into_iter()
                .map(str::to_string)
                .collect(),
            declared_faults: self.scenario.faults.clone(),
            scenario_digest: self.scenario.digest.clone(),
            started_at,
        })
    }

    /// Starts the proxies and the service, drives the workload, waits for the
    /// system to settle.
    ///
    /// Ports first, then proxies, then the service, and the order is not a
    /// preference. An ingress proxy forwards to the service, so it cannot bind
    /// until the service's address is settled; the service is pointed at its
    /// dependencies through the environment the proxies produced, so it cannot
    /// start until they have bound.
    async fn drive(
        &self,
        environment: &Environment,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
        started: Instant,
    ) -> Result<()> {
        let mut addresses = Vec::with_capacity(self.scenario.system.len());

        for _ in &self.scenario.system {
            addresses.push(service::reserve_port().await?);
        }

        let fleet = Fleet::start(self.adapters(&addresses)?, scheduler, events, cancel).await?;

        let mut services = Vec::with_capacity(self.scenario.system.len());
        let mut failed_to_start = None;

        for (system, address) in self.scenario.system.iter().zip(&addresses) {
            match Service::start(system, *address, &fleet.env(), &self.scenario.run).await {
                Ok(service) => {
                    events.emit_lifecycle(
                        started.elapsed(),
                        Event::Lifecycle(Lifecycle::SystemStarted {
                            command: system.run.clone(),
                        }),
                    );

                    services.push(service);
                }
                Err(error) => {
                    failed_to_start = Some(error);
                    break;
                }
            }
        }

        let driven = match failed_to_start {
            Some(error) => Err(error),
            None => {
                self.drive_workload(environment, events, &fleet, started)
                    .await
            }
        };

        // Unconditional from here. Every adapter holds a clone of the event
        // sink, so a fleet left unjoined is a run that never finishes reading
        // its own events, and a service left running holds the port the next
        // run is about to be handed.
        cancel.cancel();

        let served = fleet.stop().await;

        for service in services {
            service.stop().await;
        }

        driven?;
        served
    }

    async fn drive_workload(
        &self,
        environment: &Environment,
        events: &EventSink,
        fleet: &Fleet,
        started: Instant,
    ) -> Result<()> {
        let mut driver = Driver::new(environment, events);

        if let Some(endpoint) = fleet.endpoint("http") {
            driver = driver.through_ingress(endpoint.listen);
        }

        driver
            .run(&self.scenario.workload, started.elapsed())
            .await?;

        self.await_quiescence(events, started).await
    }

    /// Which proxies this run needs.
    ///
    /// Read off the workload rather than declared in the file. A scenario with
    /// a `post` step has a front door and one without has nothing to sit in
    /// front of, so the file already says it and a key asking again would be a
    /// second place for the answer to be wrong.
    fn adapters(&self, addresses: &[SocketAddr]) -> Result<Vec<(Box<dyn Adapter>, String)>> {
        let mut adapters: Vec<(Box<dyn Adapter>, String)> = Vec::new();

        let posts = self
            .scenario
            .workload
            .iter()
            .any(|step| matches!(step, Step::Post { .. }));

        if let (true, Some(address)) = (posts, addresses.first()) {
            #[cfg(feature = "http")]
            adapters.push((
                Box::new(crate::proxy::http::HttpAdapter::new()),
                address.to_string(),
            ));

            // Refused rather than run without it. A post that went straight at
            // the service would explore no ordering, and the run would pass
            // having tested nothing, which is the failure this whole format is
            // built to prevent.
            #[cfg(not(feature = "http"))]
            return Err(Error::Unsupported(format!(
                "this scenario posts to {address}, but this build has no http feature"
            )));
        }

        Ok(adapters)
    }

    /// Waits for an idle window with no proxied traffic.
    ///
    /// A heuristic, and conservative on purpose. Declaring quiescence during a
    /// 40ms CPU burst manufactures a failure that never happened, and one
    /// invented failure costs more trust than several missed real ones. Phase 3
    /// replaces this with real idleness detection, which is what gates the
    /// virtual clock: you cannot safely advance a clock without knowing the
    /// system is waiting rather than computing.
    async fn await_quiescence(&self, events: &EventSink, started: Instant) -> Result<()> {
        let deadline = self.scenario.run.timeout;

        if started.elapsed() >= deadline {
            return Err(Error::Timeout {
                what: format!("scenario `{}` never went quiescent", self.scenario.name),
                elapsed: started.elapsed(),
            });
        }

        tokio::time::sleep(self.scenario.run.quiesce_after).await;

        events.emit_lifecycle(started.elapsed(), Event::Lifecycle(Lifecycle::Quiescent));

        Ok(())
    }

    /// Runs many seeds, `parallel` at a time.
    ///
    /// Runs locally and stateless, results to stdout. Distributed search is the
    /// cloud product, and the split is structural rather than a crippled tier:
    /// nobody resents a free CLI for not containing a job scheduler.
    pub async fn fuzz(&self, seeds: Seeds, parallel: usize, shard: Option<Shard>) -> FuzzReport {
        let started = Instant::now();
        let started_at = run::now_rfc3339();

        let mine: Vec<u64> = seeds
            .iter()
            .filter(|seed| shard.is_none_or(|shard| shard.contains(*seed)))
            .collect();

        let runner = Arc::new(self.clone());

        let outcomes: Vec<_> = futures::stream::iter(mine.clone())
            .map(|seed| {
                let runner = Arc::clone(&runner);

                async move { (seed, runner.execute(Run::Seed(seed)).await) }
            })
            .buffer_unordered(parallel.max(1))
            .collect()
            .await;

        let mut failures = Vec::new();
        let mut passed = 0;
        let mut incomplete = 0;

        for (seed, result) in outcomes {
            match result {
                Ok(outcome) if outcome.passed() => passed += 1,
                Ok(outcome) => failures.push(outcome),
                Err(error) => {
                    // A harness failure is not a finding. Counted apart from
                    // both passes and failures, because presenting it as a
                    // caught bug is how a tool teaches people to ignore it, and
                    // folding it into the passes is how a sweep claims coverage
                    // it never had.
                    tracing::warn!(seed, %error, "run could not complete");
                    incomplete += 1;
                }
            }
        }

        failures.sort_by_key(|outcome| outcome.seed);

        FuzzReport {
            scenario: self.scenario.name.clone(),
            scenario_digest: self.scenario.digest.clone(),
            shard,
            seeds,
            seeds_run: mine.len() as u64,
            passed,
            incomplete,
            failures,
            started_at,
            elapsed: started.elapsed(),
        }
    }

    /// Shrinks a failing run to its minimal reproducer.
    pub async fn shrink(
        &self,
        outcome: &Outcome,
        limits: shrink::Limits,
    ) -> Result<shrink::Report> {
        let violation = outcome
            .violations
            .first()
            .ok_or_else(|| Error::Internal("shrinking a run that passed".to_string()))?;

        let mut oracle = RunOracle {
            runner: self.clone(),
            invariant: violation.invariant.clone(),
        };

        shrink::shrink(&outcome.trace, &mut oracle, limits).await
    }
}

/// Re-runs a candidate trace and reports whether the same failure survived.
///
/// Matching on the invariant name, not on "the run failed". A candidate that
/// fails for a different reason is not this failure getting smaller, and
/// accepting it would make the shrinker wander off towards whichever bug is
/// easiest to trigger.
struct RunOracle {
    runner: Runner,
    invariant: String,
}

#[async_trait]
impl Oracle for RunOracle {
    async fn still_fails(&mut self, trace: &Trace) -> Result<bool> {
        let outcome = self.runner.execute(Run::Replay(trace.clone())).await?;

        Ok(outcome
            .violations
            .iter()
            .any(|violation| violation.invariant == self.invariant))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::file::Scenario;

    const SCENARIO: &str = r#"
name = "unit"

[[system]]
run = "./service"

[[invariants]]
builtin = "eventually_quiescent"
"#;

    fn runner() -> Runner {
        Runner::new(
            Scenario::parse(SCENARIO)
                .expect("parse")
                .resolve()
                .expect("resolve"),
        )
    }

    #[test]
    fn a_shard_selects_a_spread_of_seeds_rather_than_a_block() {
        let shard = Shard::new(7, 64).expect("valid");
        let mine: Vec<u64> = Seeds::new(0, 640)
            .iter()
            .filter(|s| shard.contains(*s))
            .collect();

        assert_eq!(mine.len(), 10);
        assert_eq!(mine[0], 7);
        assert_eq!(mine[1], 71, "spread, so no worker gets an all-quiet block");
    }

    #[test]
    fn every_seed_lands_in_exactly_one_shard() {
        let shards: Vec<Shard> = (0..8).map(|i| Shard::new(i, 8).expect("valid")).collect();

        for seed in 0..500u64 {
            let owners = shards.iter().filter(|shard| shard.contains(seed)).count();

            assert_eq!(owners, 1, "seed {seed} is owned by {owners} shards");
        }
    }

    #[test]
    fn a_shard_is_written_the_way_it_reads() {
        assert_eq!(
            Shard::parse("7/64").expect("parse"),
            Shard::new(7, 64).expect("valid")
        );
        assert_eq!(Shard::parse("7/64").expect("parse").to_string(), "7/64");
        assert!(Shard::parse("7 of 64").is_err());
        assert!(Shard::parse("64/64").is_err(), "shards are zero-indexed");
        assert!(Shard::parse("0/0").is_err());
    }

    #[test]
    fn a_seed_span_means_the_same_set_every_time() {
        assert_eq!(
            Seeds::new(500, 4).iter().collect::<Vec<_>>(),
            vec![500, 501, 502, 503]
        );
    }

    #[test]
    fn a_replay_run_keeps_the_seed_of_the_trace_it_replays() {
        let trace = Trace::new(8_837_291, "unit");

        assert_eq!(Run::Replay(trace).seed(), 8_837_291);
    }

    #[tokio::test]
    async fn a_run_that_cannot_start_its_dependencies_is_an_error_not_a_finding() {
        // No Docker in the unit test environment, so this exercises the
        // distinction the whole design rests on: the harness failing is an
        // Err, and only the service under test misbehaving is a violation.
        let error = runner()
            .execute(Run::Seed(1))
            .await
            .expect_err("no containers here");

        assert!(
            matches!(error, Error::Environment(_) | Error::Unsupported(_)),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn a_harness_failure_is_not_counted_as_a_failing_seed() {
        let report = runner().fuzz(Seeds::new(1, 3), 2, None).await;

        assert_eq!(report.seeds_run, 3);
        assert_eq!(report.incomplete, 3);
        assert_eq!(report.passed, 0);
        assert!(
            report.passed(),
            "a run that could not start is not a caught bug"
        );
        assert!(
            !report.is_complete(),
            "and the sweep has to say it covered nothing"
        );
    }

    #[test]
    fn a_reproducer_names_the_dependencies_and_faults_the_failure_did_not_need() {
        // Guards the wiring, not the rendering: `Reproducer` has its own tests,
        // and this is about the outcome actually carrying what the scenario
        // declared. Passing empty slices here compiles and renders a report
        // that is silently missing half its value.
        let outcome = Outcome {
            scenario: "unit".to_string(),
            seed: 1,
            trace: Trace::new(1, "unit"),
            violations: vec![Violation {
                invariant: "eventually_quiescent".to_string(),
                detail: "still active".to_string(),
                at: Duration::from_secs(1),
            }],
            events: Vec::new(),
            elapsed: Duration::from_secs(1),
            declared_deps: vec!["nats".to_string(), "postgres".to_string()],
            declared_faults: vec![crate::schedule::FaultKind::Reorder],
            scenario_digest: Some("abc".to_string()),
            started_at: run::now_rfc3339(),
        };

        let rendered = outcome.failure().expect("a violation renders").render();

        assert!(
            rendered.contains("Nats and postgres were not involved."),
            "{rendered}"
        );
        assert!(
            rendered.contains("Fault 'reorder' was not required."),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn shrinking_a_passing_run_is_a_bug_in_the_caller() {
        let outcome = Outcome {
            scenario: "unit".to_string(),
            seed: 1,
            trace: Trace::new(1, "unit"),
            violations: Vec::new(),
            events: Vec::new(),
            elapsed: Duration::ZERO,
            declared_deps: Vec::new(),
            declared_faults: Vec::new(),
            scenario_digest: None,
            started_at: run::now_rfc3339(),
        };

        let error = runner()
            .shrink(&outcome, shrink::Limits::default())
            .await
            .expect_err("nothing to shrink");

        assert!(matches!(error, Error::Internal(_)), "got {error:?}");
    }
}

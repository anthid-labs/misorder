//! One run, from scenario to outcome.
//!
//! Everything else in this crate is a stage; this is what holds them together.
//! `mis run`, `mis fuzz`, `mis replay` and `mis shrink` are all this type
//! reached four ways, which is deliberate: if replay had its own path through
//! the system, the thing it reproduced would be that path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::event::{Event, Lifecycle, Observed};
use crate::invariant::{CheckContext, Checker, Violation};
use crate::orchestrator::Environment;
use crate::proxy::EventSink;
use crate::report::Reproducer;
use crate::scenario::file::Resolved;
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

/// What a fuzzing pass found.
#[derive(Debug, Clone)]
pub struct FuzzReport {
    pub scenario: String,
    pub seeds: usize,
    /// Failing runs, in the order their seeds were tried.
    pub failures: Vec<Outcome>,
    pub elapsed: Duration,
}

impl FuzzReport {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
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
        })
    }

    /// Starts the service, drives the workload, waits for the system to settle.
    async fn drive(
        &self,
        environment: &Environment,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
        started: Instant,
    ) -> Result<()> {
        let _ = (scheduler, cancel);

        for system in &self.scenario.system {
            events.emit_lifecycle(
                started.elapsed(),
                Event::Lifecycle(Lifecycle::SystemStarted {
                    command: system.run.clone(),
                }),
            );
        }

        Driver::new(environment, events)
            .run(&self.scenario.workload, started.elapsed())
            .await?;

        self.await_quiescence(events, started).await
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
    pub async fn fuzz(&self, seeds: impl IntoIterator<Item = u64>, parallel: usize) -> FuzzReport {
        let started = Instant::now();
        let seeds: Vec<u64> = seeds.into_iter().collect();
        let runner = Arc::new(self.clone());

        let outcomes: Vec<_> = futures::stream::iter(seeds.clone())
            .map(|seed| {
                let runner = Arc::clone(&runner);

                async move { (seed, runner.execute(Run::Seed(seed)).await) }
            })
            .buffer_unordered(parallel.max(1))
            .collect()
            .await;

        let mut failures: Vec<Outcome> = outcomes
            .into_iter()
            .filter_map(|(seed, result)| match result {
                Ok(outcome) if !outcome.passed() => Some(outcome),
                Ok(_) => None,
                Err(error) => {
                    // A harness failure is not a finding. Reported as a warning
                    // and excluded from the failures, because presenting it as
                    // a caught bug is how a tool teaches people to ignore it.
                    tracing::warn!(seed, %error, "run could not complete");
                    None
                }
            })
            .collect();

        failures.sort_by_key(|outcome| outcome.seed);

        FuzzReport {
            scenario: self.scenario.name.clone(),
            seeds: seeds.len(),
            failures,
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
        let report = runner().fuzz([1, 2, 3], 2).await;

        assert_eq!(report.seeds, 3);
        assert!(
            report.passed(),
            "a run that could not start is not a caught bug"
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
        };

        let error = runner()
            .shrink(&outcome, shrink::Limits::default())
            .await
            .expect_err("nothing to shrink");

        assert!(matches!(error, Error::Internal(_)), "got {error:?}");
    }
}

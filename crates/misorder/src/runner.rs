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
use crate::orchestrator::process::Service;
use crate::proxy::EventSink;
#[cfg(feature = "http")]
use crate::proxy::http::HttpAdapter;
// Any adapter needs these, not just the HTTP one. Gated on the set rather than
// unconditionally, because a build with no protocol feature binds no adapter
// and would carry an unused import.
//
// The set is every protocol `bind_ingress` and `bind_egress` can bind, and it
// has to stay that way: leaving `nats` out of it built under the default
// features - where `http` carries the import - and failed only for an embedder
// who took the NATS adapter on its own.
#[cfg(any(feature = "http", feature = "nats", feature = "redis"))]
use crate::proxy::{Adapter, ProxyContext};
use crate::report::Reproducer;
use crate::report::run::{
    self, Decisions, Engine, Faults, RunReport, ScenarioRef, ShardRef, SweepReport, Verdict,
};
use crate::scenario::file::{Ready, Resolved, Step};
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

/// How far along a sweep is.
///
/// Handed to [`Runner::fuzz_with`]'s callback as each run finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Runs finished, including the one being reported.
    pub completed: u64,
    /// Runs this process will do. Already sharded, so it is what this machine
    /// is responsible for rather than what the sweep was asked for.
    pub total: u64,
    /// Runs that found something, so far.
    ///
    /// Seeds rather than distinct bugs. Grouping needs the whole sweep, and a
    /// progress line that changed its mind about the count as it went would be
    /// worse than one that counts something simple.
    pub failing: u64,
    pub elapsed: Duration,
}

/// Everything one run started and has to stop again.
///
/// Held apart from [`Outcome`] because it is the teardown list rather than the
/// result: a run that failed still started a service, and the failure the user
/// reports should be the one they hit rather than "address already in use" on
/// the next run.
#[derive(Default)]
struct Running {
    services: Vec<Service>,
    /// Where the first system listens, for a terminal `check = "http"`.
    service_address: Option<std::net::SocketAddr>,
    /// Where the workload driver posts, when an ingress proxy was bound.
    ingress: Option<std::net::SocketAddr>,
    /// Every proxy serving this run: one per declared dependency, plus the
    /// ingress one when the workload posts.
    proxies: Vec<tokio::task::JoinHandle<Result<()>>>,
}

impl Running {
    /// Stops every service. Best effort, and never fails the run.
    async fn stop(self) {
        for service in self.services {
            service.stop().await;
        }
    }
}

/// Runs a scenario.
#[derive(Debug, Clone)]
pub struct Runner {
    scenario: Resolved,
    profile: Profile,
    /// Whether the service under test keeps this process's stdout and stderr.
    ///
    /// True for one run, because the service's own logs beside the reproducer
    /// are most of what makes a reordering failure legible. False for a sweep,
    /// where sixteen services sharing one terminal do not interleave neatly -
    /// they interleave *within a line*, and the result is unreadable rather
    /// than merely noisy.
    service_output: bool,
}

impl Runner {
    pub fn new(scenario: Resolved) -> Self {
        Self {
            scenario,
            profile: Profile::default(),
            service_output: true,
        }
    }

    /// Silences the service under test.
    ///
    /// Set by [`Runner::fuzz`] for its inner runs. A caller driving many runs
    /// itself wants the same thing.
    pub fn quiet(mut self) -> Self {
        self.service_output = false;
        self
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

        // Topology first, service second. The service must never observe a
        // half-built stream.
        environment
            .apply_topology(&self.scenario.deps, &events, started.elapsed())
            .await?;

        // From here on the environment and the service must be torn down
        // whatever happens, so the rest is one fallible block and the teardown
        // is unconditional. A run that failed and leaked a Postgres container
        // makes the next run fail too, and the second failure is the one the
        // user reports.
        let mut running = Running::default();

        let outcome = self
            .drive(
                &environment,
                &scheduler,
                &events,
                &cancel,
                started,
                &mut running,
            )
            .await;

        // The proxy first, so nothing is still writing events when they are
        // collected. The service stays up: the terminal checks below are about
        // to ask it what its state ended up as.
        cancel.cancel();

        for proxy in std::mem::take(&mut running.proxies) {
            let _ = proxy.await;
        }

        drop(events);

        let mut collected = Vec::new();
        while let Some(observed) = receiver.recv().await {
            checker.observe(&observed);
            collected.push(observed);
        }

        let context = CheckContext {
            service_url: running.service_address.map(|address| address.to_string()),
            postgres_url: self
                .scenario
                .deps
                .postgres
                .as_ref()
                .and_then(|postgres| environment.postgres_url(&postgres.database)),
            elapsed: started.elapsed(),
        };

        let finished = checker.finish(&context).await;

        running.stop().await;
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

    /// Starts the service, puts the proxies in front of it, drives the
    /// workload, waits for the system to settle.
    ///
    /// The order is load-bearing and is the same on every path through the
    /// tool. In particular the proxy binds *after* the service is listening and
    /// *before* any workload is driven: bound earlier it would forward to a
    /// port nothing has opened, and driven earlier the first requests would
    /// bypass the fault injection entirely and the run would quietly test less
    /// than it claimed.
    async fn drive(
        &self,
        environment: &Environment,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
        started: Instant,
        running: &mut Running,
    ) -> Result<()> {
        // Egress proxies first, because the service reads their addresses out
        // of its environment at startup. This is the "no SDK" stance in one
        // mechanism: the service reaches Redis through a different value in
        // `REDIS_URL` and is never told why.
        let readiness = crate::proxy::Readiness::new();

        let mut injected = self
            .start_egress(&mut running.proxies, scheduler, events, cancel, &readiness)
            .await?;

        // Which run this is, so a service sharing a dependency with other runs
        // can keep out of their way.
        //
        // Sixteen seeds in parallel against one Redis is sixteen services
        // writing the same keys, and the results are then about the collision
        // rather than about the ordering. A service that prefixes what it
        // touches with this is isolated again; one that ignores it is exactly as
        // isolated as it was. That is the right shape for a harness: offer the
        // one fact only the harness has, and let the service decide.
        injected.push((
            "MISORDER_SEED".to_string(),
            scheduler.trace().seed.to_string(),
        ));

        for system in &self.scenario.system {
            let mut service = Service::start(system, &injected, self.service_output).await?;

            events.emit_lifecycle(
                started.elapsed(),
                Event::Lifecycle(Lifecycle::SystemStarted {
                    command: system.run.clone(),
                }),
            );

            // Split by who can see the signal. `immediate` and `http_listening`
            // are answered from the process itself; the rest are detected from
            // traffic crossing a proxy, and only the proxies see that.
            match system.ready_when {
                Ready::Immediate | Ready::HttpListening => {
                    service
                        .await_ready(system.ready_when, self.scenario.run.ready_timeout)
                        .await?;
                }
                observed => {
                    readiness
                        .wait(observed, self.scenario.run.ready_timeout)
                        .await?;
                }
            }

            running.service_address = running.service_address.or_else(|| service.address());
            running.services.push(service);
        }

        events.emit_lifecycle(started.elapsed(), Event::Lifecycle(Lifecycle::SystemReady));

        // Ingress after the service is listening, because it forwards there.
        let ingress = self
            .start_ingress(running.service_address, scheduler, events, cancel)
            .await?;

        if let Some((address, task)) = ingress {
            running.proxies.push(task);
            running.ingress = Some(address);
        }

        Driver::new(environment, events)
            .with_ingress(running.ingress)
            .with_streams(
                self.scenario
                    .deps
                    .nats
                    .as_ref()
                    .is_some_and(|nats| !nats.streams.is_empty()),
            )
            .run(&self.scenario.workload, started.elapsed())
            .await?;

        self.await_quiescence(events, started).await
    }

    /// Binds one proxy per declared dependency, and reports the environment
    /// the service needs to reach them.
    ///
    /// Egress: the service is the one connecting, so the proxy stands where the
    /// dependency would be and the service is pointed at it through ordinary
    /// configuration. It imports nothing and is not told this is happening.
    ///
    /// Only dependencies with an `address` today. Starting a container is not
    /// implemented, and a dependency somebody else brought up is the case that
    /// actually needs no daemon here.
    async fn start_egress(
        &self,
        proxies: &mut Vec<tokio::task::JoinHandle<Result<()>>>,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
        readiness: &crate::proxy::Readiness,
    ) -> Result<Vec<(String, String)>> {
        let mut injected = Vec::new();

        for (protocol, upstream) in self.scenario.deps.external() {
            let endpoint = Self::bind_egress(
                protocol, upstream, proxies, scheduler, events, cancel, readiness,
            )
            .await?;

            tracing::debug!(
                protocol,
                upstream,
                listen = %endpoint.listen,
                "proxying a declared dependency"
            );

            injected.extend(endpoint.env);
        }

        Ok(injected)
    }

    /// Puts one adapter in front of one dependency.
    ///
    /// Its own function so the dispatch is the whole body: a build with no
    /// protocol features compiles this down to the error arm, with no binding
    /// left over to warn about.
    ///
    /// Gated on the set of protocols with an egress arm below, in the same way
    /// the imports at the top of this file are: with none of them compiled in,
    /// every parameter here is for an arm that does not exist. Add a protocol
    /// to the `any(..)` when you add its arm.
    #[cfg_attr(not(any(feature = "nats", feature = "redis")), allow(unused_variables))]
    async fn bind_egress(
        protocol: &str,
        upstream: &str,
        proxies: &mut Vec<tokio::task::JoinHandle<Result<()>>>,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
        readiness: &crate::proxy::Readiness,
    ) -> Result<crate::proxy::Endpoint> {
        match protocol {
            #[cfg(feature = "nats")]
            "nats" => {
                let mut adapter = crate::proxy::nats::NatsAdapter::new();
                let endpoint = adapter.bind(upstream).await?;

                let context = ProxyContext::new(
                    scheduler.clone(),
                    upstream.to_string(),
                    events.clone(),
                    cancel.clone(),
                )
                .with_readiness(readiness.clone());

                proxies.push(tokio::spawn(async move { adapter.serve(context).await }));

                Ok(endpoint)
            }
            #[cfg(feature = "redis")]
            "redis" => {
                let mut adapter = crate::proxy::redis::RedisAdapter::new();
                let endpoint = adapter.bind(upstream).await?;

                let context = ProxyContext::new(
                    scheduler.clone(),
                    upstream.to_string(),
                    events.clone(),
                    cancel.clone(),
                )
                .with_readiness(readiness.clone());

                proxies.push(tokio::spawn(async move { adapter.serve(context).await }));

                Ok(endpoint)
            }
            // Names the feature when the codec exists and this build left it
            // out, which is a different sentence from "not written yet" and
            // sends the reader somewhere different.
            other => Err(crate::proxy::unsupported(other)),
        }
    }

    /// Binds the ingress HTTP proxy, when the workload has anything to post.
    ///
    /// Ingress rather than egress, and the difference is which way the arrow
    /// points: a webhook is the vendor calling the service, so the proxy is
    /// what the *workload driver* posts to and the service is upstream of it.
    /// A scenario with no `post` step needs none of this, and binding one
    /// anyway would put a listening socket in every run that never used it.
    #[cfg(feature = "http")]
    async fn start_ingress(
        &self,
        service: Option<std::net::SocketAddr>,
        scheduler: &Scheduler,
        events: &EventSink,
        cancel: &CancellationToken,
    ) -> Result<Option<(std::net::SocketAddr, tokio::task::JoinHandle<Result<()>>)>> {
        let posts = self
            .scenario
            .workload
            .iter()
            .any(|step| matches!(step, Step::Post { .. }));

        if !posts {
            return Ok(None);
        }

        let Some(service) = service else {
            return Err(Error::Scenario(
                "a [[workload]] step posts, so misorder needs to put an ingress proxy in front \
                 of the service - give the [[system]] a `listen_env` naming the variable it \
                 reads its port from"
                    .to_string(),
            ));
        };

        let mut adapter = HttpAdapter::new();
        let endpoint = adapter.bind(&service.to_string()).await?;
        let listen = endpoint.listen;

        let context = ProxyContext::new(
            scheduler.clone(),
            service.to_string(),
            events.clone(),
            cancel.clone(),
        );

        let task = tokio::spawn(async move { adapter.serve(context).await });

        Ok(Some((listen, task)))
    }

    #[cfg(not(feature = "http"))]
    async fn start_ingress(
        &self,
        _service: Option<std::net::SocketAddr>,
        _scheduler: &Scheduler,
        _events: &EventSink,
        _cancel: &CancellationToken,
    ) -> Result<Option<(std::net::SocketAddr, tokio::task::JoinHandle<Result<()>>)>> {
        if self
            .scenario
            .workload
            .iter()
            .any(|step| matches!(step, Step::Post { .. }))
        {
            return Err(Error::Unsupported(
                "this scenario posts, but the build has no http feature".to_string(),
            ));
        }

        Ok(None)
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
    /// Runs locally and stateless, results to stdout. Fanning a sweep across
    /// machines is somebody else's job by design - `--shard i/N` lets a machine
    /// compute its own slice from two integers, so a shell script and a machine
    /// list are enough and this does not grow a job scheduler.
    pub async fn fuzz(&self, seeds: Seeds, parallel: usize, shard: Option<Shard>) -> FuzzReport {
        self.fuzz_with(seeds, parallel, shard, |_| {}).await
    }

    /// The same sweep, reporting each run as it finishes.
    ///
    /// A callback rather than a progress bar, for the same reason
    /// [`crate::report::Style`] is
    /// a palette rather than a decision: whether anything should be drawn
    /// depends on whether a terminal is attached, which is a fact about the
    /// process the CLI is running in and not about a sweep. A library that drew
    /// a bar itself would draw one into somebody's log file.
    ///
    /// Called once per completed run, from whichever task finished it, so an
    /// implementation has to be cheap and thread-safe. It is not called for
    /// runs still in flight: sixteen at a time means the count moves in steps.
    pub async fn fuzz_with<F>(
        &self,
        seeds: Seeds,
        parallel: usize,
        shard: Option<Shard>,
        on_progress: F,
    ) -> FuzzReport
    where
        F: Fn(Progress) + Send + Sync,
    {
        let started = Instant::now();
        let started_at = run::now_rfc3339();

        // Said once, loudly, because it undercuts the property the whole tool
        // rests on. A dependency somebody else started is not reset between
        // seeds, so what seed 41 finds can depend on what seed 40 left behind -
        // and "same seed, same run" stops being true across a sweep even though
        // it still holds for a single one.
        //
        // Not fixed by wiping it: that is somebody's Redis, and a test harness
        // that flushed it because a scenario pointed at it would be a worse
        // problem than the one it solved. Give a sweep an instance of its own,
        // or a key prefix per seed.
        let external = self.scenario.deps.external();

        if !external.is_empty() {
            tracing::warn!(
                dependencies = ?external.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                "this sweep runs against dependencies it did not start, so state carries between \
                 seeds and one seed's result can depend on another's"
            );
        }

        let mine: Vec<u64> = seeds
            .iter()
            .filter(|seed| shard.is_none_or(|shard| shard.contains(*seed)))
            .collect();

        // Quiet, because the services' own logs are worth reading for one run
        // and are a wall of interleaved fragments for four hundred.
        let runner = Arc::new(self.clone().quiet());

        let total = mine.len() as u64;
        let done = std::sync::atomic::AtomicU64::new(0);
        let failing = std::sync::atomic::AtomicU64::new(0);

        let outcomes: Vec<_> = futures::stream::iter(mine.clone())
            .map(|seed| {
                let runner = Arc::clone(&runner);
                let done = &done;
                let failing = &failing;
                let on_progress = &on_progress;

                async move {
                    let result = runner.execute(Run::Seed(seed)).await;

                    // Counted before the callback, so a progress line can never
                    // report more failures than completions.
                    if matches!(&result, Ok(outcome) if !outcome.passed()) {
                        failing.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    let completed = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                    on_progress(Progress {
                        completed,
                        total,
                        failing: failing.load(std::sync::atomic::Ordering::Relaxed),
                        elapsed: started.elapsed(),
                    });

                    (seed, result)
                }
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
            // Quiet, because shrinking re-runs the scenario dozens of times.
            // The logs worth reading are the ones from the run that failed,
            // which the caller has already seen; forty more copies of them
            // between that and the reproducer buries it.
            runner: self.clone().quiet(),
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

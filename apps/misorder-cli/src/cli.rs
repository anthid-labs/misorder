//! The `mis` command line.
//!
//! Five commands:
//!
//! - `check` parses a scenario, prints what it resolves to, and lists which
//!   invariants it will run.
//! - `run` executes one seed.
//! - `fuzz` executes many, in parallel.
//! - `replay` re-executes a recorded trace.
//! - `shrink` reduces a failing trace to its minimal reproducer.
//!
//! All five are the same machinery reached five ways. Nothing in this module
//! decides anything about a run: it parses arguments, calls
//! [`Runner`](misorder::runner::Runner), and prints the result. That is what
//! keeps `run` and `replay` from drifting apart, which they must not, because
//! the entire promise is that they do the same thing.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use misorder::corpus::{self, CorpusSource, EmptyCorpus, LocalCorpus};
use misorder::error::{Error, Result};
use misorder::invariant::builtin;
use misorder::report::junit;
use misorder::runner::{FuzzReport, Outcome, Run, Runner, Seeds, Shard};
use misorder::scenario::file::{Resolved, Scenario};
use misorder::shrink;
use misorder::trace::Trace;

/// Where a scenario is read from when no path is given.
const DEFAULT_SCENARIO_PATH: &str = "scenario.toml";

/// Whether the run found anything.
///
/// Not a `bool`, because the caller maps it to an exit code and
/// `Ok(false)` reads as failure at the call site while meaning "passed".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Passed,
    Failed,
}

/// How results are printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// For a person.
    Text,
    /// The versioned report document, for anything that stores, compares or
    /// comments on results. That document is the supported interface: it is
    /// versioned independently of this binary, so a consumer does not have to
    /// upgrade in step with the engine.
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "mis",
    version,
    about = "Runs your service against real dependencies under thousands of orderings"
)]
pub struct Cli {
    /// Log filter. `RUST_LOG` takes precedence when set.
    #[arg(long, global = true, env = "LOG_LEVEL")]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate a scenario and print what it resolves to.
    Check {
        #[arg(default_value = DEFAULT_SCENARIO_PATH)]
        scenario: PathBuf,

        /// Directory of recorded vendor behaviours, for a scenario with a
        /// `[vendors]` section.
        #[arg(long, env = "MISORDER_CORPUS")]
        corpus: Option<PathBuf>,
    },

    /// Run one seed.
    Run {
        #[arg(default_value = DEFAULT_SCENARIO_PATH)]
        scenario: PathBuf,

        /// The seed. One integer decides every timing and failure choice in the
        /// run, so this is the whole reproduction recipe for a fresh run.
        #[arg(long, default_value_t = 0)]
        seed: u64,

        /// Write the decision trace here. `-` writes it to stdout.
        #[arg(long)]
        trace: Option<PathBuf>,

        /// Shrink the trace when the run fails.
        ///
        /// Off by default because shrinking re-runs the scenario many times,
        /// and a single `mis run` should cost one run. `mis fuzz` turns it on
        /// for the failures it finds, which is where the cost is worth paying.
        #[arg(long)]
        shrink: bool,

        /// How to print the result.
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,

        #[arg(long, env = "MISORDER_CORPUS")]
        corpus: Option<PathBuf>,
    },

    /// Run many seeds.
    Fuzz {
        #[arg(default_value = DEFAULT_SCENARIO_PATH)]
        scenario: PathBuf,

        /// How many seeds to try.
        #[arg(long, default_value_t = 100)]
        seeds: u64,

        /// The first seed. Stated so a fuzzing pass is itself reproducible:
        /// "seeds 0 to 10000" has to mean the same set next week.
        #[arg(long, default_value_t = 0)]
        start: u64,

        /// How many runs at once.
        #[arg(long, default_value_t = 8)]
        parallel: usize,

        /// Stop after this many failing seeds.
        ///
        /// Ten failing seeds are usually two bugs, so the first few are worth
        /// far more than the rest of a long run.
        #[arg(long)]
        max_failures: Option<usize>,

        /// Write a JUnit XML report here.
        #[arg(long)]
        junit: Option<PathBuf>,

        /// Write the machine-readable sweep report here. `-` writes it to
        /// stdout.
        #[arg(long)]
        report: Option<PathBuf>,

        /// Run one slice of the seed space, written as `7/64`.
        ///
        /// Selection is `seed % count == index`, so a machine computes its own
        /// slice from two integers and coordinates with nobody. Split a sweep
        /// across as many machines as you like by giving each a different index
        /// and merging their reports.
        #[arg(long)]
        shard: Option<String>,

        #[arg(long, env = "MISORDER_CORPUS")]
        corpus: Option<PathBuf>,
    },

    /// Re-run a recorded trace.
    Replay {
        trace: PathBuf,

        #[arg(long, short, default_value = DEFAULT_SCENARIO_PATH)]
        scenario: PathBuf,
    },

    /// Reduce a failing trace to the decisions that caused it.
    Shrink {
        trace: PathBuf,

        #[arg(long, short, default_value = DEFAULT_SCENARIO_PATH)]
        scenario: PathBuf,

        /// Write the shrunk trace here. This is the file to commit.
        #[arg(long, short)]
        out: Option<PathBuf>,

        /// Ceiling on re-runs.
        #[arg(long, default_value_t = 2_000)]
        max_attempts: usize,
    },
}

impl Cli {
    pub async fn run(self) -> Result<Status> {
        match self.command {
            Command::Check { scenario, corpus } => check(&scenario, corpus.as_deref()).await,
            Command::Run {
                scenario,
                seed,
                trace,
                shrink,
                format,
                corpus,
            } => {
                run_one(
                    &scenario,
                    seed,
                    trace.as_deref(),
                    shrink,
                    format,
                    corpus.as_deref(),
                )
                .await
            }
            Command::Fuzz {
                scenario,
                seeds,
                start,
                parallel,
                max_failures,
                junit,
                report,
                shard,
                corpus,
            } => {
                fuzz(
                    &scenario,
                    Seeds::new(start, seeds),
                    parallel,
                    max_failures,
                    junit.as_deref(),
                    report.as_deref(),
                    shard.as_deref(),
                    corpus.as_deref(),
                )
                .await
            }
            Command::Replay { trace, scenario } => replay(&trace, &scenario).await,
            Command::Shrink {
                trace,
                scenario,
                out,
                max_attempts,
            } => shrink_trace(&trace, &scenario, out.as_deref(), max_attempts).await,
        }
    }

    /// Whether this invocation owns stdout as a data stream.
    ///
    /// Three ways that happens: a trace streamed to stdout, a sweep report
    /// streamed to stdout, and `--format json`. In all three a log line lands
    /// inside the document and makes it unparseable at some byte offset that
    /// says nothing about the cause.
    pub fn logs_to_stderr(&self) -> bool {
        match &self.command {
            Command::Run { trace, format, .. } => {
                *format == Format::Json || trace.as_deref() == Some(Path::new("-"))
            }
            Command::Fuzz { report, .. } => report.as_deref() == Some(Path::new("-")),
            _ => false,
        }
    }
}

fn load(path: &Path) -> Result<Resolved> {
    Scenario::load(path)?.resolve()
}

/// The corpus a run reads behaviours from.
///
/// A local directory, or nothing. A hosted corpus delivers files in the same
/// open format, so it is still `--corpus <directory>` here: the engine has no
/// network client, no credentials, and nothing to phone home to.
fn corpus_source(root: Option<&Path>) -> Box<dyn CorpusSource> {
    match root {
        Some(root) => Box::new(LocalCorpus::new(root)),
        None => Box::new(EmptyCorpus),
    }
}

/// Resolves the vendor behaviours a scenario named, and reports them.
async fn resolve_vendors(
    resolved: &Resolved,
    corpus: Option<&Path>,
) -> Result<Vec<misorder::corpus::BehaviorFlag>> {
    if resolved.vendors.is_empty() {
        return Ok(Vec::new());
    }

    corpus::resolve(corpus_source(corpus).as_ref(), &resolved.vendors).await
}

/// Reports what a scenario resolves to, and what it will actually check.
///
/// The second half matters more than it looks. A scenario permitting four
/// faults and naming one invariant reads as thorough, and this is where a user
/// finds out how much of that is real before spending an hour of compute on it.
async fn check(path: &Path, corpus: Option<&Path>) -> Result<Status> {
    let resolved = load(path)?;

    println!("{}: {}", path.display(), resolved.name);
    println!();
    println!("  systems     {}", resolved.system.len());

    for system in &resolved.system {
        println!("    {} (ready when {})", system.run, system.ready_when);
    }

    let declared = resolved.deps.declared();
    println!(
        "  deps        {}",
        if declared.is_empty() {
            "none".to_string()
        } else {
            declared.join(", ")
        }
    );

    println!("  workload    {} step(s)", resolved.workload.len());

    println!(
        "  faults      {}",
        if resolved.faults.is_empty() {
            "none permitted; this run cannot perturb anything".to_string()
        } else {
            resolved
                .faults
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        }
    );

    println!("  timeout     {:?}", resolved.run.timeout);

    if !resolved.vendors.is_empty() {
        let behaviors = resolve_vendors(&resolved, corpus).await?;

        println!("  vendors     {}", resolved.vendors.len());

        for flag in &behaviors {
            println!("    {} ({})", flag.name, flag.protocol);
            println!("      {}", flag.describe);
        }
    }

    println!();
    println!("  invariants");

    for spec in &resolved.invariants {
        match (&spec.builtin, &spec.name) {
            (Some(name), _) => {
                let entry = builtin::entry(name);
                let status = match entry.map(|entry| entry.status) {
                    Some(builtin::Status::Implemented) => "builtin",
                    Some(builtin::Status::Planned) => "builtin, NOT IMPLEMENTED YET",
                    None => "unknown",
                };

                println!("    {name} ({status})");

                if let Some(entry) = entry {
                    println!("      {}", entry.describe);
                }
            }
            (None, Some(name)) => println!("    {name} (user)"),
            (None, None) => println!("    (malformed)"),
        }
    }

    Ok(Status::Passed)
}

async fn run_one(
    path: &Path,
    seed: u64,
    trace_out: Option<&Path>,
    shrink_failures: bool,
    format: Format,
    corpus: Option<&Path>,
) -> Result<Status> {
    let resolved = load(path)?;
    resolve_vendors(&resolved, corpus).await?;

    let runner = Runner::new(resolved);
    let outcome = runner.execute(Run::Seed(seed)).await?;

    if let Some(destination) = trace_out {
        write_trace(&outcome.trace, destination)?;
    }

    if outcome.passed() {
        match format {
            Format::Json => print!("{}", outcome.report().to_json()),
            Format::Text => eprintln!(
                "seed {seed}: passed ({} decisions, {:?})",
                outcome.trace.active_count(),
                outcome.elapsed
            ),
        }

        return Ok(Status::Passed);
    }

    let shrunk = shrink_if_asked(&runner, &outcome, shrink_failures).await;
    let reported = shrunk.as_ref().map_or(&outcome, |(outcome, _)| outcome);

    match format {
        Format::Json => print!("{}", reported.report().to_json()),
        Format::Text => {
            let original = shrunk
                .as_ref()
                .map_or(outcome.trace.active_count(), |(_, before)| *before);

            if let Some(reproducer) = reported.reproducer(original) {
                println!("{}", reproducer.render());
            }
        }
    }

    if let (Some(destination), Some((shrunk, _))) = (trace_out, &shrunk) {
        write_trace(&shrunk.trace, destination)?;
    }

    Ok(Status::Failed)
}

#[allow(clippy::too_many_arguments)]
async fn fuzz(
    path: &Path,
    seeds: Seeds,
    parallel: usize,
    max_failures: Option<usize>,
    junit_out: Option<&Path>,
    report_out: Option<&Path>,
    shard: Option<&str>,
    corpus: Option<&Path>,
) -> Result<Status> {
    let shard = shard.map(Shard::parse).transpose()?;

    let resolved = load(path)?;
    resolve_vendors(&resolved, corpus).await?;

    let runner = Runner::new(resolved);
    let sweep = runner.fuzz(seeds, parallel, shard).await;

    // Shrunk before reporting, because an unshrunk trace signs its incidental
    // decisions along with the ones that mattered, and every seed would then
    // look like its own bug. Grouping is only worth anything after this.
    let shown = max_failures.unwrap_or(sweep.failures.len());
    let mut reports = Vec::new();

    for outcome in sweep.failures.iter().take(shown) {
        let shrunk = shrink_if_asked(&runner, outcome, true).await;
        let reported = shrunk.as_ref().map_or(outcome, |(outcome, _)| outcome);

        reports.push(reported.report());
    }

    // Anything past --max-failures is still counted, and reported unshrunk. A
    // sweep that silently dropped them would understate what it found.
    for outcome in sweep.failures.iter().skip(shown) {
        reports.push(outcome.report());
    }

    let document = sweep.to_report(reports);

    print_fuzz_summary(&sweep, &document);

    for report in document.failures.iter().take(shown) {
        if let Some(reproducer) = &report.reproducer {
            println!("{reproducer}");
        }
    }

    if let Some(destination) = junit_out {
        write_junit(&sweep, &document, destination)?;
    }

    if let Some(destination) = report_out {
        write_document(&document.to_json(), destination, "sweep report")?;
    }

    Ok(if sweep.passed() {
        Status::Passed
    } else {
        Status::Failed
    })
}

fn print_fuzz_summary(sweep: &FuzzReport, document: &misorder::report::SweepReport) {
    eprintln!(
        "{}: {} seed(s){} in {:?}",
        sweep.scenario,
        sweep.seeds_run,
        match sweep.shard {
            Some(shard) => format!(" (shard {shard})"),
            None => String::new(),
        },
        sweep.elapsed
    );

    if sweep.incomplete > 0 {
        eprintln!(
            "  {} run(s) could not complete; this sweep did not cover what it was asked to",
            sweep.incomplete
        );
    }

    if document.distinct_failures.is_empty() {
        eprintln!("  {} passed, none failing", sweep.passed);
        return;
    }

    // The line that matters. Ten failing seeds are usually two bugs, and a tool
    // that reports ten teaches people to ignore it.
    eprintln!(
        "  {} passed, {} failing across {} distinct failure(s)",
        sweep.passed,
        sweep.failures.len(),
        document.distinct_failures.len()
    );

    for group in &document.distinct_failures {
        let seeds: Vec<String> = group.seeds.iter().take(5).map(u64::to_string).collect();

        eprintln!(
            "    {} {} ({} seed(s), first: {})",
            group.signature,
            group.invariant,
            group.seeds.len(),
            seeds.join(" ")
        );
    }
}

async fn replay(trace_path: &Path, scenario_path: &Path) -> Result<Status> {
    let trace = Trace::load(trace_path)?;
    let runner = Runner::new(load(scenario_path)?);

    let outcome = runner.execute(Run::Replay(trace.clone())).await?;

    if outcome.passed() {
        eprintln!(
            "{}: replayed {} decision(s) and the failure did not reproduce",
            trace_path.display(),
            trace.active_count()
        );

        return Ok(Status::Passed);
    }

    if let Some(reproducer) = outcome.failure() {
        println!("{}", reproducer.render());
    }

    Ok(Status::Failed)
}

async fn shrink_trace(
    trace_path: &Path,
    scenario_path: &Path,
    out: Option<&Path>,
    max_attempts: usize,
) -> Result<Status> {
    let trace = Trace::load(trace_path)?;
    let runner = Runner::new(load(scenario_path)?);

    let outcome = runner.execute(Run::Replay(trace.clone())).await?;

    if outcome.passed() {
        return Err(Error::Trace(format!(
            "{} does not reproduce a failure against {}, so there is nothing to shrink",
            trace_path.display(),
            scenario_path.display()
        )));
    }

    let report = runner
        .shrink(&outcome, shrink::Limits { max_attempts })
        .await?;

    eprintln!(
        "shrank {} decisions to {} in {} re-run(s){}",
        report.before,
        report.after,
        report.attempts,
        if report.exhausted {
            "; budget exhausted, this may not be minimal"
        } else {
            ""
        }
    );

    if let Some(destination) = out {
        write_trace(&report.trace, destination)?;
    }

    Ok(Status::Failed)
}

/// Shrinks a failure and replays the result, returning the shrunk outcome and
/// how many decisions the original had.
///
/// `None` when shrinking was not asked for or did not work out. A shrinker that
/// broke must not lose the failure it was given: the unshrunk reproducer is
/// worth less, and worth far more than nothing.
async fn shrink_if_asked(
    runner: &Runner,
    outcome: &Outcome,
    shrink_first: bool,
) -> Option<(Outcome, usize)> {
    if !shrink_first {
        return None;
    }

    let report = match runner.shrink(outcome, shrink::Limits::default()).await {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(seed = outcome.seed, %error, "could not shrink");
            return None;
        }
    };

    // Replayed rather than trusted: the shrunk trace has to actually reproduce
    // the failure, and a shrunk trace that no longer fails is a bug in the
    // shrinker rather than a smaller reproducer.
    let replayed = runner
        .execute(Run::Replay(report.trace.clone()))
        .await
        .ok()
        .filter(|outcome| !outcome.passed())?;

    Some((replayed, report.before))
}

/// Writes a trace, or streams it to stdout for `-`.
fn write_trace(trace: &Trace, destination: &Path) -> Result<()> {
    if destination == Path::new("-") {
        let temporary = std::env::temp_dir().join(format!("misorder-{}.jsonl", trace.seed));

        trace.save(&temporary)?;
        let body = std::fs::read_to_string(&temporary)?;
        let _ = std::fs::remove_file(&temporary);

        print!("{body}");

        return Ok(());
    }

    trace.save(destination)?;
    eprintln!("trace written to {}", destination.display());

    Ok(())
}

/// Writes a document, or streams it to stdout for `-`.
fn write_document(body: &str, destination: &Path, what: &str) -> Result<()> {
    if destination == Path::new("-") {
        print!("{body}");

        return Ok(());
    }

    std::fs::write(destination, body)?;
    eprintln!("{what} written to {}", destination.display());

    Ok(())
}

fn write_junit(
    sweep: &FuzzReport,
    document: &misorder::report::SweepReport,
    destination: &Path,
) -> Result<()> {
    let cases: Vec<junit::Case> = document
        .failures
        .iter()
        .map(|report| junit::Case {
            name: format!("seed-{}", report.seed),
            elapsed: std::time::Duration::from_millis(report.elapsed_ms),
            message: report
                .violations
                .first()
                .map(|violation| format!("{}: {}", violation.invariant, violation.detail)),
            body: report.reproducer.clone(),
        })
        .collect();

    // A pass still produces a report, with one case standing for the whole
    // sweep. A CI job whose report file is missing on success has to special
    // case it, and the special case is where the "no tests ran" bug lives.
    let cases = if cases.is_empty() {
        vec![junit::Case {
            name: format!("seeds-{}", sweep.seeds_run),
            elapsed: sweep.elapsed,
            message: None,
            body: None,
        }]
    } else {
        cases
    };

    std::fs::write(destination, junit::render(&sweep.scenario, &cases))?;
    eprintln!("junit report written to {}", destination.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn a_scenario_path_defaults_so_the_common_case_is_bare() {
        let cli = Cli::try_parse_from(["mis", "check"]).expect("parse");

        assert!(matches!(
            cli.command,
            Command::Check { ref scenario, .. } if scenario == Path::new("scenario.toml")
        ));
    }

    #[test]
    fn writing_a_trace_to_stdout_moves_the_logs_to_stderr() {
        let cli = Cli::try_parse_from(["mis", "run", "--trace", "-"]).expect("parse");

        assert!(cli.logs_to_stderr());
    }

    #[test]
    fn writing_a_trace_to_a_file_leaves_the_logs_on_stdout() {
        let cli = Cli::try_parse_from(["mis", "run", "--trace", "t.jsonl"]).expect("parse");

        assert!(!cli.logs_to_stderr());
    }

    #[test]
    fn asking_for_json_moves_the_logs_to_stderr() {
        let cli = Cli::try_parse_from(["mis", "run", "--format", "json"]).expect("parse");

        assert!(cli.logs_to_stderr());
    }

    #[test]
    fn streaming_a_sweep_report_to_stdout_moves_the_logs_to_stderr() {
        let cli = Cli::try_parse_from(["mis", "fuzz", "--report", "-"]).expect("parse");

        assert!(cli.logs_to_stderr());
    }

    #[test]
    fn writing_a_sweep_report_to_a_file_leaves_the_logs_on_stdout() {
        let cli = Cli::try_parse_from(["mis", "fuzz", "--report", "s.json"]).expect("parse");

        assert!(!cli.logs_to_stderr());
    }

    #[test]
    fn a_shard_is_taken_as_written() {
        let cli = Cli::try_parse_from(["mis", "fuzz", "--shard", "7/64"]).expect("parse");

        assert!(matches!(
            cli.command,
            Command::Fuzz { shard: Some(ref shard), .. } if shard == "7/64"
        ));
    }

    #[test]
    fn a_fuzz_pass_states_its_first_seed_so_the_set_is_reproducible() {
        let cli =
            Cli::try_parse_from(["mis", "fuzz", "--seeds", "10", "--start", "500"]).expect("parse");

        assert!(matches!(
            cli.command,
            Command::Fuzz {
                seeds: 10,
                start: 500,
                ..
            }
        ));
    }
}

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

use misorder::error::{Error, Result};
use misorder::invariant::builtin;
use misorder::report::junit;
use misorder::runner::{FuzzReport, Outcome, Run, Runner};
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
            Command::Check { scenario } => check(&scenario),
            Command::Run {
                scenario,
                seed,
                trace,
                shrink,
            } => run_one(&scenario, seed, trace.as_deref(), shrink).await,
            Command::Fuzz {
                scenario,
                seeds,
                start,
                parallel,
                max_failures,
                junit,
            } => {
                fuzz(
                    &scenario,
                    start,
                    seeds,
                    parallel,
                    max_failures,
                    junit.as_deref(),
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
    pub fn logs_to_stderr(&self) -> bool {
        matches!(
            &self.command,
            Command::Run {
                trace: Some(path),
                ..
            } if path == Path::new("-")
        )
    }
}

fn load(path: &Path) -> Result<Resolved> {
    Scenario::load(path)?.resolve()
}

/// Reports what a scenario resolves to, and what it will actually check.
///
/// The second half matters more than it looks. A scenario permitting four
/// faults and naming one invariant reads as thorough, and this is where a user
/// finds out how much of that is real before spending an hour of compute on it.
fn check(path: &Path) -> Result<Status> {
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
) -> Result<Status> {
    let runner = Runner::new(load(path)?);
    let outcome = runner.execute(Run::Seed(seed)).await?;

    if let Some(destination) = trace_out {
        write_trace(&outcome.trace, destination)?;
    }

    if outcome.passed() {
        eprintln!(
            "seed {seed}: passed ({} decisions, {:?})",
            outcome.trace.active_count(),
            outcome.elapsed
        );

        return Ok(Status::Passed);
    }

    report_failure(&runner, &outcome, shrink_failures, trace_out).await?;

    Ok(Status::Failed)
}

async fn fuzz(
    path: &Path,
    start: u64,
    count: u64,
    parallel: usize,
    max_failures: Option<usize>,
    junit_out: Option<&Path>,
) -> Result<Status> {
    let runner = Runner::new(load(path)?);
    let seeds = start..start.saturating_add(count);

    let report = runner.fuzz(seeds, parallel).await;

    print_fuzz_summary(&report);

    let shown = max_failures.unwrap_or(report.failures.len());

    for outcome in report.failures.iter().take(shown) {
        report_failure(&runner, outcome, true, None).await?;
    }

    if let Some(destination) = junit_out {
        write_junit(&report, destination)?;
    }

    Ok(if report.passed() {
        Status::Passed
    } else {
        Status::Failed
    })
}

fn print_fuzz_summary(report: &FuzzReport) {
    eprintln!(
        "{}: {} seed(s) in {:?}, {} failing",
        report.scenario,
        report.seeds,
        report.elapsed,
        report.failures.len()
    );

    if !report.failures.is_empty() {
        let seeds: Vec<String> = report
            .failures
            .iter()
            .map(|outcome| outcome.seed.to_string())
            .collect();

        eprintln!("failing seeds: {}", seeds.join(" "));
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

/// Prints a failure, shrinking it first when asked.
async fn report_failure(
    runner: &Runner,
    outcome: &Outcome,
    shrink_first: bool,
    trace_out: Option<&Path>,
) -> Result<()> {
    let shrunk = if shrink_first {
        match runner.shrink(outcome, shrink::Limits::default()).await {
            Ok(report) => Some(report),
            // A shrinker that broke should not lose the failure it was given.
            // The unshrunk reproducer is worth less, and worth much more than
            // nothing.
            Err(error) => {
                tracing::warn!(seed = outcome.seed, %error, "could not shrink");
                None
            }
        }
    } else {
        None
    };

    match shrunk {
        Some(report) => {
            let replayed = runner
                .execute(Run::Replay(report.trace.clone()))
                .await
                .ok()
                .filter(|outcome| !outcome.passed());

            // Rendered against the pre-shrink count, so the report says
            // "6 of 847" and not "6 of 6".
            if let Some(reproducer) = replayed
                .as_ref()
                .and_then(|outcome| outcome.reproducer(report.before))
            {
                println!("{}", reproducer.render());
            }

            if let Some(destination) = trace_out {
                write_trace(&report.trace, destination)?;
            }
        }
        None => {
            if let Some(reproducer) = outcome.failure() {
                println!("{}", reproducer.render());
            }
        }
    }

    Ok(())
}

/// Writes a trace, or streams it to stdout for `-`.
fn write_trace(trace: &Trace, destination: &Path) -> Result<()> {
    if destination == Path::new("-") {
        let temporary = std::env::temp_dir().join(format!("misorder-{}.jsonl", trace.seed));

        trace.save(&temporary)?;
        print!("{}", std::fs::read_to_string(&temporary)?);
        let _ = std::fs::remove_file(&temporary);

        return Ok(());
    }

    trace.save(destination)?;
    eprintln!("trace written to {}", destination.display());

    Ok(())
}

fn write_junit(report: &FuzzReport, destination: &Path) -> Result<()> {
    let cases: Vec<junit::Case> = report
        .failures
        .iter()
        .map(|outcome| junit::Case {
            name: format!("seed-{}", outcome.seed),
            elapsed: outcome.elapsed,
            failure: outcome.failure(),
        })
        .collect();

    // A pass still produces a report, with one case standing for the whole
    // sweep. A CI job whose report file is missing on success has to special
    // case it, and the special case is where the "no tests ran" bug lives.
    let cases = if cases.is_empty() {
        vec![junit::Case {
            name: format!("seeds-{}", report.seeds),
            elapsed: report.elapsed,
            failure: None,
        }]
    } else {
        cases
    };

    std::fs::write(destination, junit::render(&report.scenario, &cases))?;
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
            Command::Check { ref scenario } if scenario == Path::new("scenario.toml")
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

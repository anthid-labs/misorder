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
use misorder::report::Style;
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

/// How a sweep report is written.
///
/// Separate from [`Format`] because they answer different questions. `Format`
/// is how a single run talks to the terminal; this is which of two documents a
/// sweep writes to a file, and one of them is not a document at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ReportFormat {
    /// The versioned sweep report. The supported interface, and what anything
    /// that stores or compares results should read.
    #[default]
    Json,
    /// One row per failing seed. Not versioned and not an interface to build
    /// on - it is for a person with a spreadsheet who wants to know which
    /// invariant is costing them the most seeds, without writing a parser to
    /// find out.
    Csv,
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

    /// Print without colour.
    ///
    /// Colour is on by default when a terminal is attached, and off when the
    /// output is redirected, so a report piped to a file does not arrive full
    /// of escape sequences. `NO_COLOR` turns it off too, and `CLICOLOR_FORCE`
    /// turns it on for a CI runner that renders colour without being a tty.
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// Whether to colour this process's output.
///
/// In precedence order, and the order is the point - each rule is more explicit
/// than the one below it:
///
/// 1. `--no-color`, because a flag someone typed beats everything.
/// 2. `NO_COLOR`, set to anything at all. The one convention with real
///    adoption, and it is off-by-presence rather than by value.
/// 3. `CLICOLOR_FORCE`, for a CI runner that renders colour without being a
///    tty. Above `TERM` deliberately: "force" that a terminfo entry could
///    override would not be forcing anything.
/// 4. `TERM=dumb`, taking at its word a terminal that says it cannot.
/// 5. Whether stderr is a tty.
///
/// Detection is on **stderr** rather than stdout, and that is not arbitrary.
/// Results go to stdout and are routinely piped somewhere; the summaries and
/// reproducers a person reads go to stderr. Deciding on stdout would print
/// plain text to the terminal every time someone ran
/// `mis fuzz ... > report.json`, which is the common case rather than the edge
/// one.
pub fn style(no_color: bool) -> Style {
    use std::io::IsTerminal;

    // Present *and* non-empty, which is what the NO_COLOR convention actually
    // specifies. `NO_COLOR=` from a cleared shell variable means the variable
    // is not set, and treating it as set would make `FOO= cmd` a way to
    // silently lose colour that nobody could find.
    let set = |name: &str| std::env::var_os(name).is_some_and(|value| !value.is_empty());

    if no_color || set("NO_COLOR") {
        return Style::plain();
    }

    if set("CLICOLOR_FORCE") {
        return Style::colour();
    }

    // A terminal that says it cannot do anything is taken at its word.
    if std::env::var("TERM").is_ok_and(|term| term == "dumb") {
        return Style::plain();
    }

    if std::io::stderr().is_terminal() {
        Style::colour()
    } else {
        Style::plain()
    }
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

        /// Write the machine-readable sweep report here. `-` writes it to
        /// stdout.
        #[arg(long)]
        report: Option<PathBuf>,

        /// Which document `--report` writes.
        #[arg(long, value_enum, default_value_t = ReportFormat::Json)]
        report_format: ReportFormat,

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
        let style = style(self.no_color);

        match self.command {
            Command::Check { scenario, corpus } => {
                check(&scenario, corpus.as_deref(), &style).await
            }
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
                    &style,
                )
                .await
            }
            Command::Fuzz {
                scenario,
                seeds,
                start,
                parallel,
                max_failures,
                report,
                report_format,
                shard,
                corpus,
            } => {
                fuzz(
                    &scenario,
                    Seeds::new(start, seeds),
                    parallel,
                    max_failures,
                    report.as_deref(),
                    report_format,
                    shard.as_deref(),
                    corpus.as_deref(),
                    &style,
                )
                .await
            }
            Command::Replay { trace, scenario } => replay(&trace, &scenario, &style).await,
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
/// A local directory, or nothing. Anything that distributes corpus entries
/// delivers files in this same format, so it is still `--corpus <directory>`
/// here: the engine has no network client, no credentials, and nothing to
/// phone home to.
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
async fn check(path: &Path, corpus: Option<&Path>, style: &Style) -> Result<Status> {
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
            declared
                .iter()
                .map(|name| {
                    // A dependency whose adapter was compiled out cannot be
                    // proxied, and the run fails at the point it tries. Saying
                    // so here is the whole job of `check`.
                    if builtin::compiled_in(name) {
                        name.to_string()
                    } else {
                        format!(
                            "{name} ({})",
                            style.paint(
                                style.warn,
                                format!("no adapter in this build: needs the `{name}` feature"),
                            )
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
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
                // The planned and unknown cases are the whole reason `check`
                // exists: a scenario permitting four faults and naming one
                // invariant reads as thorough, and this is where you find out
                // how much of that is real before spending an hour of compute.
                let status = match entry {
                    Some(entry) if entry.available() => style.paint(style.good, "builtin"),
                    Some(entry) if entry.status == builtin::Status::Planned => {
                        style.paint(style.warn, "builtin, NOT IMPLEMENTED YET")
                    }
                    // Written, and not in this binary. A different problem from
                    // the one above and it has a different fix, so it gets its
                    // own line rather than being folded into "not implemented".
                    Some(entry) => style.paint(
                        style.warn,
                        format!(
                            "builtin, NOT IN THIS BUILD: needs the `{}` feature",
                            entry.dependency
                        ),
                    ),
                    None => style.paint(style.bad, "unknown"),
                };

                println!("    {name} ({status})");

                if let Some(entry) = entry {
                    println!("      {}", entry.describe);
                }
            }
            (None, Some(name)) => {
                println!("    {name} ({})", style.paint(style.good, "user"))
            }
            (None, None) => println!("    {}", style.paint(style.bad, "(malformed)")),
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
    style: &Style,
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
                "seed {seed}: {} ({} decisions, {:?})",
                style.paint(style.good, "passed"),
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
                println!("{}", reproducer.render_with(style));
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
    report_out: Option<&Path>,
    report_format: ReportFormat,
    shard: Option<&str>,
    corpus: Option<&Path>,
    style: &Style,
) -> Result<Status> {
    let shard = shard.map(Shard::parse).transpose()?;

    let resolved = load(path)?;
    resolve_vendors(&resolved, corpus).await?;

    let runner = Runner::new(resolved);
    // Drawn only when stderr is a terminal, so a redirected sweep writes
    // nothing extra and a person watching one knows it is alive.
    let bar = crate::progress::Bar::new(*style);

    let sweep = runner
        .fuzz_with(seeds, parallel, shard, |progress| bar.update(progress))
        .await;

    // Shrunk before reporting, because an unshrunk trace signs its incidental
    // decisions along with the ones that mattered, and every seed would then
    // look like its own bug. Grouping is only worth anything after this.
    let shown = max_failures.unwrap_or(sweep.failures.len());
    let mut reports = Vec::new();

    // Rendered here rather than read back out of the document. The
    // document's `reproducer` field is plain by construction - it is a
    // versioned interface other tools parse, and escape codes in it would be
    // somebody else's bug - so the coloured copy has to come from the outcome
    // while it is still in hand.
    let mut rendered = Vec::new();

    let shrinking = std::time::Instant::now();

    for (index, outcome) in sweep.failures.iter().take(shown).enumerate() {
        bar.phase("shrinking", index as u64, shown as u64, shrinking.elapsed());

        let shrunk = shrink_if_asked(&runner, outcome, true).await;
        let reported = shrunk.as_ref().map_or(outcome, |(outcome, _)| outcome);

        let original = shrunk
            .as_ref()
            .map_or(outcome.trace.active_count(), |(_, before)| *before);

        if let Some(reproducer) = reported.reproducer(original) {
            rendered.push(reproducer.render_with(style));
        }

        reports.push(reported.report());
    }

    bar.phase("shrinking", shown as u64, shown as u64, shrinking.elapsed());
    bar.finish();

    // Anything past --max-failures is still counted, and reported unshrunk. A
    // sweep that silently dropped them would understate what it found.
    for outcome in sweep.failures.iter().skip(shown) {
        reports.push(outcome.report());
    }

    let document = sweep.to_report(reports);

    print_fuzz_summary(&sweep, &document, style);

    for reproducer in &rendered {
        println!("{reproducer}");
    }

    if let Some(destination) = report_out {
        let (body, what) = match report_format {
            ReportFormat::Json => (document.to_json(), "sweep report"),
            ReportFormat::Csv => (document.to_csv(), "sweep csv"),
        };

        write_document(&body, destination, what)?;
    }

    Ok(if sweep.passed() {
        Status::Passed
    } else {
        Status::Failed
    })
}

fn print_fuzz_summary(sweep: &FuzzReport, document: &misorder::report::SweepReport, style: &Style) {
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

    // Yellow, not red. A run that could not complete is not a finding, and
    // colouring it like one is the same mistake as folding it into the failure
    // count: it teaches people that red means "look into it eventually".
    if sweep.incomplete > 0 {
        eprintln!(
            "  {}",
            style.paint(
                style.warn,
                format!(
                    "{} run(s) could not complete; this sweep did not cover what it was asked to",
                    sweep.incomplete
                )
            )
        );
    }

    if document.distinct_failures.is_empty() {
        eprintln!(
            "  {}, none failing",
            style.paint(style.good, format!("{} passed", sweep.passed))
        );
        return;
    }

    // The line that matters. Ten failing seeds are usually two bugs, and a tool
    // that reports ten teaches people to ignore it.
    eprintln!(
        "  {}, {}",
        style.paint(style.good, format!("{} passed", sweep.passed)),
        style.paint(
            style.bad,
            format!(
                "{} failing across {} distinct failure(s)",
                sweep.failures.len(),
                document.distinct_failures.len()
            )
        )
    );

    for group in &document.distinct_failures {
        let seeds: Vec<String> = group.seeds.iter().take(5).map(u64::to_string).collect();

        eprintln!(
            "    {} {} {}",
            style.paint(style.dim, &group.signature),
            style.paint(style.bad, &group.invariant),
            style.paint(
                style.dim,
                format!(
                    "({} seed(s), first: {})",
                    group.seeds.len(),
                    seeds.join(" ")
                )
            )
        );
    }
}

async fn replay(trace_path: &Path, scenario_path: &Path, style: &Style) -> Result<Status> {
    let trace = Trace::load(trace_path)?;
    let runner = Runner::new(load(scenario_path)?);

    let outcome = runner.execute(Run::Replay(trace.clone())).await?;

    if outcome.passed() {
        eprintln!(
            "{}: replayed {} decision(s) and the failure {}",
            trace_path.display(),
            trace.active_count(),
            style.paint(style.warn, "did not reproduce")
        );

        return Ok(Status::Passed);
    }

    if let Some(reproducer) = outcome.failure() {
        println!("{}", reproducer.render_with(style));
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
    //
    // Quiet, like the shrink attempts before it. The service already logged
    // the run that failed, and a second unlabelled copy of its output between
    // that and the reproducer reads as the same run printed twice. What the
    // minimal ordering was is in the reproducer's `delivery order` section,
    // which says it better than a log dump does.
    let replayed = runner
        .clone()
        .quiet()
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

#[cfg(test)]
mod tests {
    /// Every binary an example runs is one this workspace actually builds.
    ///
    /// A `[[bin]]` rename is invisible to the examples: they name a path under
    /// `target/debug`, and the old binary sits there from the previous build
    /// until someone clones the repository fresh. `redis_naive_lock` and
    /// `stripe_invoice_lifecycle` both shipped pointing at names that no longer
    /// existed, and both ran fine on the machine that renamed them.
    ///
    /// The scenarios whose services are not written yet are listed rather than
    /// skipped by heuristic, so an aspirational path stays distinguishable from
    /// a typo.
    #[test]
    fn every_example_runs_a_binary_this_workspace_builds() {
        /// Named in an example, not built by anything here yet. Each of these
        /// scenarios documents the orchestration it is waiting on.
        const NOT_BUILT_YET: &[&str] = &["ledger", "billing", "oms"];

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("the workspace root is two levels above this crate")
            .to_path_buf();

        // Every `[[bin]]` name any member declares.
        let mut built = Vec::new();
        for entry in std::fs::read_dir(root.join("apps")).expect("apps/ is readable") {
            let manifest = entry.expect("a readable entry").path().join("Cargo.toml");

            let Ok(text) = std::fs::read_to_string(&manifest) else {
                continue;
            };

            for line in text.lines() {
                if let Some(rest) = line.trim().strip_prefix("name = ") {
                    built.push(rest.trim().trim_matches('"').to_string());
                }
            }
        }

        assert!(
            built.contains(&"redis_demo".to_string()),
            "found: {built:?}"
        );

        let mut missing = Vec::new();

        for entry in std::fs::read_dir(root.join("examples")).expect("examples/ is readable") {
            let path = entry.expect("a readable entry").path();

            if path.extension().is_none_or(|ext| ext != "toml") {
                continue;
            }

            let text = std::fs::read_to_string(&path).expect("a readable scenario");

            for line in text.lines() {
                let line = line.trim();

                let Some(rest) = line.strip_prefix("run = ") else {
                    continue;
                };

                let command = rest.trim().trim_matches('"');

                let Some(name) = command.strip_prefix("./target/debug/") else {
                    continue;
                };

                if built.contains(&name.to_string()) || NOT_BUILT_YET.contains(&name) {
                    continue;
                }

                missing.push(format!(
                    "{}: runs `{name}`, which no [[bin]] in this workspace declares",
                    path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
        }

        assert!(missing.is_empty(), "{}", missing.join("\n"));
    }

    /// Every protocol the engine has is one this binary can forward.
    ///
    /// The workspace dependency on `misorder` sets `default-features = false`,
    /// so this crate's `[features]` list is the whole of what `mis` can speak.
    /// A protocol added to the library and not to that list compiles, tests
    /// green, and is unreachable from the command anybody runs: `mis check`
    /// resolves the scenario, `mis run` refuses it, and the two disagree.
    /// `redis` shipped that way.
    ///
    /// Reads both manifests as text rather than asking cargo, so the test is
    /// hermetic and runs in the same second as everything else here.
    #[test]
    fn the_cli_forwards_every_engine_feature() {
        fn features(manifest: &str) -> Vec<String> {
            let mut names = Vec::new();
            let mut inside = false;

            for line in manifest.lines() {
                let line = line.trim();

                if line.starts_with('[') {
                    inside = line == "[features]";
                    continue;
                }

                if !inside || line.is_empty() || line.starts_with('#') {
                    continue;
                }

                if let Some((name, _)) = line.split_once('=') {
                    let name = name.trim();

                    // `default` is a set of the others, not a protocol.
                    if name != "default" {
                        names.push(name.to_string());
                    }
                }
            }

            names.sort();
            names
        }

        let engine = features(include_str!("../../../crates/misorder/Cargo.toml"));
        let cli = features(include_str!("../Cargo.toml"));

        assert!(!engine.is_empty(), "the engine manifest parsed to nothing");
        assert_eq!(
            engine, cli,
            "misorder-cli must forward every misorder feature, or `mis` cannot speak \
             the protocol at all"
        );
    }

    /// The workspace dependency on `misorder` names the version that will be
    /// published, not the one being built.
    ///
    /// Cargo cannot inherit `version` into `[workspace.dependencies]`, so it is
    /// written out a second time, and a stale value is not a build error.
    /// Locally the `path` wins and everything passes. It is `cargo publish`
    /// that rewrites the dependency to the version in the manifest, so a bump
    /// that misses this line ships a `misorder-cli` requiring `misorder
    /// ^<old>`, which resolves against the previous library on crates.io rather
    /// than the one it was built and tested with. Nothing fails; the wrong code
    /// is simply what users get.
    ///
    /// Only one direction needs guarding. A dependency version *ahead* of the
    /// package version fails the build outright, because no path crate
    /// satisfies the requirement. It is the version left *behind* that cargo
    /// accepts, since the newer path crate still satisfies the older caret
    /// requirement, and that is the one that reaches crates.io.
    ///
    /// Caught here rather than in CI because CI finds it after the publish,
    /// and a version number on crates.io is burned even if the crate is yanked
    /// a minute later.
    #[test]
    fn the_workspace_dependency_version_matches_the_package_version() {
        /// The value of `key = "..."` in `[<section>]`, as text.
        fn quoted(manifest: &str, section: &str, key: &str) -> Option<String> {
            let mut inside = false;

            for line in manifest.lines() {
                let line = line.trim();

                if line.starts_with('[') {
                    inside = line == section;
                    continue;
                }

                if !inside || !line.starts_with(key) {
                    continue;
                }

                // The dependency entry is an inline table, so take the first
                // quoted run after the key rather than the rest of the line.
                let (name, rest) = line.split_once('=')?;

                if name.trim() != key {
                    continue;
                }

                let after = rest.find('"')? + 1;
                let end = rest[after..].find('"')? + after;

                return Some(rest[after..end].to_string());
            }

            None
        }

        let root = include_str!("../../../Cargo.toml");

        let package = quoted(root, "[workspace.package]", "version")
            .expect("[workspace.package] has no version");

        // Read out of the dependency line's inline table.
        let dependency = root
            .lines()
            .find(|line| line.trim_start().starts_with("misorder = {"))
            .and_then(|line| {
                let start = line.find("version = \"")? + "version = \"".len();
                let end = line[start..].find('"')? + start;
                Some(line[start..end].to_string())
            })
            .expect("[workspace.dependencies] has no misorder version");

        assert_eq!(
            package, dependency,
            "[workspace.package] version is {package} but [workspace.dependencies] \
             publishes misorder-cli against misorder {dependency}; bump both or the \
             published CLI resolves against the wrong library"
        );
    }

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

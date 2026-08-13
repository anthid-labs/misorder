//! The `mis` command.
//!
//! Thin by design: everything that decides or runs anything lives in the
//! [`misorder`] library, and this crate is the argument parsing, the logging
//! setup and the exit code around it.

mod cli;
mod telemetry;

use clap::Parser;
use misorder::error::{Error, Result};

use crate::cli::{Cli, Status};
use crate::telemetry::{LogSink, setup_telemetry_client_to};

/// Exit codes, and the distinction between them is the point.
///
/// A CI job needs to tell "the service under test has a bug" from "the harness
/// could not run". Collapsing both into 1 means a broken Docker socket looks
/// like a caught bug, someone chases it for an hour, and the next real finding
/// gets the same treatment.
const EXIT_HARNESS_ERROR: i32 = 1;
const EXIT_INVARIANT_VIOLATED: i32 = 2;

/// Returning `Result` from `main` would print the error's `Debug` form,
/// `Scenario("no [[system]] block")`, because that is what `Termination` uses.
/// Handling it here prints `Display` instead, which is what the messages were
/// written for, and keeps the exit code explicit.
#[tokio::main]
async fn main() {
    match run().await {
        Ok(Status::Passed) => {}
        Ok(Status::Failed) => std::process::exit(EXIT_INVARIANT_VIOLATED),
        Err(error) => {
            eprintln!("mis: {error}");
            std::process::exit(EXIT_HARNESS_ERROR);
        }
    }
}

async fn run() -> Result<Status> {
    let cli = Cli::parse();

    // A command writing a trace to stdout owns it as a data stream, so its
    // diagnostics go to stderr. A log line in the middle of a JSON Lines
    // document makes the file unparseable at some byte offset, and what the
    // reader reports says nothing about the cause.
    let sink = if cli.logs_to_stderr() {
        LogSink::Stderr
    } else {
        LogSink::Stdout
    };

    // Set up before any work so failures are reported through the same path as
    // everything else.
    let telemetry =
        setup_telemetry_client_to(env!("CARGO_PKG_NAME"), cli.log_level.as_deref(), sink)
            .map_err(Error::Scenario)?;

    let outcome = cli.run().await;

    telemetry.shutdown().await;

    outcome
}

//! Deterministic simulation testing for services that talk to real
//! dependencies.
//!
//! Your integration tests run one ordering. Production runs a different one at
//! 3am. misorder runs a service against its real dependencies, takes every
//! timing and failure decision from a seeded PRNG, and explores thousands of
//! orderings of the same scenario. When one breaks the service, it shrinks the
//! failure to the handful of events that caused it.
//!
//! For the command line tool, install `misorder-cli`. This crate is the engine
//! underneath it, for embedding the same machinery in another program.
//!
//! # The shape of a run
//!
//! Six stages, and the boundaries between them are where the design lives:
//!
//! 1. [`scenario`] parses the one file a user writes: what to run, what it
//!    depends on, what to drive at it, which faults are permitted, and what
//!    must always be true.
//! 2. [`orchestrator`] starts the declared dependencies as real containers and
//!    applies their topology.
//! 3. [`proxy`] sits between the service and every one of those dependencies,
//!    speaking the real wire protocol in both directions.
//! 4. [`schedule`] answers every question the proxy asks: deliver now or in
//!    40ms, drop this connection, swallow this ack, reorder these two replies.
//! 5. [`trace`] records each of those answers, so a run is an artifact rather
//!    than an anecdote.
//! 6. [`invariant`] watches the resulting event stream and says what broke.
//!
//! [`runner`] is what holds those together, and is the type most callers want.
//! [`shrink`] and [`report`] are what happen after one of them fails.
//!
//! # Two things follow from that shape
//!
//! **The scheduler is the only source of nondeterminism.** Not "the main
//! source". If a proxy adapter reads the clock, calls `rand`, or races two
//! tasks whose order it does not control, the trace stops being a complete
//! description of the run and replay quietly becomes a lie. Every branch that
//! could go two ways goes through [`schedule::Scheduler::decide`], and that is
//! a hard rule for anything added under [`proxy`].
//!
//! **You shrink the trace, not the seed.** Seeds 8837291 and 8837292 produce
//! unrelated schedules, so there is no gradient to descend and nothing a
//! smaller seed would mean. What shrinks is the list of decisions: replace one
//! with the neutral choice, replay, and keep the replacement if the run still
//! fails. See [`shrink`].
//!
//! # Getting started
//!
//! ```no_run
//! use misorder::runner::{Run, Runner};
//! use misorder::scenario::file::Scenario;
//!
//! # async fn example() -> misorder::error::Result<()> {
//! let scenario = Scenario::load("scenario.toml")?;
//!
//! let outcome = Runner::new(scenario.resolve()?)
//!     .execute(Run::Seed(8_837_291))
//!     .await?;
//!
//! if let Some(failure) = outcome.failure() {
//!     println!("{}", failure.render());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Logging
//!
//! Events are emitted through [`tracing`]. No subscriber is installed here;
//! that belongs to whatever binary is at the top of the stack.
//!
//! # Language stance
//!
//! The interface is the wire protocol and a TOML file. A service under test
//! imports nothing, links nothing, and sets no build flags: it connects to the
//! address misorder gives it and behaves normally. A Go service and a Rust
//! service adopt this identically, and the cost of supporting both is zero,
//! because cost scales with protocols and not with languages.

pub mod error;
pub mod event;
pub mod invariant;
pub mod orchestrator;
pub mod proxy;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod schedule;
pub mod shrink;
pub mod trace;
pub mod workload;

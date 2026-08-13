# misorder

Deterministic simulation testing for services with real dependencies.

This is the engine. For the command line tool, install
[`misorder-cli`](https://crates.io/crates/misorder-cli), which installs a
command called `mis`.

```toml
[dependencies]
misorder = "0.1"
```

```rust,no_run
use misorder::runner::{Run, Runner};
use misorder::scenario::file::Scenario;

# async fn example() -> misorder::error::Result<()> {
let scenario = Scenario::load("scenario.toml")?;

let outcome = Runner::new(scenario.resolve()?)
    .execute(Run::Seed(8_837_291))
    .await?;

if let Some(failure) = outcome.failure() {
    println!("{}", failure.render());
}
# Ok(())
# }
```

Every timing and failure decision comes from a PRNG seeded with one integer, and
every decision is recorded, so a failure is a replayable artifact rather than an
anecdote. When one breaks you, the shrinker reduces it to the handful of events
that caused it.

The crate emits `tracing` events and installs no subscriber; that belongs to
whatever binary is at the top of the stack.

Features, one per protocol and all on by default: `nats`, `postgres`, `http`.

See the [repository](https://github.com/misorder/misorder) for the scenario
format, the built-in invariant catalogue, and the roadmap.

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

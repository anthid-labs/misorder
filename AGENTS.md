# misorder Agent Guide

## Scope

This is the misorder Rust workspace: a test harness that runs a service against
its real dependencies, takes every timing and failure decision from a seeded
PRNG, and shrinks a failure to the handful of events that caused it. It is
early, and most of the tree is a skeleton with working seams. Make the smallest
cohesive change that fits the layout below, and do not scaffold directories or
crates that the current task does not need.

## Architecture at a glance

- `apps/*` contains independently built binaries. A package can expose multiple
  binaries; use its `[[bin]]` declarations rather than assuming the package name
  is the executable name. `misorder-cli` builds a command called `mis`.
- `crates/*` contains shared, domain-neutral Rust code. Prefer an existing
  library over duplicating logic in an app.
- `crates/misorder` is the engine and `apps/misorder-cli` is the command around
  it. Both are published to crates.io, so the split is a published API boundary,
  not just a directory: anything that decides, runs, or checks belongs in the
  library, and argument parsing, the global log subscriber and the exit code
  belong in the binary. A library that installed a subscriber would fight
  whatever its host application had already set up.
- Workspace members are globbed. A new crate under `apps/` or `crates/` joins
  the workspace without a root `Cargo.toml` edit.

The stages, in the order a run goes through them: `scenario` parses the file,
`orchestrator` starts real containers, `proxy` sits between the service and each
dependency, `schedule` answers every fork, `trace` records the answers,
`invariant` says what broke, `shrink` reduces it, `report` renders it. `runner`
holds them together.

## Determinism is the product

Everything else is negotiable. These are not.

- **The scheduler is the only source of nondeterminism.** Every branch that
  could go two ways goes through `ProxyContext::decide`. No `Instant::now`, no
  `rand`, no `tokio::select!` over futures whose completion order the code does
  not control, no `HashMap` iteration order affecting a decision. An adapter
  that breaks this does not fail loudly: it produces traces that replay into a
  different run, and the tool's one promise stops being true.
- **Decisions are a pure function of `(seed, fork)`.** Not a sequential PRNG
  stream. A shared stream makes the schedule depend on which task reaches it
  first, which the OS decides. `DecisionSource::decide` takes `&self` for this
  reason; keep it that way.
- **A fork is identified by `(kind, connection, ordinal)`,** never by its
  position in the trace. Shrinking removes decisions, which changes what later
  forks exist. Position-keyed lookup would misapply every subsequent decision
  and report a reproduction that reproduced something else.
- **Every decision has a neutral counterpart.** That is what "remove this
  decision" means, and what makes shrinking possible at all. A new `Decision`
  variant with no sensible neutral form is a design error, not a special case.
- **You shrink the trace, never the seed.** Adjacent seeds produce unrelated
  schedules, so there is no gradient and no meaning to a smaller seed.

## A harness failure is not a finding

`Error` is misorder failing. `Violation` is the service under test failing.
Never let one become the other:

- Docker unreachable, a container that would not start, an adapter that cannot
  decode a frame: `Err`, exit code 1.
- An invariant firing: a `Violation`, exit code 2.

One invented failure costs more trust than several missed real ones. When a
heuristic has to guess, such as quiescence detection, guess in the direction of
missing a bug rather than manufacturing one, and say so in a comment.

## Compatibility surfaces

Two formats outlive any build, because both are committed to users' repositories
and run in their CI months later.

- **The scenario TOML.** Treat it as something a generator emits, not only
  something a human types: Phase 2 produces these programmatically, and a format
  migration then would invalidate every committed reproducer. Unknown keys stay
  refused. New keys are optional with defaults that preserve existing behaviour.
- **The trace JSON Lines format.** Bump `trace::FORMAT_VERSION` whenever an
  older misorder would misread a newer trace. Reading a format from the future
  is refused rather than guessed at.

## Dependencies

misorder drives Docker and speaks wire protocols. It has no message broker of
its own, no datastore, and no service mesh. Client libraries (`async-nats`,
`tokio-postgres`) are for *driving* a workload and *checking* final state, never
for proxying: a client library hides exactly the frame-level decisions this tool
exists to control. Proxy adapters speak the protocol themselves.

`bollard` is an HTTP client for the Docker daemon's own API. It does not appear
in the public API, so replacing it, or pointing it at Podman, stays a change in
`orchestrator::docker` and nowhere else.

**Never use `dashmap`.** Holding a reference into one shard while touching
another deadlocks, and the API does nothing to stop you. Prefer state owned by a
single task and passed by message; where sharing is unavoidable, use
`std::sync::Mutex`/`RwLock` around a plain `HashMap` so the lock scope is
visible at the call site.

**No SDK for the service under test, ever.** The interface is the wire protocol
and a TOML file. A Go service and a Rust service adopt misorder identically, and
cost scales with protocols rather than languages. Anything that would require an
import, a build flag, or a linked library in the service under test needs an
explicit decision from the user first. The optional idle-signalling SDK
contemplated for Phase 3 stays optional forever; the moment it is required,
this is N language products again.

## Rust conventions

- The workspace uses Rust 2024. Shared dependency versions belong in
  `[workspace.dependencies]`; consume them with `<name> = { workspace = true }`.
  Add a crate-local version only when a package genuinely needs to diverge.
- One feature per protocol, all on by default. Keep code that uses an optional
  client correctly gated. **Never paywall an adapter**: the long tail of vendors
  is only ever covered by people who needed one, and a licence boundary there
  ends the contributions.
- Prefer `tracing` with useful context over `println!`. The CLI prints results;
  the library logs. Never log payload contents: a scenario's traffic is the
  user's production shape.
- Preserve cancellation and error propagation in async code. Do not detach tasks
  without a clear lifecycle, shutdown path, and error reporting.
- Unimplemented work returns `Error::Unsupported` with a plain message, not
  `todo!()`. A skeleton that panics is indistinguishable from a bug.

## Proxy and adapter work

- Treat every frame as hostile input. It arrives from a service under test that
  may be mid-bug, and from vendors whose framing does not match their own
  documentation. Handle malformed frames rather than assuming them away.
- Adding an adapter is the intended contribution path. The surface is
  deliberately small: bind, accept, speak the protocol, ask before branching.
- The sorting rule for whether something needs a simulator rather than a proxy:
  **does anything I care about happen without a client asking?** JetStream's
  `ack_wait` fires on the server's own timer with no frame crossing the wire, so
  a proxy cannot intercept it. Postgres, Redis and ClickHouse are reactive and
  stay proxied indefinitely.
- Built-in invariants learn the topology from events, not from the scenario
  file. Reading the scenario would check the configuration that was asked for
  rather than the one the server actually has.

## Open core

**This repository is public.** `github.com/misorder/misorder`, Apache-2.0. The
hosted product is a separate private repository, `github.com/misorder/platform`,
in the same organisation.

Nothing hosted goes here. Not behind a feature flag, not behind a licence check,
not as a stub. If a change needs state that outlives one run, a second machine,
or a network, it belongs in the platform, and what belongs here is the document
that carries the information across.

**The coupling between the two is file formats and process boundaries, never a
Rust API.** The platform does not depend on this crate. It reads documents this
engine writes, writes documents it reads, and runs `mis` as a child process.
[`docs/INTERFACES.md`](docs/INTERFACES.md) is that contract, and it is the file
to read before changing any of these:

- the scenario TOML
- the corpus TOML (`corpus::FORMAT_VERSION`)
- the trace JSON Lines (`trace::FORMAT_VERSION`)
- the run and sweep report JSON (`report::run::FORMAT_VERSION`)
- the CLI surface and its three exit codes

Everything else in this repository is free to be refactored in any release, and
should be. An open core whose internals have become a compatibility surface
stops being developed.

The dividing line for a new feature is **stateless and local stays open;
persistent and shared is hosted.** Grouping failures within one sweep is open,
because it needs no state and it is what makes local output honest. Tracking
which pull request introduced a signature is hosted, because it needs a
database.

Open, permanently: the runner, the scenario format, the proxy layer, every
adapter, the decision recorder, the seeded scheduler, the built-in invariants,
trace shrinking, local fuzzing, any simulated dependency, the virtual clock, the
scrubber, the transcript format.

Three of those are specifically not moveable. Paywalling an **adapter** ends the
community contribution that is the only way the long tail of vendors is ever
covered. Paywalling **shrinking** makes the free tier produce failures less
useful than the incident they predicted. Paywalling the **virtual clock** makes
the free tier slow, and everyone concludes the tool is slow.

## The engine never phones home

No network client, no account, no credentials, no telemetry, no licence check,
no usage counter. The only sockets this process opens are the Docker daemon, the
dependencies it started, and the service under test.

This is a sales requirement, not a preference. Buyers in this segment treat
silent collection of anything resembling production traffic as a compliance
incident rather than a PR problem, and one security review that finds an
unexpected outbound connection ends the conversation permanently. The claim
survives only if it stays literally true, so it is checkable by reading this
repository.

It also follows from the pricing. Charging per service under test or per
integration monitored, rather than per compute, means there is nothing to meter:
a seed-hour meter would make someone cap the nightly sweep at 10k, the bug that
needed seed 71830 would never surface, and they would churn saying it never
found anything.

## Verification

Run checks from the repository root and scope them to the package changed:

```bash
cargo fmt --check
cargo clippy -p <package> --all-targets --all-features -- -D warnings
cargo test -p <package> --lib --bins --tests
```

`cargo test --workspace` stays hermetic: no Docker, no network. Anything needing
a real container goes behind a non-default feature or into `docker/`. Do not
claim a test passed if it was skipped for a missing dependency.

## Writing style

This applies to everything you write: code comments, documentation, commit
messages, pull request text, and replies to the user.

- Never use an em-dash or an en-dash. Rewrite the sentence with a comma, a
  colon, a full stop, or brackets. A hyphen inside a compound word is fine.
- Use simple, direct language. Prefer short sentences and plain words.
- Cut filler and hedging. Say the thing once, then stop.
- Comments say why, not what. The reasoning behind a decision that looks
  arbitrary is the part that survives.

## Change handoff

State which package and binaries are affected, and report the exact verification
commands run and their outcome. Call out anything that changes the scenario
format, the trace format, the CLI surface, or the meaning of a seed separately:
the last one invalidates every committed reproducer in every user's repository.

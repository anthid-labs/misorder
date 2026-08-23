# misorder

Your integration tests run one ordering. Production runs a different one at 3am.

misorder runs your service against its real dependencies, takes every timing and
failure decision from a seeded PRNG, and explores thousands of orderings of the
same scenario. When one breaks you, it shrinks the failure to the six events
that caused it.

The power of misorder is that it can find bugs that your tests never could;
instead of a single or small suite of custom integration tests or synthetics, 
we test tens of thousands of variants and
combinations for the weak points unknown until a production outage.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Status: the HTTP loop closes; the other two protocols do not yet.** An HTTP
scenario runs end to end today — `mis run` starts your service, puts the proxy
in front of it, drives the workload through it, checks the invariants and hands
back a shrunk reproducer, with no Docker involved. What is not done: the NATS
and Postgres wire adapters, and the container orchestration the scenarios
needing them would use. Those report `not supported yet` rather than pretending.
See [Roadmap](#roadmap).

## The words this README uses

The terms the rest of this document leans on. Each one links to the section
that goes further.

**Ordering.** The sequence in which concurrent things actually happened: which
response came back first, whether the ack beat the redelivery, whether the
refresh finished before the request that was already in flight. Your code has
one ordering in test and a different one at 3am. Orderings are the bug class
this whole tool is about.

**Scenario.** The one TOML file you write. It declares what to run, what it
depends on, what to drive at it, which faults are permitted, and what must
always be true. There is no SDK and nothing to import — your service connects to
an address misorder gives it and behaves normally. See
[The interface is a TOML file](#the-interface-is-a-toml-file).

**Adapter.** The piece that speaks one wire protocol — `http`, `nats`,
`postgres` — in both directions, so the service and its dependency both think
they are talking to the real thing. Adding one is the intended way to
contribute, and the surface is deliberately small: bind, accept, speak the
protocol, ask before branching. Every adapter is open, because the long tail of
vendors is only ever covered by the person who needed one.

**Fork.** Any point in a run where things could go two ways: a connection is
accepted or refused, a response goes now or in 40ms, an ack is delivered or
swallowed. The proxy never decides a fork itself — it asks the scheduler.

**Ordinal.** The number that names a fork, counted per connection and per kind
of fork. The third response fork on connection 2 is ordinal 2 of
`(Response, conn 2)`. It exists so a fork has a stable identity across runs: on
replay, a decision is looked up by `(kind, connection, ordinal)`, so an adapter
that numbered its forks differently on a second run would replay
plausible-looking decisions at the wrong places, and nothing would report it.

**Seed.** One integer. It is the *entire* input that decides a schedule, so one
scenario file plus 10,000 seeds is 10,000 scenarios with no generator to write.
Seeds are not ordered or related: 8837291 and 8837292 produce completely
unrelated schedules, which is why you can never "shrink the seed".

**Deterministic.** Same seed, same run — on any machine, on any number of cores,
in any thread order. This is stronger than it sounds and it is not free.

The obvious implementation is one PRNG advanced once per decision, and it is
wrong. A run has several proxied connections being served at once; if they all
draw from one sequential stream, the schedule depends on the order tasks happen
to reach it, which the OS decides. Same seed, different machine, different run.

So there is no stream. Every fork derives *its own* generator from
`(seed, kind, connection, ordinal)`, and ChaCha8 turns that into an independent
draw. Nothing is shared, so there is nothing to race over, and concurrency stops
being able to affect the answer. ChaCha8 rather than the standard library's
`StdRng` for a related reason: `StdRng`'s algorithm is explicitly not stable
across releases, so a dependency bump would silently renumber every seed and
invalidate every committed reproducer in every user's repository. See
[Determinism, and what it actually costs](#determinism-and-what-it-actually-costs).

**Decision.** The answer at a fork — deliver now, delay 40ms, drop, close the
connection, reorder, corrupt a byte. Every decision has a **neutral choice**
(deliver immediately, change nothing), and that is a design requirement rather
than a convenience: it is what lets shrinking say "this fault was available and
was not needed" instead of deleting the line.

**Trace.** The complete list of every decision a run made, as JSON Lines: a
header, then one line per decision, each naming its fork and the answer it got.
It is what turns a failure from an anecdote into an artifact — a trace is a
full, replayable description of one run, small enough to commit to your
repository and read in a diff.

The thing to understand about a trace is what is *not* in it. A trace records
decisions, not messages. "The response on connection 2 was held for 40ms" is a
line; the body of that response is not, and neither is any payload the workload
sent. That is a deliberate property rather than an omission: it is what lets
someone in a regulated industry share a reproducer at all, and it is checked
rather than assumed.

**Replay.** Re-running a trace instead of a seed. The same decisions at the same
forks, in seconds, on every pull request. A committed reproducer either
reproduces or it does not — it is not a flaky test.

**Reproducer.** A shrunk trace, committed. This is the unit of work the tool
exists to produce: the artifact you attach to a ticket, hand to a colleague, or
run in CI on every pull request. It is not a flaky test — it is an exact
schedule that either reproduces or does not.

**Shrinking.** Taking a failure of 847 decisions down to the six that actually
caused it. It works by replacing decisions with their neutral choice, re-running,
and keeping the replacement if the run still fails — using delta debugging
(ddmin) rather than a single greedy pass, because a greedy pass gets stuck
whenever two decisions are only redundant together. The result is *1-minimal*:
removing any single remaining decision makes the failure go away. Not the
globally smallest set, which is exponential to find and not worth it.

The negative space in the output is worth as much as the steps. "Postgres was
not involved" tells you which half of your system to stop reading, and naming
the permitted faults that turned out not to be needed stops you concluding the
bug needs a network partition when it needs one dropped ack. See
[Shrinking](#shrinking).

**Invariant.** Something that must be true no matter which ordering happened. An
invariant is what turns "the run finished" into a verdict: without one, a fuzzer
that explored ten thousand orderings has nothing to report.

They come in two kinds and you need both.

*Built-in* invariants ship with each adapter and take zero input from you,
because they encode the semantics of the dependency itself —
`no_delivery_after_ack`, `max_deliver_respected`, `no_commit_after_error`. This
is what gets a first-time user a caught bug before they have learned what the
tool is.

*Yours* are the domain assertions no protocol invariant could know, because
nothing about NATS understands that fills never exceed order quantity. The shape
to reach for is a query that searches for the bad state and expects nothing back
— `expect = "empty"` — because it then needs no knowledge of how many rows a
correct run produces, and stays a test of your service rather than of your
scenario. Write five, get ten thousand orderings.

`mis check` prints which invariants your scenario actually resolves to and marks
the ones that are specified but not yet implemented, so you find out how much of
your scenario is real before spending an hour of compute on it. See
[Built-in invariants](#built-in-invariants) and
[Your invariants](#your-invariants).

**Corpus.** A directory of `<vendor>.toml` files recording what a vendor was
actually observed doing, pointed at with `--corpus <directory>` or
`MISORDER_CORPUS`. Each entry is a named **behaviour flag** — written as a
sentence about the vendor, `no_ack_on_second_replace` rather than `ls_bug_14`,
because the name appears in scenarios, reports and drift alerts.

What makes it a corpus rather than a wiki is that every behaviour carries its
**provenance**, and the three kinds are ranked honestly:

- `recorded` — observed on the wire, carrying the digest of the transcript it
  came from, so a consumer can verify the entry rather than trust it. The
  strongest claim, and the only one that cannot be got by reading.
- `documented` — the vendor said so, in a changelog or a support ticket. Weaker
  than a recording, and worth having because it dates the change.
- `reported` — someone else hit it, in a GitHub issue or a forum thread. The
  weakest claim, and often the first sign of a real one.

The format is versioned (`corpus::FORMAT_VERSION`) because an entry contributed
today should still be readable by a build shipped in two years.

### Why not just chaos testing

Chaos engineering injects faults at random. That finds real problems, and it
fails at the part that decides whether anyone acts on them.

- **A random failure is not reproducible.** You get an incident, not a test. The
  artifact is a Slack thread, and the fix is verified by not seeing it again for
  a while.
- **A random failure is not minimal.** Something broke somewhere in a five
  minute window with forty things going wrong at once. Working out which of them
  mattered is manual, and it is most of the cost.
- **Nobody can put it in CI.** A test that fails 0.3% of the time on a schedule
  nobody controls gets retried, then muted, then deleted.

misorder keeps the fault injection and fixes all three. Every run is addressed by
one integer, every decision is recorded, the failure shrinks to its minimal
cause automatically, and what you commit replays exactly. The point is not that
it breaks your service in more interesting ways — it is that when it does, you
get something you can hand to someone.

## Who this is for

Teams whose bugs come from external systems behaving differently than
documented: brokers, payment processors, banking APIs, carriers, EHR. Not the
median web team.

## The interface is a TOML file

No SDK. No imports. No build flags. Your service connects to the address
misorder gives it and behaves normally, so a Go service and a Rust service adopt
this identically. Cost scales with protocols, not with languages.

```toml
name = "dead_letter_no_redelivery"

[[system]]
run = "./target/debug/ledger"
ready_when = "nats_subscription_active"

[[deps.nats.streams]]
name = "LEDGER"
subjects = ["ledger.>"]
max_deliver = 5
ack_wait = "30s"
discard = "old"

[deps.postgres]
migrations = "./migrations"

[[workload]]
publish = "ledger.org.org_1.account.acct_1.order"
payload = { order_id = "ord_1", kind = "fill", qty = 100 }

[faults]
enabled = ["ack_timeout", "redelivery", "connection_drop", "reorder"]

[[invariants]]
builtin = "no_infinite_redelivery"
window = "5m"
same_payload_max = 10
```

That is the only artifact you write. [`misorder.example.toml`](misorder.example.toml)
documents every key.

## Install

```bash
cargo install misorder-cli
```

The package is `misorder-cli`; the command it installs is `mis`. Two crates are
published, because the two audiences are different:

| Crate                                                     | What it is                                          |
| --------------------------------------------------------- | --------------------------------------------------- |
| [`misorder`](https://crates.io/crates/misorder)           | The library: the engine, for embedding in a program. |
| [`misorder-cli`](https://crates.io/crates/misorder-cli)   | The `mis` command.                                   |

Or build from source:

```bash
cargo build --release --package misorder-cli
```

## Start here: a worked example

Five Stripe webhooks for one subscription, against a deliberately naive billing
handler. Everything it needs is in this repository and none of it is Docker:

```bash
cargo build --workspace
./target/debug/mis fuzz examples/stripe_invoice_lifecycle.toml --seeds 400
```

```text
  applied evt_1    customer.subscription.created    -> incomplete
  applied evt_2    invoice.payment_failed           -> past_due
  applied evt_3    invoice.payment_succeeded        -> active
  applied evt_5    customer.subscription.deleted    -> canceled
  applied evt_4    invoice.payment_failed           -> past_due

MINIMAL REPRODUCER: stripe_invoice_lifecycle
seed 3, 1 of 1 decisions

  1. [    24ms] conn:1 reorder delivery behind #5 (POST /webhooks/stripe)

  terminal_state_is_final: /checks/reopened_after_cancel returned 1 row(s), expected none

  Faults 'delay' and 'connection_drop' were not required.

stripe_invoice_lifecycle: 400 seed(s) in 11.3s
  388 passed, 12 failing across 1 distinct failure(s)
    f3f080cd86bcbb60 terminal_state_is_final (12 seed(s), first: 3 6 41 93 189)
```

One delivery arrived after the cancellation, and a cancelled customer is being
dunned again. Nobody wrote a bug: the handler even deduplicates on the event id,
which is exactly what Stripe's documentation tells you to do. The duplicate
advice is a heading with a code sample; the ordering advice is one sentence with
nothing to copy, and it is the one that costs money.

Twelve seeds found it and they are **one** failure, not twelve — grouped by the
signature of the shape, which is what stops a sweep reading as a wall of red.

The pieces: [`examples/stripe_invoice_lifecycle.toml`](examples/stripe_invoice_lifecycle.toml)
is the scenario, [`apps/billing-demo`](apps/billing-demo) is the service under
test, and [`examples/corpus/stripe.toml`](examples/corpus/stripe.toml) is where
the claims about Stripe's behaviour come from, each with its source.

Reproduce it, then commit it:

```bash
./target/debug/mis run examples/stripe_invoice_lifecycle.toml --seed 3 --shrink \
  --trace tests/reproducers/reopened-after-cancel.jsonl

./target/debug/mis replay tests/reproducers/reopened-after-cancel.jsonl \
  -s examples/stripe_invoice_lifecycle.toml
```

That second command runs in under a second on every pull request, and either
reproduces or does not.

There is also a version of the same story with no scenario file at all, driving
the engine as a library — [`crates/misorder/examples/stripe_webhook_ordering.rs`](crates/misorder/examples/stripe_webhook_ordering.rs),
runnable with `cargo run -p misorder --example stripe_webhook_ordering`.

## Quick start

```bash
mis check scenario.toml                    # validate, print what it will actually check
mis run scenario.toml --seed 8837291       # one ordering
mis fuzz scenario.toml --seeds 10000 --parallel 16
mis replay trace-8837291.jsonl             # re-run a recorded one
mis shrink trace-8837291.jsonl -o repro.jsonl
```

Results are also available as a versioned JSON document, for anything that wants
to store, compare or comment on them:

```bash
mis run scenario.toml --seed 8837291 --format json
mis fuzz scenario.toml --seeds 10000 --report sweep.json
```

Large sweeps split across machines with no coordinator. Each one computes its
own slice from two integers:

```bash
mis fuzz scenario.toml --seeds 100000 --shard 7/64 --report shard-7.json
```

Exit codes are the thing CI needs, and they are three-valued on purpose:

| Code | Meaning                                                     |
| ---- | ----------------------------------------------------------- |
| 0    | Every invariant held.                                        |
| 1    | misorder could not run. Not a finding.                       |
| 2    | An invariant was violated. A finding.                        |

Collapsing 1 and 2 means a broken Docker socket looks like a caught bug, someone
chases it for an hour, and the next real finding gets the same treatment.

## How it works

Six stages, and the boundaries between them are where the design lives.

1. **Scenario.** One TOML file declares what to run, what it depends on, what to
   drive at it, which faults are permitted, and what must always be true.
2. **Environment.** The declared dependencies start as real containers. Real
   Postgres, real NATS. Fidelity is free here and unarguable, and it matters
   more than speed.
3. **Proxy.** Every connection from the service goes through misorder, which
   speaks the real wire protocol in both directions. This is where all
   nondeterminism gets injected: drop the connection, delay the response,
   reorder two in-flight replies, swallow an ack, hold statement B until
   statement A commits.
4. **Schedule.** Every one of those choices comes from a PRNG seeded with a
   single integer. One scenario file plus 10,000 seeds is 10,000 scenarios.
5. **Trace.** Every choice is appended to a JSON Lines file. A trace is a
   complete, replayable description of one run, which is what turns a failure
   from an anecdote into an artifact you can commit.
6. **Invariants.** Built-ins that ship with each adapter, plus the domain
   assertions only you can write.

## Determinism, and what it actually costs

The obvious implementation is one PRNG advanced once per decision. It is wrong,
and the reason only shows up under load.

A run has several proxied connections being served concurrently. If they draw
from a shared sequential PRNG, the schedule depends on the order the tasks reach
it, which the OS decides. Same seed, different machine, different run.
Determinism would be a claim rather than a property, and the first time
someone's reproducer failed to reproduce, the tool would be over.

So there is no stream. Each fork derives its own generator from
`(seed, kind, connection, ordinal)`, and ChaCha8 turns that into an independent
draw. Concurrency stops mattering, because nothing is shared to race over.

The algorithm is ChaCha8 rather than `rand::rngs::StdRng` for the same reason:
StdRng's algorithm is explicitly not stable across `rand` releases, so a minor
version bump would silently renumber every seed and invalidate every committed
reproducer in every user's repository.

## Shrinking

847 decisions collapse to six.

```
MINIMAL REPRODUCER: dead_letter_no_redelivery
seed 8837291, 6 of 847 decisions

  1. [    12ms] conn:1 delay delivery by 40ms (ledger.org.org_1.account.acct_1.order)
  2. [    41ms] conn:1 drop ack (ledger.org.org_1.account.acct_1.order)
  3. [   120ms] conn:1 drop ack (ledger.org.org_1.account.acct_1.order)
  4. [ 30400ms] conn:2 drop delivery (ledger.dead_letter)
  ...

  no_infinite_redelivery: the payload on ledger.dead_letter was delivered
  11 times within 300s, over the limit of 10

  Postgres was not involved. Faults 'reorder' and 'connection_drop' were
  not required.
```

The negative space is worth as much as the steps. "Postgres was not involved"
tells you which half of your system to stop reading, and naming the faults that
were available and not needed stops you concluding the bug needs a network
partition when it needs one dropped ack.

Two things about how this works:

**You cannot shrink the seed.** Seeds 8837291 and 8837292 produce unrelated
schedules. There is no gradient to descend and no meaning to a halfway point.
What shrinks is the trace.

**Removing a decision does not delete the line.** It becomes the neutral choice:
the fork still happens and takes the boring path. That is what makes the output
readable as "this fault was available and was not needed", and it is why every
decision has a neutral counterpart by construction.

The search is delta debugging (ddmin), not a single pass of removals. A greedy
pass gets stuck whenever two decisions are only redundant together: neither can
go alone, so neither goes. Shrinking 847 decisions to three costs about 40
re-runs.

Shrinking is not an add-on. A version of this tool that found failures and did
not reduce them would hand you something *less* useful than the incident it
predicted.

## Built-in invariants

Zero user input. They encode the semantics of the dependency itself, so a
first-time user gets a caught bug before learning what this tool is.

| Invariant                                | Dependency | Status  |
| ---------------------------------------- | ---------- | ------- |
| `max_deliver_respected`                  | nats       | works   |
| `no_delivery_after_ack`                  | nats       | works   |
| `no_infinite_redelivery`                 | nats       | works   |
| `consumer_filter_excludes_dead_letter`   | nats       | works   |
| `no_commit_after_error`                  | postgres   | works   |
| `no_query_outside_transaction`           | postgres   | planned |
| `set_local_role_survives_pooler`         | postgres   | planned |
| `every_request_reaches_terminal_state`   | http       | works   |
| `idempotent_retry_returns_same_response` | http       | works   |
| `eventually_quiescent`                   | any        | works   |

`mis check` prints this for your scenario, and marks the planned ones. A
scenario permitting four faults and naming one invariant reads as thorough, and
`check` is where you find out how much of that is real before spending an hour
of compute.

## Your invariants

Protocol invariants cannot know that fills never exceed order quantity.

```toml
[[invariants]]
name = "fills_never_exceed_order_qty"
check = "sql"
query = """
select 1 from orders o
join fills f on f.order_id = o.id
group by o.id, o.qty having sum(f.qty) > o.qty
"""
expect = "empty"
```

Write five, get ten thousand orderings. `expect = "empty"` is the shape to reach
for: a query that searches for the bad state needs no knowledge of how many rows
a correct run produces, so it stays a test of the service rather than of the
scenario.

## CI

The exit code is the whole integration. Three-valued, so a broken Docker socket
never looks like a caught bug:

```bash
mis fuzz scenario.toml --seeds 5000 --parallel 16
```

For anything that wants to store or compare results, `--report` writes the
sweep as JSON and `--report-format csv` writes one row per failing seed.
Both are described in [`docs/INTERFACES.md`](docs/INTERFACES.md).

**Sweeps belong on a schedule, not on every pull request.** Five thousand
orderings is minutes, and the point of a sweep is to find something new. What
belongs on every pull request is what the sweep already found:

```bash
mis replay tests/reproducers/reopened-after-cancel.jsonl -s scenario.toml
```

A shrunk trace committed to the repository is not a flaky test. It is an exact
schedule that runs in under a second and either reproduces or does not.

## Layout

| Path                    | Purpose                                                       |
| ----------------------- | ------------------------------------------------------------- |
| `crates/misorder`       | The library: scenario, orchestrator, proxy, schedule, trace, invariants, shrinker. |
| `apps/misorder-cli`     | The `mis` binary: argument parsing, logging setup, exit codes. |
| `apps/billing-demo`     | The service under test in the worked example. Wrong on purpose. |
| `examples/`             | Scenario files, including the one in this README, and a corpus. |
| `docker/`               | Dockerfile and a compose example.                              |
| `docs/INTERFACES.md`    | The stable formats anything built on top reads and writes.     |
| `docs/LEDGER_IMPORT.md` | Reading recorded vendor behaviour out of a webhook events table. Specified, not implemented. |
| `.github/workflows/`    | Lint, test, scan, and publishing.                              |

The split is along process boundaries. Anything that decides, runs, or checks is
in the library, so it can be embedded and tested without a binary; the CLI is
argument parsing, the global log subscriber, and an exit code.

Workspace members are globbed, so a new crate under `apps/` or `crates/` is
picked up without editing the root `Cargo.toml`.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The suite is hermetic: no Docker, no network. That is possible because the
interesting logic is pure. The shrinker is tested against a synthetic oracle
that fails only when a specific set of decisions survives, so the search is
verified without running a single container. The scheduler is tested by
asserting that a fork gets the same answer however many other forks came first,
which is the property the whole tool rests on.

## Roadmap

**Phase 1, the loop.** One service, real dependencies, seeded faults, a failure
that reproduces, a minimal reproducer. Sellable on its own as reproducible chaos
testing with shrinking, which nobody ships today. This is what the tree is.

**Phase 2, fidelity.** Stop guessing what vendors do. Record real sessions in
passthrough mode, scrub them of everything customer-specific, replay them as
scenarios, and promote recorded surprises into named behaviour flags. The
permutation engine then explores every ordering of what a vendor actually does,
including the orderings nobody happened to observe. The scrubber is open source
and inspectable, and that is load-bearing: the buyers are regulated, and silent
collection of anything resembling production traffic is a compliance incident
rather than a PR problem.

There is a second way into the same corpus that needs nothing in the traffic
path. Most integrations already store every webhook they received, and every
duplicate, reorder and delay is already in that table.
[`docs/LEDGER_IMPORT.md`](docs/LEDGER_IMPORT.md) specifies reading it, with
Stripe as the worked example.

**Phase 3, speed.** Quiescence detection first, because it gates everything: to
advance a virtual clock safely you have to know the system is idle rather than
mid-computation. Then the virtual clock, so `ack_wait = "30s"` costs
microseconds. Then a simulated JetStream, which is first among the simulators
for a specific reason: `ack_wait` fires on the server's own timer with no client
involved, so a proxy cannot intercept a decision that never crossed the wire.
Then differential conformance, running the same scenario against the sim and the
real container and failing on disagreement.

### What stays open

This repository is the engine, and it is complete on its own: it runs, it finds
bugs, it shrinks them, and it needs no account.

**Permanently open:** the runner, the scenario format, the proxy layer, every
adapter, the decision recorder, the seeded scheduler, the built-in invariants,
trace shrinking, local fuzzing, any simulated dependency, the virtual clock, the
scrubber, and the transcript format. Nothing in that list moves, and three of
them could not move without breaking the tool.

**Adapters**, because the long tail of vendors is only ever covered by the
person who needed one, and a licence boundary there ends those contributions
entirely. **Shrinking**, because a version of this tool that found failures and
did not reduce them would produce something less useful than the incident it
predicted. **The virtual clock**, because a slow tool teaches everyone that the
tool is slow.

The engine has no network client, no account, no telemetry, no licence check,
and no usage counter. The only sockets this process opens are the Docker daemon,
the dependencies it started, and your service. That is checkable by reading this
repository, and it is meant to be.

[`docs/INTERFACES.md`](docs/INTERFACES.md) documents the formats anything built
on top reads and writes, so that anything you want to build around this — in any
language — has a stable surface to build on.

## Not done

- **The NATS and Postgres wire adapters.** The seam is defined and the fault
  vocabulary is complete; the codecs are not written. The HTTP one is:
  `proxy::http` speaks HTTP/1.1 and asks at every fork.
- **Egress HTTP.** The ingress placement is wired: the workload posts through
  the proxy to your service. The other direction — your service calling a vendor
  through the proxy — works when driven as a library, but a scenario cannot
  declare an HTTP dependency yet, so there is no `mis run` path to it.
- **Container orchestration.** `orchestrator::docker` connects to the daemon and
  reports a clear error; it does not yet start anything. A scenario declaring no
  dependencies never reaches it, which is why the worked example runs without
  Docker.
- **Publishing a workload step.** The NATS side of the driver. Posting is
  implemented, and posts go out pipelined on one connection followed by a
  half-close, which is what gives a reorder two requests in flight to swap.
- **Quiescence detection.** Phase 1 uses an idle window, which is a heuristic.
  It is deliberately conservative: calling quiescence during a 40ms CPU burst
  would manufacture a failure that never happened.

## Contributing

Contributions are welcome: issues, bug reports, and pull requests alike. Adding
a protocol adapter is the intended path, and the surface is deliberately small:
bind, accept, speak the protocol, ask before branching.

[`AGENTS.md`](AGENTS.md) has the conventions in full. The parts that are not
negotiable are in **Determinism is the product**: an adapter that reads the
clock or calls `rand` does not fail loudly, it produces traces that replay into
a different run.

Before opening a pull request:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

CI runs exactly these, with warnings denied.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

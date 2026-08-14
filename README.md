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

**Status: skeleton. The seams work, the protocol adapters do not yet.** What is
implemented and tested today: the scenario format, the seeded scheduler, the
trace format and replay, the delta-debugging shrinker, the built-in invariant
set, the reproducer and JUnit output, and the CLI around all of it. What is not:
the Docker orchestration and the NATS and Postgres wire adapters. Those report
`not supported yet` rather than pretending. See [Roadmap](#roadmap).

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

Shrinking ships in the open source tier. Withholding it would mean the free tier
produces failures *less* useful than the incident they predicted.

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

```bash
mis fuzz scenario.toml --seeds 5000 --parallel 16 --junit junit.xml
```

A shrunk trace committed to the repository is not a flaky test. It is an exact
replayable sequence:

```bash
mis replay tests/reproducers/dead-letter.jsonl
```

That runs in seconds on every pull request and either reproduces or does not.

## Layout

| Path                    | Purpose                                                       |
| ----------------------- | ------------------------------------------------------------- |
| `crates/misorder`       | The library: scenario, orchestrator, proxy, schedule, trace, invariants, shrinker. |
| `apps/misorder-cli`     | The `mis` binary: argument parsing, logging setup, exit codes. |
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

**Phase 4, commercial surface.** Vendor drift detection, triage and dedup, a
shared reproducer library, compliance artifacts, a hosted corpus.

### Open core

This repository is the engine, and it is complete on its own: it runs, it finds
bugs, it shrinks them, and it needs no account. There is a hosted product
alongside it, and the split is structural rather than a crippled tier.

**Open, permanently:** the runner, the scenario format, the proxy layer, every
adapter, the decision recorder, the seeded scheduler, the built-in invariants,
trace shrinking, local fuzzing, any simulated dependency, the virtual clock, the
scrubber, the transcript format.

**Hosted:** the curated vendor corpus and drift detection, compliance artifacts,
cross-run triage and history, distributed seed-search orchestration.

The dividing line is **stateless and local stays open; persistent and shared is
hosted.** Grouping failures within one sweep is here, because it needs no state
and it is what makes the local output honest. Tracking which pull request
introduced a failure needs a database, and a stateless CLI should not grow one.

Three things are specifically never paywalled. **Adapters**, because the long
tail of vendors is only ever covered by people who needed one, and a licence
boundary there ends those contributions. **Shrinking**, because withholding it
would make the free tier produce failures less useful than the incident they
predicted. **The virtual clock**, because an open source tool that is slow
teaches everyone that the tool is slow.

The engine has no network client, no account, no telemetry, and no usage
counter. The only sockets it opens are the Docker daemon, the dependencies it
starts, and your service. That is checkable by reading this repository, and it
is meant to be.

[`docs/INTERFACES.md`](docs/INTERFACES.md) documents the formats anything built
on top reads and writes.

## Not done

- **The NATS and Postgres wire adapters.** The seam is defined and the fault
  vocabulary is complete; the codecs are not written. The HTTP one is:
  `proxy::http` speaks HTTP/1.1 and asks at every fork.
- **Wiring the adapters into a run.** `runner` does not start a proxy, and a
  scenario has no way to ask for one. The HTTP adapter binds, serves and is
  tested on its own; nothing calls it yet.
- **Container orchestration.** `orchestrator::docker` connects to the daemon and
  reports a clear error; it does not yet start anything.
- **The workload driver.** Publishing and posting are declared and validated,
  and neither is wired to a client yet. For HTTP the driver owes the ingress
  proxy one thing: send the posts without waiting for each answer, then shut
  down the write half, so a request the schedule deferred has something to be
  overtaken by and something to release it.
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

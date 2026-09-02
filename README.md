# misorder
[![🔐 static security analysis](https://github.com/anthid-labs/misorder/actions/workflows/security-static.yml/badge.svg)](https://github.com/anthid-labs/misorder/actions/workflows/security-static.yml)
[![🏗️ Build and Push Docker Image](https://github.com/anthid-labs/misorder/actions/workflows/build-and-push.yml/badge.svg)](https://github.com/anthid-labs/misorder/actions/workflows/build-and-push.yml)

Your integration tests run one ordering. Production runs a different one at 3am.

misorder runs your service against its real dependencies, takes every timing and
failure decision from a seeded PRNG, and explores thousands of orderings of the
same scenario. When one breaks you, it shrinks the failure to the six events
that caused it.

[![Crates.io](https://img.shields.io/crates/v/misorder.svg)](https://crates.io/crates/misorder)
[![Docs.rs](https://docs.rs/misorder/badge.svg)](https://docs.rs/misorder)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Status: the loop closes for HTTP, Redis and NATS. Postgres does not yet.**
`mis run` starts your service, puts a proxy between it and everything it talks
to, drives the workload through that, checks the invariants and hands back a
shrunk reproducer, with no Docker involved, in either direction:

- **Ingress**, where the vendor calls you. A webhook endpoint, with the workload
  driver standing in for Stripe.
- **Egress**, where you call the dependency. Your service reaches Redis or NATS
  through the proxy by reading a different value out of `REDIS_URL` or
  `NATS_URL`.

What that finds, on the example in this repository: **400 orderings of five
Stripe webhooks in 11.3 seconds**, twelve of them failing, grouped into **one**
distinct bug, shrunk to **a single reordered delivery**.

It is for teams whose bugs come from external systems behaving differently than
documented: brokers, payment processors, banking APIs, carriers, EHR. Not the
median web team.

How it behaves when a dependency is unreachable, a scenario names something
unbuilt, or a sweep is not isolated is documented in
[Behaviour when things go wrong](#behaviour-when-things-go-wrong) instead of
left to be discovered. Everything unimplemented reports `not supported yet`
rather than pretending. See [Not done](#not-done).

## Install

```bash
cargo install misorder-cli
```

The package is `misorder-cli`; the command it installs is `mis`. Two crates are
published, because the two audiences are different:

| Crate                                                   | What it is                                           |
| ------------------------------------------------------- | ---------------------------------------------------- |
| [`misorder`](https://crates.io/crates/misorder)         | The library: the engine, for embedding in a program.  |
| [`misorder-cli`](https://crates.io/crates/misorder-cli) | The `mis` command.                                    |

Library documentation is on [docs.rs](https://docs.rs/misorder).

Or with Docker:

```bash
docker pull ghcr.io/anthid-labs/misorder:latest
```

Two tags per build from the default branch:

| Tag            | What it points at                                 |
| -------------- | ------------------------------------------------- |
| `sha-<commit>` | Exactly one build. The only tag that never moves.  |
| `latest`       | The newest build on the default branch.            |

Built natively per architecture and merged into one multi-arch manifest, so
`linux/amd64` and `linux/arm64` both resolve from the same tag. Or build from
source:

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

The one artifact you write is a TOML file. No SDK, no imports, no build flags:
your service connects to the address misorder gives it and behaves normally, so
a Go service and a Rust service adopt this identically. Cost scales with
protocols, not with languages.

```toml
name = "redis_naive_lock"

[[system]]
run = "./target/debug/worker"
ready_when = "immediate"

# Already running - `docker compose up redis`. misorder puts a proxy in front of
# it and points the service at that instead, through REDIS_URL.
[deps.redis]
address = "127.0.0.1:6379"

[faults]
enabled = ["delay", "reorder", "connection_drop"]

# Ships with the adapter and takes no input from you: the whole lock exchange
# crosses the wire, so the proxy can see a client release a lock it no longer
# owns without knowing anything about your service.
[[invariants]]
builtin = "lock_released_by_owner"

[[invariants]]
name = "no_order_processed_twice"
check = "http"
query = "/checks/duplicate_fulfilment"
expect = "empty"
```

`mis check` is the one to run first. It prints which invariants your scenario
actually resolves to and marks the ones that are specified but not yet
implemented, so you find out how much of your scenario is real before spending
an hour of compute on it.

[`misorder.example.toml`](misorder.example.toml) documents every key, including
the Postgres block whose adapter is still being built. The full reference is in
[The scenario file](#the-scenario-file).

## Worked examples

All three are in this repository. The first needs nothing but a Rust toolchain.

### Webhooks that arrive out of order

Five Stripe webhooks for one subscription, against a deliberately naive billing
handler. No Docker:

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

  delivery order
    sent      #1 #2 #3 #4 #5 #6
    received  #1 #2 #3 #4 #6 #5

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

Twelve seeds found it and they are **one** failure, not twelve, grouped by the
signature of the shape, which is what stops a sweep reading as a wall of red.

Reproduce it, then commit it:

```bash
./target/debug/mis run examples/stripe_invoice_lifecycle.toml --seed 3 --shrink \
  --trace tests/reproducers/reopened-after-cancel.jsonl

./target/debug/mis replay tests/reproducers/reopened-after-cancel.jsonl \
  -s examples/stripe_invoice_lifecycle.toml
```

That second command runs in under a second on every pull request, and either
reproduces or does not.

The pieces: [`examples/stripe_invoice_lifecycle.toml`](examples/stripe_invoice_lifecycle.toml)
is the scenario, [`apps/demos`](apps/demos) builds the service under test
(`billing_demo`), and [`examples/corpus/stripe.toml`](examples/corpus/stripe.toml)
is where the claims about Stripe's behaviour come from, each with its source.

### A lock released by the wrong owner

That one is **ingress**: the vendor calls you. The egress example is a worker
taking a Redis lock the way everybody writes it first, and it needs a Redis you
already have:

```bash
mis fuzz examples/redis_naive_lock.toml --seeds 60
```

```text
MINIMAL REPRODUCER: redis_naive_lock
seed 7, 1 of 2 decisions

  1. [   212ms] conn:1 delay statement by 160ms (GET)

  lock_released_by_owner: conn:1 sent DEL on a key currently held by conn:2 under
  a different token; releasing a lock you no longer own lets two clients into the
  same critical section. Release with a script that compares the token first.

  Faults 'reorder' and 'connection_drop' were not required.
```

`SET key token NX PX ttl` to acquire, `DEL key` to release. Hold one reply long
enough that the work outlasts the lock, and the release frees somebody else's.
`lock_released_by_owner` ships with the adapter and takes no input from you: the
whole exchange crosses the wire, so the proxy sees it without knowing anything
about the service.

The worker reads `REDIS_URL` and is never told it is not talking to Redis.
[`apps/demos`](apps/demos) builds the service (`redis_demo`);
[`examples/redis_naive_lock.toml`](examples/redis_naive_lock.toml) is the
scenario.

### A dead letter that comes back

A JetStream consumer filtered on `ledger.>` that dead-letters what it cannot
handle to `ledger.dead_letter`. Both lines are the ordinary thing to write, and
`ledger.>` matches `ledger.dead_letter`, so the dead letter comes straight back
to the consumer that produced it. It needs a NATS with JetStream on:

```bash
docker run -d --rm -p 14222:4222 nats:2.10-alpine -js
mis run examples/dead_letter_loop.toml --seed 8837291
```

```text
MINIMAL REPRODUCER: dead_letter_loop
seed 8837291, 4 of 4 decisions

  1. [   212ms] conn:1 delay delivery by 64ms
  2. [   277ms] conn:1 delay ack by 36ms ($JS.ACK.LEDGER.LEDGER_WORKER.1.6.7...)
  3. [   355ms] conn:1 drop delivery (ledger.dead_letter)
  4. [  7566ms] conn:1 drop delivery

  no_infinite_redelivery: the payload on ledger.dead_letter was delivered 4 times
  within 300s, over the limit of 3

  Fault 'redelivery' was not required.
```

`max_deliver` does not stop this, which is the part worth sitting with: every
republish is a new message with a fresh delivery count, so the server's own
limit is never reached. Every component behaves exactly as documented and the
system does not stop.

Note what the last line says, and what the scenario's fault list does not
change. This failure is not about ordering, and a run permitting no faults at
all finds it just the same. That is the honest baseline a scenario should be
able to report, rather than a tool that only ever blames the schedule.

No vendor is involved either. Not every ordering bug comes from someone else's
system, and the two lines that combine to cause this one live in different
files.

The worker reads `NATS_URL` and is never told it is not talking to NATS.
[`apps/demos`](apps/demos) builds the service (`nats_demo`);
[`examples/dead_letter_loop.toml`](examples/dead_letter_loop.toml) is the
scenario.

### As a library

There is a version of the Stripe story with no scenario file at all, driving the
engine directly:
[`crates/misorder/examples/stripe_webhook_ordering.rs`](crates/misorder/examples/stripe_webhook_ordering.rs),
runnable with `cargo run -p misorder --example stripe_webhook_ordering`.

## How it works

A scenario declares what to run; every connection between your service and
anything else is routed through a proxy that speaks the real wire protocol; the
proxy asks a scheduler at every point where things could go two ways; and the
answers are recorded. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) draws each
stage.

Two things follow from that shape and explain most of the design:

- **The proxy never decides anything itself.** Any point where a run could go
  two ways is a **fork**: a connection is accepted or refused, a response goes
  now or in 40ms, an ack is delivered or swallowed. An adapter's whole job is to
  speak the protocol and *ask* before branching. That is what keeps the schedule
  in one place and makes it reproducible.
- **Every answer is written down.** The **trace** is the complete list of
  decisions a run made, as JSON Lines. A run is therefore replayable without a
  seed, which is what turns a failure from an anecdote into an artifact you can
  commit.

The dependencies themselves are real. Real Postgres, real NATS. Fidelity is free
here and unarguable, and it matters more than speed: forcing an exact
interleaving against a real server gives real serialization failures and real
isolation semantics, which no simulator reproduces by accident.

### Why not chaos testing

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
it breaks your service in more interesting ways. It is that when it does, you
get something you can hand to someone.

## The scenario file

TOML. It declares what to run, what it depends on, what to drive at it, which
faults are permitted, and what must always be true.

| Block            | Purpose                                                                     |
| ---------------- | --------------------------------------------------------------------------- |
| `name`           | *required.* Identifies the scenario in reports and reproducer signatures.    |
| `[[system]]`     | *required.* The service under test: `run`, `cwd`, `env`, `ready_when`, `listen_env`. |
| `[deps.*]`       | What it talks to, one block per dependency: `nats`, `postgres`, `redis`.     |
| `[[workload]]`   | What to drive at it.                                                         |
| `[faults]`       | Which faults the scheduler is permitted to choose.                           |
| `[[invariants]]` | What must hold. `builtin = "..."` or a query of your own.                    |
| `[run]`          | `timeout`, `ready_timeout`, `quiesce_after`.                                 |

`ready_when` decides when the workload is allowed to start, and getting it wrong
is the difference between a real finding and a race against your own startup:

| Value                     | Ready when                                        |
| ------------------------- | ------------------------------------------------- |
| `first_connection`         | *default.* The service opens its first proxied connection. |
| `nats_subscription_active` | A subscription is live on the NATS proxy.         |
| `postgres_connected`       | A Postgres session is established.                |
| `http_listening`           | The service is accepting on its `listen_env` port. |
| `immediate`                | Start driving at once.                            |

The faults a `[faults] enabled` list may name:

| Fault              | What it does                                                        |
| ------------------ | ------------------------------------------------------------------- |
| `delay`            | Hold a message without losing it.                                    |
| `reorder`          | Let a later in-flight message overtake an earlier one.               |
| `connection_drop`  | Close a connection mid-conversation, in either direction.            |
| `swallow_ack`      | Drop an acknowledgement outright. The server never learns the message was handled. |
| `ack_timeout`      | Hold an ack long enough that the server's own `ack_wait` expires first, so the ack arrives at a server that already redelivered. |
| `redelivery`       | Drop a delivery so the server sends it again. The counterpart to `ack_timeout`: this loses the message on the way out, that one loses the receipt on the way back. |
| `hold_statement`   | Hold one statement until another completes. Postgres-shaped.         |
| `corrupt_frame`    | Flip a byte in a frame. Rarely the cause of anything, and kept because when a vendor's framing is wrong it is the only fault that finds it. |

[`misorder.example.toml`](misorder.example.toml) documents every key.

### What your service is told

Nothing, beyond its ordinary configuration:

| Variable                          | When                                                |
| --------------------------------- | --------------------------------------------------- |
| the one you name in `listen_env`  | An ingress scenario. misorder picks a free port per run and sets it to that, then binds the proxy in front. |
| `REDIS_URL`, and one per proxied dependency | The proxy's address, never the dependency's. That separation is what makes the fault injection unavoidable rather than opt-in. |
| `MISORDER_SEED`                   | Always. Which run this is.                           |

The last one is worth a sentence, because ignoring it is a trap. Sixteen seeds
in parallel against one Redis is sixteen services writing the same keys, and the
failures a sweep then reports are about the collision rather than about the
ordering, the most expensive kind of wrong answer a testing tool can give. A
service that prefixes what it touches with the seed is isolated again; one that
ignores it is exactly as isolated as it was. That is the right shape for a
harness: offer the one fact only it has, and stay out of the decision.

### Environment

| Variable           | Purpose                                                    |
| ------------------ | ---------------------------------------------------------- |
| `MISORDER_CORPUS`  | Directory of recorded vendor behaviours. Same as `--corpus`. |
| `RUST_LOG`         | Log filter. Takes precedence over `--log-level`.            |
| `NO_COLOR`         | Disable colour. `--no-color` does the same.                 |
| `CLICOLOR_FORCE`   | Force colour on for a CI runner that renders it without being a terminal. |

Output is coloured when a terminal is attached and plain when it is not, so a
report piped to a file arrives without escape sequences. Green held, red broke,
yellow needs attention and is not a finding.

A sweep draws a progress bar while it runs, and a second one while it shrinks
what it found. Ten thousand seeds is minutes, and a tool that prints nothing for
minutes is one people interrupt to check it is alive. It draws only when a
terminal is attached.

When a command writes a trace to stdout, its diagnostics move to stderr. A log
line in the middle of a JSON Lines document makes the file unparseable at some
byte offset, and what the reader reports says nothing about the cause.

## Determinism

**Same seed, same run, on any machine, on any number of cores, in any thread
order.** This is stronger than it sounds and it is not free.

A **seed** is one integer, and it is the *entire* input that decides a schedule,
so one scenario file plus 10,000 seeds is 10,000 scenarios with no generator to
write. Seeds are not ordered or related: 8837291 and 8837292 produce completely
unrelated schedules, which is why you can never "shrink the seed".

The obvious implementation is one PRNG advanced once per decision, and it is
wrong. A run has several proxied connections being served at once; if they all
draw from one sequential stream, the schedule depends on the order tasks happen
to reach it, which the OS decides. Same seed, different machine, different run.
Determinism would be a claim rather than a property, and the first time
someone's reproducer failed to reproduce, the tool would be over.

So there is no stream. Every fork derives *its own* generator from
`(seed, kind, connection, ordinal)`, and ChaCha8 turns that into an independent
draw. Nothing is shared, so there is nothing to race over, and concurrency stops
being able to affect the answer.

Three terms in that tuple are worth naming, because they are what the trace is
addressed by:

- **Ordinal.** The number that names a fork, counted per connection and per kind
  of fork. The third response fork on connection 2 is ordinal 2 of
  `(Response, conn 2)`. It exists so a fork has a stable identity across runs: on
  replay, a decision is looked up by `(kind, connection, ordinal)`, so an adapter
  that numbered its forks differently on a second run would replay
  plausible-looking decisions at the wrong places.
- **Decision.** The answer at a fork: deliver now, delay 40ms, drop, close the
  connection, reorder, corrupt a byte.
- **The neutral choice.** Every decision has one — deliver immediately, change
  nothing — and that is a design requirement rather than a convenience. It is
  what lets shrinking say "this fault was available and was not needed" instead
  of deleting the line.

ChaCha8 rather than the standard library's `StdRng` for a related reason:
`StdRng`'s algorithm is explicitly not stable across `rand` releases, so a minor
version bump would silently renumber every seed and invalidate every committed
reproducer in every user's repository.

## Invariants

An invariant is something that must be true no matter which ordering happened.
It is what turns "the run finished" into a verdict: without one, a fuzzer that
explored ten thousand orderings has nothing to report.

They come in two kinds and you need both.

### Built in

Zero user input. They encode the semantics of the dependency itself, so a
first-time user gets a caught bug before learning what this tool is.

| Invariant                                | Dependency | What it holds                                                     | Status  |
| ---------------------------------------- | ---------- | ----------------------------------------------------------------- | ------- |
| `max_deliver_respected`                  | nats       | A message is never delivered more than the stream's `max_deliver`. | works   |
| `no_delivery_after_ack`                  | nats       | An acknowledged message is not delivered again.                    | works   |
| `no_infinite_redelivery`                 | nats       | The same payload does not recur past `same_payload_max` within `window`. | works |
| `consumer_filter_excludes_dead_letter`   | nats       | A consumer's filter subject does not match its own dead-letter subject. | planned |
| `no_commit_after_error`                  | postgres   | A connection that reported an error does not then commit.          | works   |
| `no_query_outside_transaction`           | postgres   | No query is issued outside a transaction that claimed one.         | planned |
| `set_local_role_survives_pooler`         | postgres   | `SET LOCAL ROLE` is still in effect for the statements that follow it. | planned |
| `every_command_gets_a_reply`             | redis      | Every command that reached the server got a reply.                 | works   |
| `lock_released_by_owner`                 | redis      | A key taken with `SET NX` is not deleted by a client that no longer holds it. | works |
| `every_request_reaches_terminal_state`   | http       | Every accepted request gets a response or an explicit failure.     | works   |
| `idempotent_retry_returns_same_response` | http       | A retried idempotency key returns the response the first attempt got. | works |
| `eventually_quiescent`                   | any        | The system stops doing work once the workload is done.             | works   |

**A planned entry is listed, not hidden, and it never reads as passing.** The
failure mode of the other choice is the one this whole tool argues against: a
scenario names an invariant, ten thousand seeds pass, and the report says the
service is fine on a question nobody actually asked. `mis check` prints this
table for your scenario and marks what is planned.

There is a second reason to run `mis check` rather than trusting the table.
"Implemented" is a claim about the source, not about every build of it: a
built-in whose protocol feature is compiled out cannot run either, and `check`
distinguishes the two.

### Yours

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

## Traces, shrinking and reproducers

A **trace** is the complete list of every decision a run made, as JSON Lines: a
header, then one line per decision, each naming its fork and the answer it got.
It is small enough to commit to your repository and read in a diff.

The thing to understand about a trace is what is *not* in it. A trace records
decisions, not messages. "The response on connection 2 was held for 40ms" is a
line; the body of that response is not, and neither is any payload the workload
sent. That is a deliberate property rather than an omission: it is what lets
someone in a regulated industry share a reproducer at all, and it is checked
rather than assumed.

**Replay** is re-running a trace instead of a seed: the same decisions at the
same forks, in seconds. It is not a separate execution mode — it is the same run
with a different decision source plugged in, which is what makes it
trustworthy. If replay had its own code path, the thing it reproduced would be
that path and not the original run.

### Shrinking

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
re-runs. The result is *1-minimal*: removing any single remaining decision makes
the failure go away. Not the globally smallest set, which is exponential to find
and not worth it.

Shrinking is not an add-on. A version of this tool that found failures and did
not reduce them would hand you something *less* useful than the incident it
predicted.

### The reproducer

A shrunk trace, committed. This is the unit of work the tool exists to produce:
the artifact you attach to a ticket, hand to a colleague, or run in CI on every
pull request. It is not a flaky test — it is an exact schedule that either
reproduces or does not.

## The vendor corpus

A directory of `<vendor>.toml` files recording what a vendor was actually
observed doing, pointed at with `--corpus <directory>` or `MISORDER_CORPUS`.
Each entry is a named **behaviour flag**, written as a sentence about the
vendor, `no_ack_on_second_replace` rather than `ls_bug_14`, because the name
appears in scenarios, reports and drift alerts.

What makes it a corpus rather than a wiki is that every behaviour carries its
**provenance**, and the three kinds are ranked honestly:

| Provenance   | What it means                                                                 |
| ------------ | ----------------------------------------------------------------------------- |
| `recorded`   | Observed on the wire, carrying the digest of the transcript it came from, so a consumer can verify the entry rather than trust it. The strongest claim, and the only one that cannot be got by reading. |
| `documented` | The vendor said so, in a changelog or a support ticket. Weaker than a recording, and worth having because it dates the change. |
| `reported`   | Someone else hit it, in a GitHub issue or a forum thread. The weakest claim, and often the first sign of a real one. |

The format is versioned (`corpus::FORMAT_VERSION`) because an entry contributed
today should still be readable by a build shipped in two years.

[`examples/corpus/stripe.toml`](examples/corpus/stripe.toml) is the worked
example.

## Behaviour when things go wrong

One rule underlies all of this:

> **A service that broke an invariant is a result. Anything else that went wrong
> is an error.** They never share an exit code, a counter, or a line in a
> report.

Conflating the two is how a harness ends up reporting its own bugs as the
user's, and once a tool has cried wolf about a failure that was never real,
nobody trusts the failures that are.

Exit codes are three-valued for exactly that reason:

| Code | Meaning                                                     |
| ---- | ----------------------------------------------------------- |
| 0    | Every invariant held.                                        |
| 1    | misorder could not run. Not a finding.                       |
| 2    | An invariant was violated. A finding.                        |

Collapsing 1 and 2 means a broken Docker socket looks like a caught bug, someone
chases it for an hour, and the next real finding gets the same treatment.

| Condition | What happens | Exit |
| --- | --- | :-: |
| An invariant is violated | Reported as a finding, with the decisions that caused it, shrunk if asked | 2 |
| The scenario is malformed or self-contradictory | Startup failure naming what is wrong | 1 |
| The scenario is valid but asks for something unbuilt | `not supported yet`, naming the feature. Distinct from a bad scenario: the file is not wrong, the feature is missing | 1 |
| A Postgres dependency is declared | `not supported yet`. The codec is unwritten; the seam and the fault vocabulary are not | 1 |
| A scenario asks misorder to start a container | `not supported yet`. Declare an already-running dependency by `address` instead | 1 |
| The Docker daemon is unreachable | `the Docker daemon did not answer`, naming the underlying error. A scenario declaring no dependencies, or only ones by `address`, never reaches this | 1 |
| The service under test needs a port and has no `listen_env` | Startup failure telling you to add one, rather than binding something arbitrary | 1 |
| The service does not become ready inside `ready_timeout` | Timed out, naming what was waited for and how long | 1 |
| A Redis client sends `SUBSCRIBE` | Refused, naming the command. Pub/Sub breaks the one-reply-per-command pairing the adapter and its invariants rest on, and forwarding it would mis-pair quietly | 1 |
| A dependency sends a frame the adapter cannot decode | Protocol error naming the protocol. From the service under test this is a finding; from the real dependency it is a bug in the adapter | 1 |
| A single seed in a sweep cannot run | Counted as **incomplete**, apart from both passes and failures, with a warning naming the seed | see below |
| A sweep runs against a dependency misorder did not start | Warned at the top of the run: state carries between seeds | 0/2 |
| A scenario names a `planned` built-in | `mis check` marks it. It never reports as holding | 0 |
| A built-in is implemented but its protocol feature is compiled out | `mis check` says so, separately from `planned` | 0 |
| A replay does not follow the trace it was given | Tracked, **not yet reported.** See [Not done](#not-done) | 0/2 |

An incomplete run is never folded into the passes. Presenting a harness failure
as a caught bug is how a tool teaches people to ignore it, and folding it into
the passes is how a sweep claims coverage it never had.

### A sweep against a dependency you started

misorder did not start it, so it is not reset between seeds. Whatever seed 40
wrote is still there for seed 41, and a run's outcome can then depend on a run
before it — which is exactly the property `mis fuzz` exists to rule out. Same
seed, same run stops being true across a sweep even though it still holds for a
single one.

`mis fuzz` warns when it sees one, naming the dependencies. A single `mis run`
is unaffected.

This is not fixed by wiping the thing: that is your Redis, and a harness that
flushed it because a scenario pointed at it would be a worse problem than the
one it solved. Give a sweep an instance of its own, or key everything the
service touches by `MISORDER_SEED`.

### A replay that drifts

Replay reports two kinds of divergence, and the distinction matters because one
of them is normal:

- **Unmatched** forks are ones the run reached that the trace has nothing for.
  They take the neutral choice. During shrinking this is expected — a removed
  fault means a connection survives and reaches forks the original run never got
  to.
- **Unused** decisions are ones in the trace the run never reached. A plain
  replay with unused decisions did not follow the recorded path, and whatever it
  proved is about a different run.

Both are tracked, and today neither is surfaced by `mis replay`. A committed
reproducer that has drifted from its scenario will therefore pass or fail
without saying that it stopped reproducing the recorded schedule. See
[Not done](#not-done).

## CI

The exit code is the whole integration.

```bash
mis fuzz scenario.toml --seeds 5000 --parallel 16
```

Results are also a versioned JSON document, for anything that wants to store,
compare or comment on them:

```bash
mis run scenario.toml --seed 8837291 --format json
mis fuzz scenario.toml --seeds 10000 --report sweep.json
```

`--report-format csv` writes one row per failing seed instead. Both are
described in [`docs/INTERFACES.md`](docs/INTERFACES.md).

Large sweeps split across machines with no coordinator. Each one computes its
own slice from two integers:

```bash
mis fuzz scenario.toml --seeds 100000 --shard 7/64 --report shard-7.json
```

**Sweeps belong on a schedule, not on every pull request.** Five thousand
orderings is minutes, and the point of a sweep is to find something new. What
belongs on every pull request is what the sweep already found:

```bash
mis replay tests/reproducers/reopened-after-cancel.jsonl -s scenario.toml
```

That runs in under a second and either reproduces or does not.

## Docker

```bash
docker run --rm \
  -v "$PWD:/work" \
  ghcr.io/anthid-labs/misorder:latest run scenario.toml
```

Static musl build on Alpine, with `/work` as the working directory. There is no
default `CMD` on purpose: `run` and `fuzz` both start processes and drive
traffic, so neither is a safe thing to do the moment a container starts.

The entrypoint runs as PID 1, where the kernel installs no default signal
handlers, so `--init` is worth passing for anything that needs `docker stop` to
be prompt. [`docker/compose.yaml`](docker/compose.yaml) is the one wired to a
local build for development.

## Layout

| Path                    | Purpose                                                       |
| ----------------------- | ------------------------------------------------------------- |
| `crates/misorder`       | The library: scenario, orchestrator, proxy, schedule, trace, invariants, shrinker. |
| `apps/misorder-cli`     | The `mis` binary: argument parsing, logging setup, exit codes. |
| `apps/demos`            | The services under test: `billing_demo` for the ingress example, `redis_demo` and `nats_demo` for the egress ones. All wrong on purpose. |
| `examples/`             | Scenario files, including the ones in this README, and a corpus. |
| `docker/`               | Dockerfile and a compose example.                              |
| `docs/ARCHITECTURE.md`  | How the components fit, with a flowchart for each.             |
| `docs/INTERFACES.md`    | The stable formats anything built on top reads and writes.     |
| `docs/LEDGER_IMPORT.md` | Reading recorded vendor behaviour out of a webhook events table. Specified, not implemented. |
| `.github/workflows/`    | Lint, test, security scans, image publishing.                  |

The split is along process boundaries. Anything that decides, runs, or checks is
in the library, so it can be embedded and tested without a binary; the CLI is
argument parsing, the global log subscriber, and an exit code. A library that
installed a global subscriber would fight whatever its host application had
already set up, which is why that lives in the binary.

Workspace members are globbed, so a new crate under `apps/` or `crates/` is
picked up without editing the root `Cargo.toml`.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The suite needs no Docker and nothing off the machine. That is possible because
the interesting logic is pure: the shrinker is tested against a synthetic oracle
that fails only when a specific set of decisions survives, so the search is
verified without running a single container, and the scheduler is tested by
asserting that a fork gets the same answer however many other forks came first,
which is the property the whole tool rests on. The adapters bind loopback
sockets and talk to a few dozen lines of fake server, which is enough to test
what they decide.

One more thing CI checks, and it is the one that fails for somebody else rather
than for you:

```bash
for feature in nats postgres redis http; do
  cargo check -p misorder     --no-default-features --features "$feature"
  cargo check -p misorder-cli --no-default-features --features "$feature"
done
```

One feature per protocol, and a downstream embedder may enable exactly one. A
missing `#[cfg]` compiles fine with the default set and breaks only for the
person who turned the others off, which is the worst place to find it.

The CLI is in that loop for a second reason. It forwards each feature to the
engine by hand, and the workspace dependency sets `default-features = false`, so
that list is the whole of what `mis` can speak. A protocol added to the library
and not to the list compiles, tests green, and is unreachable from the command
anybody actually runs. `the_cli_forwards_every_engine_feature` asserts the two
lists match, because a check that only lives in CI is one you find out about
after you have pushed.

## Roadmap

**Phase 1, the loop.** One service, real dependencies, seeded faults, a failure
that reproduces, a minimal reproducer. The loop is closed for HTTP, Redis and
NATS, in both placements, against a service that imports nothing. What is left
is protocol coverage — the Postgres codec — and starting the containers a
scenario declares rather than pointing at ones you already started.

The next adapter is worth naming. **gRPC is the one that pays twice**: it needs
HTTP/2, and HTTP/2 is also what makes `reorder` work properly for the HTTP
adapter, whose current form needs a pipelining client that almost nobody writes.
Kafka is high value and belongs after the virtual clock, because its best
failures (rebalance, session timeout, eviction) fire on a timer with no frame
crossing the wire, which is precisely what a proxy cannot reach.

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

## Not done

- **The Postgres wire codec.** The seam is defined and the fault vocabulary is
  complete; the codec is not written. The other three are: `proxy::http` speaks
  HTTP/1.1, `proxy::redis` speaks RESP, `proxy::nats` speaks the NATS line
  protocol, and all three ask at every fork.
- **Replay divergence is tracked but not reported.** `Replay` records the forks
  a run reached that the trace has nothing for, and the decisions the trace held
  that the run never reached, and answers `is_faithful()`. Nothing outside its
  own tests reads any of that, so `mis replay` cannot yet tell you that a
  committed reproducer stopped reproducing the schedule it recorded — which is
  the one thing a reproducer is for.
- **Container orchestration.** `orchestrator::docker` connects to the daemon and
  reports a clear error; it does not start anything. A scenario that declares no
  dependencies never reaches it, and one that declares an already-running
  dependency by `address` does not either, which is why all three worked
  examples run with no daemon.
- **JetStream `ack_wait` on demand.** `ack_timeout` holds an ack for a fixed
  span and the server's expiry fires on its own wall clock, so the duplicate
  processing race is explored rather than commanded. Nothing crosses the wire
  when that timer fires, so there is no decision for a proxy to intercept: it is
  the case Phase 3's simulated JetStream exists for.
- **JetStream dead-letter advisories.** `consumer_filter_excludes_dead_letter`
  is written and tested, and nothing emits the `DeadLettered` event it fires on,
  so today it can never fire. A dead letter is a JetStream advisory published on
  a subject of its own rather than something crossing a proxied connection, so
  reaching it means subscribing to `$JS.EVENT.ADVISORY.>` alongside the proxy.
  Marked `planned` rather than left reading as working. The dead-letter loop is
  still caught, by `no_infinite_redelivery`.
- **Perturbing a service's own publish.** `fork_kinds("nats")` gives the adapter
  `Connection`, `Deliver` and `Ack`. An ordinary `PUB` on the service's own
  subject is observed and forwarded untouched, so an outbox publish can be seen
  in a report but not delayed or dropped. Widening that is a change to the fork
  vocabulary rather than to the adapter.
- **Egress HTTP from a scenario.** Redis proved the egress placement end to end,
  and the same works for HTTP when the engine is driven as a library. What is
  missing is a way to *declare* an HTTP dependency in a scenario, so there is no
  `mis run` path to it.
- **Redis pub/sub.** After `SUBSCRIBE` the server sends messages no command asked
  for, which breaks the one-reply-per-command pairing the adapter and its
  invariants rest on. Refused with a clear message rather than forwarded and
  quietly mis-paired.
- **TLS, and HTTP/2.** Both adapters are plaintext. The service under test is on
  loopback and a vendor's delivery has already been terminated by the time
  misorder sees it, so neither is in the way yet. HTTP/2 is what would make
  `reorder` useful against a real HTTP client, which today needs pipelining that
  almost nobody does.
- **Quiescence detection.** An idle window, which is a heuristic. Deliberately a
  conservative one: calling quiescence during a 40ms CPU burst would manufacture
  a failure that never happened. It is what gates the virtual clock.
- **Isolation across a sweep** against a dependency misorder did not start. It
  warns rather than fixing it. See
  [Behaviour when things go wrong](#a-sweep-against-a-dependency-you-started).

## Contributing

Contributions are welcome: issues, bug reports, and pull requests alike. Adding
a protocol adapter is the intended path, and the surface is deliberately small:
bind, accept, speak the protocol, ask before branching. Every adapter is open,
because the long tail of vendors is only ever covered by the person who needed
one.

### Getting set up

Rust 1.88 or newer. Edition 2024 puts the floor at 1.85, but `time` 0.3.47 and
later declare 1.88, and the MSRV-aware resolver will not select them under a
lower one. It is stated in `[workspace.package]` rather than inferred, so a
build on an older toolchain fails with the version it needs instead of with an
error inside a dependency, and `cargo clippy` enforces it so the declared
version cannot drift below what the code uses.

```bash
git clone https://github.com/anthid-labs/misorder
cd misorder
cargo test --workspace
```

No Docker needed. The NATS worked example wants a JetStream server, and the
tests do not.

### Before opening a pull request

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

CI runs exactly these, with warnings denied. If a change touches a protocol
feature boundary, also run the per-feature check loop from
[Development](#development).

### What a change is expected to carry

- **A test that would fail without it.** The engine is testable without
  containers on purpose; a change that can only be demonstrated against a live
  dependency usually means the seam is in the wrong place.
- **Comments that say why, not what.** The existing code explains the reasoning
  behind decisions that look arbitrary: why an invariant violation is not an
  error, why `consumer_filter_excludes_dead_letter` is `planned` rather than
  implemented, why the CLI owns the log subscriber. That is the house style, and
  it is the part that survives.
- **Anything that changes the scenario format, a trace, a report, or an exit
  code called out separately** in the pull request description. Those are the
  surfaces other people build on.

### Things to know

[`AGENTS.md`](AGENTS.md) has the conventions in full. The parts that are not
negotiable:

- **Determinism is the product.** An adapter that reads the clock or calls
  `rand` does not fail loudly — it produces traces that replay into a different
  run. Every draw comes from `(seed, kind, connection, ordinal)`.
- **Fork ordinals must be stable across runs.** An adapter that numbers its
  forks differently on a second run replays plausible-looking decisions at the
  wrong places, and nothing reports it.
- **Every decision needs a neutral choice.** Shrinking is defined as replacing a
  decision with its neutral counterpart; a fault with no neutral form cannot be
  shrunk out and will appear in every reproducer that touched it.
- **A harness failure is never a finding.** Exit 1 and exit 2 are different
  things, and so are the `incomplete` and `failed` counters in a sweep.
- **A planned invariant is listed, never silently passed.** An invariant that
  cannot fire must say so.

## What stays open

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

The engine has no network client, no account, no licence check, and no usage
counter, and nothing it emits leaves the machine: the `tracing` events it
produces go to your terminal. The only sockets this process opens are the Docker
daemon, the dependencies it started, and your service. That is checkable by
reading this repository, and it is meant to be.

[`docs/INTERFACES.md`](docs/INTERFACES.md) documents the formats anything built
on top reads and writes, so that anything you want to build around this, in any
language, has a stable surface to build on.

## Support

misorder is free and Apache-2.0, and stays that way. If it saved you some
trouble and you feel like saying thanks,
[buy me a coffee](https://buymeacoffee.com/dallinwright).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Contributions are accepted under the same license, per section 5 of the Apache
License: any contribution intentionally submitted for inclusion is licensed
Apache-2.0, with no additional terms.

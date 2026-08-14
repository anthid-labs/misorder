# Interfaces

This repository is the open core: `github.com/misorder/misorder`, public,
Apache-2.0. The hosted product lives in `github.com/misorder/platform`, private,
in the same organisation.

This document is the contract between them. It exists so the engine can be
refactored freely without breaking the platform, and so the platform can be
written in whatever language suits it.

## The rule

**Coupling is through file formats and process boundaries. Never through a Rust
API.**

The platform does not depend on the `misorder` crate. It reads documents the
engine writes, writes documents the engine reads, and runs `mis` as a child
process. Nothing else.

This is not fastidiousness. Three things follow from it, and each of them is
load-bearing:

- **The engine stays refactorable.** Every internal type, trait and module in
  this repository is free to change in any release. If the platform linked the
  crate, the open core's internals would become a compatibility surface, and an
  open core that cannot be refactored stops being developed.
- **The platform stays unblocked.** It can be Python, Go, TypeScript, or
  anything else, because parsing JSON is not a language commitment. More than
  one language is expected to sit on this seam, and a Rust API would have made
  that choice for all of them.
- **The boundary stays auditable.** A buyer in a regulated segment can read this
  repository and see the complete list of what leaves the machine, because the
  engine has no network client, no credentials and nothing to send anywhere.
  That claim survives only if it stays literally true.

## The five interfaces

Everything the platform needs is one of these. Each is versioned independently
of the binary.

| Interface           | Direction | Format          | Version constant                  |
| ------------------- | --------- | --------------- | --------------------------------- |
| Scenario            | in        | TOML            | (implicit; additive only)         |
| Corpus              | in        | TOML            | `corpus::FORMAT_VERSION`          |
| Trace               | both      | JSON Lines      | `trace::FORMAT_VERSION`           |
| Run / sweep report  | out       | JSON            | `report::run::FORMAT_VERSION`     |
| CLI and exit codes  | both      | argv, exit code | the `mis` version                 |

### Versioning rules

- **Adding a field does not bump the version.** Consumers must ignore fields
  they do not recognise. A report is read by things on their own release cycle,
  and requiring them to upgrade in step with the engine would make every engine
  release a coordinated one.
- **Removing or repurposing a field bumps it.**
- **Readers refuse a version from the future** rather than guessing. A trace or
  a corpus entry from a newer build is an error, not a best effort.
- **The meaning of a seed is frozen.** Changing how a decision is derived from
  `(seed, fork)` invalidates every committed reproducer in every user's
  repository. It is a breaking change to the whole product, not to a format.

### Scenario, in

`misorder.example.toml` documents every key. The platform generates these when
it turns a recorded session into a scenario. Treat the format as something a
generator emits: no positional meaning, no shorthand that only reads well by
hand, every optional key genuinely optional.

### Corpus, in

A directory of `<vendor>.toml` files. See `examples/corpus/` for the shape and
`crates/misorder/src/corpus/` for the parser.

The engine reads a corpus from a **local directory**, always. A hosted corpus
delivers files in this format and the user points `--corpus` at the result. The
engine gets no registry client, because the moment it has one it has an
outbound connection to explain in every security review.

### Trace, in and out

JSON Lines, one header and one line per decision. A shrunk trace is a committed
reproducer: it runs in CI in seconds and either reproduces or does not.

### Run and sweep report, out

`mis run --format json` and `mis fuzz --report <path>`. This is the ingestion
seam for everything hosted. It carries the verdict, the violations, the failure
signature, what was permitted versus what was used, the scenario digest, and
which engine build produced it.

### CLI and exit codes

| Code | Meaning                               |
| ---- | ------------------------------------- |
| 0    | Every invariant held.                  |
| 1    | The engine could not run. Not a finding. |
| 2    | An invariant was violated. A finding.  |

An orchestrator that treats 1 as 2 reports a broken Docker socket as a caught
bug. Keep them distinct in anything that consumes them.

## Where each hosted feature attaches

### 1. Vendor corpus and drift detection

The business. Curated, verified transcripts and behaviour flags, plus continuous
diffing of production traffic against them.

- **Attaches to:** the corpus format, in.
- **The engine provides:** `corpus::FORMAT_VERSION`, the `<vendor>.toml` schema,
  provenance with a transcript digest so an entry is verifiable rather than
  asserted, and refusal to run a scenario naming a behaviour the corpus lacks.
- **The engine must never provide:** a registry client, an account, or a fetch.
  Delivery is the platform's job and its output is a directory.
- **Not yet specified here:** the transcript *body* format. Recording sessions
  is Phase 2, and inventing a frame encoding before there is a recorder to
  validate it would fix the wrong shape into a compatibility surface.
  Behaviours reference a transcript by id and digest, which is what scenarios
  and drift detection need today.

The scrubber, when it lands, is **open source and in this repository**. That is
not a giveaway, it is the condition of sale: silent collection of anything
resembling production traffic is a compliance incident rather than a PR problem,
and the mechanism has to be exchange rather than extraction.

### 2. Compliance artifacts

Signed conformance reports, evidence bundles, coverage attestation.

- **Attaches to:** the sweep report, out.
- **The engine provides:** everything an auditor needs to be told, as facts:
  which scenario by content digest, which engine build, which seeds were asked
  for, which actually ran, how many passed, how many could not complete. A
  sweep that did not cover what it was asked to says so through
  `is_complete()`, because "10,000 seeds passed" when 4,000 never started is a
  true sentence that misleads.
- **The engine must never provide:** a signature, a key, or a claim about
  itself. The engine states facts; the platform attests to them. A binary that
  signed its own output would be attesting that it is trustworthy, which is not
  a thing a binary can do.

### 3. Team surface

Cross-run triage, dedup, historical trends, shared reproducer library, PR bot.

- **Attaches to:** the run and sweep reports, out.
- **The engine provides:** `Trace::signature()`, a stable identity for the
  *shape* of a failure. Two runs that found the same bug agree; two that found
  different bugs do not. Also within-sweep grouping, so a local `mis fuzz`
  reports "10 failing seeds, 2 distinct failures" without an account.
- **The engine must never provide:** history. Grouping needs no state and stays
  here; tracking when a signature first appeared, which pull request introduced
  it, and what a team has already triaged all need a database, and a stateless
  single-machine CLI should not grow one.

The line is **stateless and local stays open; persistent and shared is hosted.**
It is a real line, not a crippled tier: nobody resents a free CLI for not
containing a database.

### 4. Distributed seed search

Orchestration for large sweeps, BYOC.

- **Attaches to:** `mis fuzz --shard i/N --report <path>`.
- **The engine provides:** shard selection by `seed % count == index`, so a
  machine computes its own slice from two integers and coordinates with nobody,
  and a report that states which slice it ran. Modulo rather than contiguous
  ranges, so no worker draws an all-quiet block while another runs for an hour.
- **The engine must never provide:** the fan-out, the merge, or the queue.

Note this is the weakest of the four. The harness is open source and anyone runs
100k seeds on spot for pocket change, so the orchestration is a convenience
rather than a moat.

## Pricing follows the architecture

Per service under test, or per integration monitored. **Not per compute.**

This has a technical consequence, so it belongs in this document rather than
only in a pricing page: **nothing in the engine meters, counts, or reports
usage.** No seat check, no license file, no run counter, no phone home. A
seed-hour meter makes someone cap the nightly sweep at 10k, the bug that needed
seed 71830 never surfaces, and they churn saying it never found anything.

## What stays open, permanently

The runner, the scenario format, the proxy layer, **every adapter**, the
decision recorder, the seeded scheduler, the built-in invariants, **trace
shrinking**, local fuzzing, any simulated dependency, **the virtual clock**, the
scrubber, and the transcript format.

Three of those are worth naming as specifically not moveable:

- **Adapters.** The long tail of vendors is only ever covered by people who
  needed one. A licence boundary there ends the contributions that are the only
  way it gets covered.
- **Shrinking.** Withholding it means the free tier produces failures *less*
  useful than the incident they predicted: an 847-line trace nobody can act on.
  The six lines are what makes someone adopt the tool.
- **The virtual clock.** An open source tool that is slow teaches everyone that
  the tool is slow.

## Before adding anything to this repository

Ask whether it needs state that outlives one run, a second machine, or a
network. If it does, it belongs in the platform, and what belongs here is the
document that carries the information across.

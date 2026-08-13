# misorder-cli

The `mis` command: run your service against its real dependencies under
thousands of orderings, and shrink whatever breaks to a minimal reproducer.

```bash
cargo install misorder-cli
```

The package is `misorder-cli`; the command it installs is `mis`.

```bash
mis check scenario.toml                    # validate, print what it will actually check
mis run scenario.toml --seed 8837291       # one ordering
mis fuzz scenario.toml --seeds 10000 --parallel 16
mis replay trace-8837291.jsonl             # re-run a recorded one
mis shrink trace-8837291.jsonl -o repro.jsonl
```

Exit codes are three-valued, because CI needs to tell a caught bug from a broken
harness:

| Code | Meaning                                |
| ---- | -------------------------------------- |
| 0    | Every invariant held.                   |
| 1    | misorder could not run. Not a finding.  |
| 2    | An invariant was violated. A finding.   |

The engine is [`misorder`](https://crates.io/crates/misorder). This crate is
argument parsing, the log subscriber, and the exit code.

See the [repository](https://github.com/misorder/misorder) for the scenario
format and the roadmap.

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

//! End to end tests for the `mis` binary.
//!
//! These run the real command as a child process, so they cover argument
//! parsing, the exit codes, and what actually reaches stdout. Everything here
//! is hermetic: no Docker, no network. The commands that need a container are
//! covered by asserting they fail as a *harness* error rather than as a
//! finding, which is the distinction CI depends on.

use std::path::Path;
use std::process::{Command, Output};

const MIS: &str = env!("CARGO_BIN_EXE_mis");

const SCENARIO: &str = r#"
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

[[workload]]
publish = "ledger.org.org_1.account.acct_1.order"
payload = { order_id = "ord_1", kind = "fill", qty = 100 }

[faults]
enabled = ["ack_timeout", "redelivery", "connection_drop", "reorder"]

[[invariants]]
builtin = "no_infinite_redelivery"
window = "5m"
same_payload_max = 10
"#;

fn write_scenario(directory: &Path, body: &str) -> std::path::PathBuf {
    let path = directory.join("scenario.toml");
    std::fs::write(&path, body).expect("write scenario");

    path
}

fn mis(args: &[&str]) -> Output {
    Command::new(MIS)
        .args(args)
        .output()
        .expect("the mis binary runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn check_reports_what_a_scenario_resolves_to() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), SCENARIO);

    let output = mis(&["check", path.to_str().expect("utf-8")]);

    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);

    assert!(text.contains("dead_letter_no_redelivery"), "{text}");
    assert!(text.contains("no_infinite_redelivery"), "{text}");
    assert!(text.contains("nats"), "{text}");
    assert!(text.contains("ack_timeout"), "{text}");
}

#[test]
fn check_says_when_a_scenario_permits_no_faults() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(
        directory.path(),
        r#"
name = "baseline"

[[system]]
run = "./service"

[[invariants]]
builtin = "eventually_quiescent"
"#,
    );

    let output = mis(&["check", path.to_str().expect("utf-8")]);
    let text = stdout(&output);

    assert!(
        text.contains("cannot perturb anything"),
        "a silently fault-free scenario is the failure mode this warning exists for: {text}"
    );
}

#[test]
fn check_marks_an_invariant_that_is_specified_but_not_implemented() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(
        directory.path(),
        r#"
name = "planned"

[[system]]
run = "./service"

[deps.postgres]

[[invariants]]
builtin = "no_query_outside_transaction"
"#,
    );

    let output = mis(&["check", path.to_str().expect("utf-8")]);

    assert!(
        stdout(&output).contains("NOT IMPLEMENTED YET"),
        "{}",
        stdout(&output)
    );
}

#[test]
fn a_malformed_scenario_exits_as_a_harness_error_not_a_finding() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), "name = \"broken\"\nnope = 1\n");

    let output = mis(&["check", path.to_str().expect("utf-8")]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a bad scenario is exit 1, never exit 2: {}",
        stderr(&output)
    );
    assert!(stderr(&output).starts_with("mis: "), "{}", stderr(&output));
}

#[test]
fn a_missing_scenario_says_where_it_looked() {
    let output = mis(&["check", "/nonexistent/scenario.toml"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("/nonexistent/scenario.toml"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn a_run_that_cannot_start_its_dependencies_exits_one_not_two() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), SCENARIO);

    let output = mis(&["run", path.to_str().expect("utf-8"), "--seed", "8837291"]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "an environment that will not start is not a caught bug: {}",
        stderr(&output)
    );
}

#[test]
fn replaying_a_malformed_trace_reports_the_line() {
    let directory = tempfile::tempdir().expect("tempdir");
    let scenario = write_scenario(directory.path(), SCENARIO);
    let trace = directory.path().join("trace.jsonl");

    std::fs::write(&trace, "not json\n").expect("write trace");

    let output = mis(&[
        "replay",
        trace.to_str().expect("utf-8"),
        "--scenario",
        scenario.to_str().expect("utf-8"),
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("trace.jsonl:1"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn version_and_help_work_without_a_scenario() {
    assert!(mis(&["--version"]).status.success());
    assert!(mis(&["--help"]).status.success());
    assert!(mis(&["fuzz", "--help"]).status.success());
}

#[test]
fn a_bad_shard_is_refused_before_anything_starts() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), SCENARIO);

    let output = mis(&[
        "fuzz",
        path.to_str().expect("utf-8"),
        "--shard",
        "64/64",
        "--seeds",
        "4",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("zero-indexed") || stderr(&output).contains("does not exist"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn a_sweep_report_states_the_shard_and_the_coverage_hole() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), SCENARIO);
    let report = directory.path().join("sweep.json");

    // No Docker here, so every run is incomplete. That is the case worth
    // pinning: a sweep that covered nothing must not read as a clean pass.
    let output = mis(&[
        "fuzz",
        path.to_str().expect("utf-8"),
        "--seeds",
        "8",
        "--shard",
        "1/4",
        "--report",
        report.to_str().expect("utf-8"),
    ]);

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));

    let body = std::fs::read_to_string(&report).expect("report written");
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");

    assert_eq!(parsed["shard"]["index"], 1);
    assert_eq!(parsed["shard"]["count"], 4);
    assert_eq!(parsed["seed_count"], 8);
    assert_eq!(parsed["seeds_run"], 2, "one seed in four, twice over");
    assert_eq!(parsed["incomplete"], 2);
    assert_eq!(parsed["passed"], 0);
    assert!(
        parsed["engine"]["version"].is_string(),
        "a result outlives the binary, so it has to say which one made it"
    );
    assert!(
        parsed["scenario"]["digest"].is_string(),
        "and which scenario it attests to"
    );
}

#[test]
fn a_sweep_report_streams_to_stdout_with_the_logs_out_of_the_way() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(directory.path(), SCENARIO);

    let output = Command::new(MIS)
        .args([
            "fuzz",
            path.to_str().expect("utf-8"),
            "--seeds",
            "2",
            "--report",
            "-",
        ])
        .env("LOG_LEVEL", "debug")
        .output()
        .expect("the mis binary runs");

    serde_json::from_str::<serde_json::Value>(&stdout(&output))
        .expect("stdout is the document and nothing else");
}

#[test]
fn a_scenario_naming_a_vendor_needs_a_corpus() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_scenario(
        directory.path(),
        r#"
name = "vendor"

[[system]]
run = "./service"

[vendors.lightspeed]
behaviors = ["no_ack_on_second_replace"]

[[invariants]]
builtin = "eventually_quiescent"
"#,
    );

    let output = mis(&["check", path.to_str().expect("utf-8")]);

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("--corpus"), "{}", stderr(&output));
}

#[test]
fn a_behaviour_the_corpus_does_not_have_is_refused_and_lists_what_it_does() {
    let directory = tempfile::tempdir().expect("tempdir");
    let corpus = directory.path().join("corpus");
    std::fs::create_dir(&corpus).expect("mkdir");
    std::fs::write(
        corpus.join("lightspeed.toml"),
        r#"
vendor = "lightspeed"

[[behaviors]]
name = "no_ack_on_second_replace"
protocol = "fix"
describe = "A second replace gets no execution report."
provenance = { kind = "documented", url = "https://example.invalid" }
"#,
    )
    .expect("write corpus");

    let path = write_scenario(
        directory.path(),
        r#"
name = "vendor"

[[system]]
run = "./service"

[vendors.lightspeed]
behaviors = ["a_behaviour_nobody_recorded"]

[[invariants]]
builtin = "eventually_quiescent"
"#,
    );

    let output = mis(&[
        "check",
        path.to_str().expect("utf-8"),
        "--corpus",
        corpus.to_str().expect("utf-8"),
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("no_ack_on_second_replace"),
        "the message should say what the vendor does have: {}",
        stderr(&output)
    );
}

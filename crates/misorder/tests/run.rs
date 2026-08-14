//! One scenario, run for real, end to end.
//!
//! Everything else in the suite tests a stage. This tests the loop: ports
//! reserved, the proxy bound, a real service process started and waited for,
//! the workload posted through the proxy, quiescence, invariants, teardown.
//!
//! Hermetic all the same. The only sockets are loopback, the only process is
//! this test binary, and nothing here needs Docker. That is what a scenario
//! declaring no dependencies buys.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

use misorder::event::{Event, HttpEvent};
use misorder::runner::{Run, Runner};
use misorder::scenario::file::Scenario;

/// The variable the fixture below reads its port from.
///
/// Deliberately not `PORT`. This fixture lives inside the ordinary test binary,
/// so a name anyone else might have exported would turn a normal `cargo test`
/// into a test run that serves HTTP forever.
const FIXTURE_PORT: &str = "MISORDER_FIXTURE_PORT";

/// Not a test.
///
/// An integration test can only be sure of spawning one executable, its own,
/// so the service under test is this binary re-invoked by name. Without the
/// variable set it returns immediately, which is what happens on every normal
/// run of the suite.
#[test]
fn fixture_service() {
    let Ok(port) = std::env::var(FIXTURE_PORT) else {
        return;
    };

    let listener = TcpListener::bind(("127.0.0.1", port.parse::<u16>().expect("a port")))
        .expect("the fixture binds the port it was given");

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };

        std::thread::spawn(move || answer(stream));
    }
}

/// Answers every request on one connection, in the order it arrives.
fn answer(stream: TcpStream) {
    let mut write = stream.try_clone().expect("clone");
    let mut read = BufReader::new(stream);

    loop {
        let mut length = 0usize;
        let mut lines = 0;

        loop {
            let mut line = String::new();

            match read.read_line(&mut line) {
                Ok(0) => return,
                Ok(_) => {}
                Err(_) => return,
            }

            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().unwrap_or(0);
            }

            lines += 1;

            if line == "\r\n" || line == "\n" {
                break;
            }

            if lines > 64 {
                return;
            }
        }

        let mut body = vec![0u8; length];

        if read.read_exact(&mut body).is_err() {
            return;
        }

        if write
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .is_err()
        {
            return;
        }
    }
}

/// A scenario whose service is the fixture above.
fn scenario(posts: usize, faults: &[&str]) -> misorder::scenario::file::Resolved {
    let fixture = std::env::current_exe().expect("this test binary");

    let mut text = format!(
        r#"
name = "end_to_end"

[[system]]
run = "{} --exact fixture_service --nocapture"
listen_env = "{FIXTURE_PORT}"

[faults]
enabled = [{}]

[[invariants]]
builtin = "every_request_reaches_terminal_state"

[[invariants]]
builtin = "eventually_quiescent"

[run]
timeout = "30s"
ready_timeout = "10s"
quiesce_after = "100ms"
"#,
        fixture.display(),
        faults
            .iter()
            .map(|fault| format!("\"{fault}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );

    for index in 0..posts {
        text.push_str(&format!(
            "\n[[workload]]\npost = \"/webhooks/stripe\"\npayload = {{ id = \"evt_{index}\" }}\n"
        ));
    }

    Scenario::parse(&text)
        .expect("the scenario parses")
        .resolve()
        .expect("the scenario resolves")
}

fn requests(outcome: &misorder::runner::Outcome) -> usize {
    outcome
        .events
        .iter()
        .filter(|observed| matches!(observed.event, Event::Http(HttpEvent::Request { .. })))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scenario_with_no_dependencies_runs_without_docker() {
    // No faults: the honest baseline. Every post should arrive, and if this
    // fails the bug was never about ordering.
    let outcome = Runner::new(scenario(3, &[]))
        .execute(Run::Seed(1))
        .await
        .expect("the run completes");

    assert!(
        outcome.passed(),
        "nothing here should violate anything: {:?}",
        outcome.violations
    );

    assert_eq!(
        requests(&outcome),
        3,
        "every post should have reached the service"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delivery_the_schedule_cut_off_is_not_a_harness_failure() {
    // `connection_drop` at the front door closes the connection the driver is
    // still writing to, which is the fault working. Reporting the resulting
    // broken pipe as an error would give exit code 1 for a run that did exactly
    // what the scenario permitted, and a harness that cries wolf about its own
    // faults is one nobody reads the output of.
    for seed in 0..12 {
        Runner::new(scenario(6, &["connection_drop"]))
            .execute(Run::Seed(seed))
            .await
            .unwrap_or_else(|error| panic!("seed {seed} should have completed: {error}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_seed_produces_the_same_trace() {
    let first = Runner::new(scenario(4, &["delay", "reorder", "connection_drop"]))
        .execute(Run::Seed(8_837_291))
        .await
        .expect("first run");

    let second = Runner::new(scenario(4, &["delay", "reorder", "connection_drop"]))
        .execute(Run::Seed(8_837_291))
        .await
        .expect("second run");

    assert_eq!(
        first.trace.records.len(),
        second.trace.records.len(),
        "the same seed must reach the same forks"
    );

    for (first, second) in first.trace.records.iter().zip(&second.trace.records) {
        assert_eq!(
            (first.point.key, first.decision),
            (second.point.key, second.decision),
            "the same fork must get the same answer"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schedule_actually_reaches_the_forks_the_adapter_opens() {
    let outcome = Runner::new(scenario(6, &["delay", "reorder", "connection_drop"]))
        .execute(Run::Seed(44_122))
        .await
        .expect("the run completes");

    let kinds: Vec<_> = outcome
        .trace
        .records
        .iter()
        .map(|record| record.point.key.kind)
        .collect();

    assert!(
        kinds.contains(&misorder::trace::PointKind::Connection),
        "the accept fork should be reached"
    );
    assert!(
        kinds.contains(&misorder::trace::PointKind::Deliver),
        "every request should reach a fork"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_that_will_not_start_is_an_error_and_not_a_finding() {
    let text = r#"
name = "no_such_service"

[[system]]
run = "./no-such-binary-anywhere"

[[workload]]
post = "/webhooks/stripe"

[[invariants]]
builtin = "eventually_quiescent"

[run]
ready_timeout = "1s"
"#;

    let scenario = Scenario::parse(text)
        .expect("parses")
        .resolve()
        .expect("resolves");

    let error = Runner::new(scenario)
        .execute(Run::Seed(1))
        .await
        .expect_err("the service cannot start");

    // Exit code 1, not 2. A harness that reported this as a caught bug would
    // teach someone to ignore the next real one.
    assert!(
        matches!(error, misorder::error::Error::Environment(_)),
        "got {error:?}"
    );
}

#[test]
fn the_fixture_is_a_no_op_without_its_variable() {
    // Guards the one way this file could wedge the suite: if the fixture ever
    // started serving without being asked, every `cargo test` would hang here.
    assert!(std::env::var(FIXTURE_PORT).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_leaves_nothing_behind_for_the_next_one() {
    // Two runs of one scenario, back to back. A service the first run failed to
    // kill would still be holding a port, and a proxy it failed to join would
    // still be serving one, so the second run is the assertion.
    for seed in [7, 8] {
        let outcome = Runner::new(scenario(2, &["delay", "reorder"]))
            .execute(Run::Seed(seed))
            .await
            .unwrap_or_else(|error| panic!("run at seed {seed}: {error}"));

        assert!(outcome.passed(), "seed {seed}: {:?}", outcome.violations);
    }
}

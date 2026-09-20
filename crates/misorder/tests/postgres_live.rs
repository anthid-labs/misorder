// Only in a build with the adapter, because the whole file drives it. The
// dev-dependency on `tokio-postgres` is unconditional, but
// `misorder::proxy::postgres` is not, and without this the crate fails to
// build its tests under any feature set that leaves the adapter out - which is
// the set an embedder taking one protocol uses.
#![cfg(feature = "postgres")]
//! The Postgres loop, against a real server in a real container.
//!
//! Every test here is `#[ignore]`, so `cargo test --workspace` stays hermetic:
//! it needs no daemon and nothing off the machine, and a suite that quietly
//! required Docker would make the first five minutes of this repository a
//! Docker troubleshooting session.
//!
//! ```bash
//! cargo test -p misorder --test postgres_live -- --ignored
//! ```
//!
//! `#[ignore]` rather than a skip the tests decide for themselves, because a
//! skipped test still reports `ok` and a suite that says it passed for having
//! run nothing is how a gap survives a green CI. Ignored tests are counted
//! separately and say so in the summary.
//!
//! What this proves that the unit tests cannot, and the reason it exists
//! alongside them rather than instead of them:
//!
//! - A real `tokio-postgres` client, speaking the real extended query
//!   protocol, reaches a real Postgres through the adapter and does not notice
//!   it is there. The unit tests drive hand-written frames, so they would
//!   still pass against a codec a real client refused to talk to.
//! - A real `40001` from a real serializable conflict, with the real command
//!   tag Postgres answers a doomed `COMMIT` with. Nothing in a fake server
//!   argues that Postgres actually behaves this way, and that behaviour is the
//!   whole premise of `no_commit_after_error`.
//! - The container path end to end: pulled, started, waited for, migrated,
//!   proxied, and removed.
//!
//! What it deliberately does not prove is what the schedule does. Asserting
//! that the server saw the second statement first needs a server whose order
//! of arrival a test can read back, which is exactly what the in-process fake
//! is for.

use std::path::PathBuf;
use std::time::Duration;

use misorder::error::Result;
use misorder::event::{Event, Observed, PostgresEvent};
use misorder::invariant::Invariant;
use misorder::invariant::builtin::postgres::NoCommitAfterError;
use misorder::orchestrator::Environment;
use misorder::proxy::postgres::PostgresAdapter;
use misorder::proxy::{Adapter, EventSink, ProxyContext};
use misorder::scenario::file::{Deps, Postgres, RunSettings};
use misorder::schedule::{Profile, Scheduler};
use tokio_util::sync::CancellationToken;

/// Everything one test needs: a running server, a proxy in front of it, and
/// somewhere the events land.
struct Live {
    environment: Environment,
    url: String,
    events: tokio::sync::mpsc::UnboundedReceiver<Observed>,
    cancel: CancellationToken,
    serving: tokio::task::JoinHandle<Result<()>>,
    container: String,
}

impl Live {
    async fn start(migrations: Option<PathBuf>) -> Self {
        let postgres = Postgres {
            database: "ledger".to_string(),
            migrations,
            ..Postgres::default()
        };

        let deps = Deps {
            postgres: Some(postgres.clone()),
            ..Deps::default()
        };

        let settings = RunSettings::default();

        let environment = Environment::start(&deps, &settings)
            .await
            .expect("a postgres container starts");

        let container = environment
            .dependencies()
            .first()
            .expect("one dependency")
            .container_id
            .clone();

        let (events, receiver) = EventSink::new();

        environment
            .apply_topology(&deps, &events, Duration::ZERO)
            .await
            .expect("migrations apply");

        let upstream = environment
            .address_of("postgres")
            .expect("a started container has an address")
            .to_string();

        let mut adapter =
            PostgresAdapter::new(&postgres.database, &postgres.user, &postgres.password);

        let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

        let url = endpoint
            .env
            .iter()
            .find(|(key, _)| key == "DATABASE_URL")
            .map(|(_, value)| value.clone())
            .expect("the adapter injects a URL");

        let cancel = CancellationToken::new();

        // No faults permitted, so every fork takes the neutral path. This test
        // is about whether a real client can hold a conversation through the
        // adapter at all; what the schedule does to that conversation is the
        // unit tests' job.
        let scheduler = Scheduler::seeded(1, Vec::new(), Profile::default(), "postgres_live");
        let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

        let serving = tokio::spawn(async move { adapter.serve(context).await });

        Self {
            environment,
            url,
            events: receiver,
            cancel,
            serving,
            container,
        }
    }

    /// A real client, connected through the proxy.
    async fn client(&self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(&self.url, tokio_postgres::NoTls)
            .await
            .expect("a real client reaches the server through the proxy");

        tokio::spawn(connection);

        client
    }

    /// Stops the proxy and hands back everything it saw.
    async fn events(&mut self) -> Vec<Observed> {
        self.cancel.cancel();

        (&mut self.serving)
            .await
            .expect("the adapter task")
            .expect("the adapter ends cleanly");

        let mut collected = Vec::new();

        while let Ok(observed) = self.events.try_recv() {
            collected.push(observed);
        }

        collected
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.environment.stop().await;
    }
}

fn postgres_events(observed: &[Observed]) -> Vec<&PostgresEvent> {
    observed
        .iter()
        .filter_map(|observed| match &observed.event {
            Event::Postgres(event) => Some(event),
            _ => None,
        })
        .collect()
}

/// Whether the daemon still has a container.
///
/// Through the CLI rather than through `bollard`, because a test crate sees
/// this library's public API and its dev-dependencies, and the daemon client
/// is neither. It is also what somebody checking by hand would type.
fn container_exists(id: &str) -> bool {
    std::process::Command::new("docker")
        .args(["inspect", "--format", "{{.Id}}", id])
        .output()
        .is_ok_and(|output| output.status.success())
}

#[tokio::test]
#[ignore = "starts a real Postgres container; run with --ignored"]
async fn a_real_client_reaches_a_real_server_through_the_adapter() {
    let directory = tempfile::tempdir().expect("a temporary directory");

    std::fs::write(
        directory.path().join("001_ledger.sql"),
        "CREATE TABLE ledger (id INT PRIMARY KEY, n INT NOT NULL);\n\
         INSERT INTO ledger VALUES (1, 0);\n",
    )
    .expect("write a migration");

    let mut live = Live::start(Some(directory.path().to_path_buf())).await;

    let client = live.client().await;

    client
        .batch_execute("BEGIN")
        .await
        .expect("a transaction starts");

    client
        .execute("UPDATE ledger SET n = n + 1 WHERE id = $1", &[&1i32])
        .await
        .expect("a prepared statement runs");

    client.batch_execute("COMMIT").await.expect("it commits");

    // Read back through the same proxy, so the assertion is about what the
    // server ended up with rather than about what the client believes.
    let row = client
        .query_one("SELECT n FROM ledger WHERE id = $1", &[&1i32])
        .await
        .expect("the row is there");

    assert_eq!(
        row.get::<_, i32>(0),
        1,
        "the migration and the update both ran"
    );

    drop(client);

    let observed = live.events().await;
    let events = postgres_events(&observed);

    assert!(
        events.contains(&&PostgresEvent::Connected {
            database: "ledger".to_string()
        }),
        "the session says which database it is for: {events:?}"
    );
    assert!(events.contains(&&PostgresEvent::Begin), "{events:?}");
    assert!(events.contains(&&PostgresEvent::Commit), "{events:?}");
    assert!(
        events.iter().any(|event| matches!(
            event,
            PostgresEvent::Statement { sql } if sql.contains("UPDATE ledger")
        )),
        "a prepared statement reports the text it parsed: {events:?}"
    );

    live.stop().await;
}

/// The premise of `no_commit_after_error`, against the server that has to be
/// behaving this way for the invariant to mean anything.
///
/// Two serializable transactions touching one row. The second is blocked until
/// the first commits, then fails with `40001`. The service under test is
/// written here as the buggy one: it swallows the error and commits anyway,
/// which Postgres turns into a rollback while telling the client almost
/// nothing. The write the service believes it made did not happen.
#[tokio::test]
#[ignore = "starts a real Postgres container; run with --ignored"]
async fn a_real_serialization_failure_makes_no_commit_after_error_fire() {
    let directory = tempfile::tempdir().expect("a temporary directory");

    std::fs::write(
        directory.path().join("001_ledger.sql"),
        "CREATE TABLE ledger (id INT PRIMARY KEY, n INT NOT NULL);\n\
         INSERT INTO ledger VALUES (1, 0);\n",
    )
    .expect("write a migration");

    let mut live = Live::start(Some(directory.path().to_path_buf())).await;

    let first = live.client().await;
    let second = live.client().await;

    for client in [&first, &second] {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .expect("a serializable transaction starts");

        // Both read the row, which is what gives the server a predicate to
        // find a conflict in. Two blind writes would serialize by locking
        // rather than fail.
        client
            .query_one("SELECT n FROM ledger WHERE id = 1", &[])
            .await
            .expect("both read the row");
    }

    first
        .batch_execute("UPDATE ledger SET n = n + 1 WHERE id = 1")
        .await
        .expect("the first writer wins");

    first.batch_execute("COMMIT").await.expect("and commits");

    let conflict = second
        .batch_execute("UPDATE ledger SET n = n + 1 WHERE id = 1")
        .await
        .expect_err("the second writer cannot serialize");

    assert_eq!(
        conflict.code().map(tokio_postgres::error::SqlState::code),
        Some("40001"),
        "a real serialization failure, not a lock timeout: {conflict}"
    );

    // The bug. A service that treated the error as retryable and carried on.
    second
        .batch_execute("COMMIT")
        .await
        .expect("Postgres accepts the COMMIT and answers ROLLBACK");

    drop(first);
    drop(second);

    let observed = live.events().await;

    let mut check = NoCommitAfterError::default();
    let violation = observed
        .iter()
        .find_map(|observed| check.observe(observed))
        .expect("the invariant fires on what the adapter saw");

    assert!(
        violation.detail.contains("40001"),
        "the reproducer names the SQLSTATE: {violation}"
    );

    live.stop().await;
}

/// A run that leaked its Postgres makes the next run fail too, and the second
/// failure is the one that gets reported.
#[tokio::test]
#[ignore = "starts a real Postgres container; run with --ignored"]
async fn the_container_a_run_started_is_removed_when_the_run_ends() {
    let live = Live::start(None).await;
    let container = live.container.clone();

    assert!(
        container_exists(&container),
        "the run started a container of its own"
    );

    live.stop().await;

    assert!(
        !container_exists(&container),
        "and removed it: {container} is still there"
    );
}

/// A declared Postgres that is already running needs no daemon, which is the
/// case somebody with `docker compose up postgres` is in.
///
/// Runs under the same switch because it still wants a server, and the cheapest
/// way to have one here is the container the other tests use.
#[tokio::test]
#[ignore = "starts a real Postgres container; run with --ignored"]
async fn an_address_is_proxied_without_starting_anything() {
    let started = Live::start(None).await;
    let address = started
        .environment
        .address_of("postgres")
        .expect("an address")
        .to_string();

    let deps = Deps {
        postgres: Some(Postgres {
            address: Some(address.clone()),
            database: "ledger".to_string(),
            ..Postgres::default()
        }),
        ..Deps::default()
    };

    let external = Environment::start(&deps, &RunSettings::default())
        .await
        .expect("an already-running server needs nothing started");

    assert_eq!(external.address_of("postgres"), Some(address.as_str()));
    assert_eq!(
        external
            .dependencies()
            .first()
            .expect("one dependency")
            .container_id,
        "external",
        "nothing was started, so there is nothing to stop"
    );

    // Not ours to stop, and this must leave the container the other half of
    // this test started alone.
    external.stop().await;

    assert!(
        container_exists(&started.container),
        "stopping an external environment must not touch somebody else's container"
    );

    started.stop().await;
}

/// The whole loop, through the runner rather than around it.
///
/// The live tests above bind the adapter themselves, which leaves the run's own
/// wiring unproven: that a container misorder started is the thing a proxy gets
/// put in front of, and that the service is handed the proxy's address rather
/// than the server's. Getting that wrong produces a run that passes every
/// invariant while the service talks straight to Postgres, which is the failure
/// mode the whole egress placement exists to prevent.
///
/// The service under test is a shell that writes down what it was told. That is
/// enough: the question here is what reaches its environment.
#[tokio::test]
#[ignore = "starts a real Postgres container; run with --ignored"]
async fn the_runner_points_the_service_at_the_proxy_and_not_at_the_container() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let written = directory.path().join("env");

    let scenario = directory.path().join("scenario.toml");
    let script = directory.path().join("service.sh");

    // A script rather than a command line, because `run` is split on
    // whitespace and given no shell, which is the right call for a service
    // under test and means a test that wants one has to bring its own.
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n%s' \"$DATABASE_URL\" \"$PGPORT\" > {}\nsleep 0.3\n",
            written.display()
        ),
    )
    .expect("write the service");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
    }

    std::fs::write(
        &scenario,
        format!(
            r#"
name = "postgres_wiring"

[[system]]
run = "{}"
ready_when = "immediate"

[deps.postgres]
database = "ledger"

[run]
timeout = "60s"
quiesce_after = "100ms"

[[workload]]
wait = "400ms"

[faults]
enabled = []

[[invariants]]
builtin = "eventually_quiescent"
"#,
            script.display()
        ),
    )
    .expect("write a scenario");

    let resolved = misorder::scenario::Scenario::load(&scenario)
        .expect("the scenario parses")
        .resolve()
        .expect("and resolves");

    let environment_address = {
        // Started once here only to learn what a container's address looks
        // like, so the assertion below can say the service was not given one.
        let deps = resolved.deps.clone();
        let probe = Environment::start(&deps, &RunSettings::default())
            .await
            .expect("a postgres container starts");

        let address = probe
            .address_of("postgres")
            .expect("an address")
            .to_string();

        probe.stop().await;

        address
    };

    let outcome = misorder::runner::Runner::new(resolved)
        .quiet()
        .execute(misorder::runner::Run::Seed(1))
        .await
        .expect("the run completes");

    assert!(
        outcome.violations.is_empty(),
        "nothing was perturbed: {:?}",
        outcome.violations
    );

    let told = std::fs::read_to_string(&written).expect("the service wrote down its environment");
    let mut lines = told.lines();
    let url = lines.next().expect("a DATABASE_URL");
    let port = lines.next().expect("a PGPORT");

    assert!(
        url.starts_with("postgres://misorder:misorder@127.0.0.1:"),
        "the service is given a full URL for the proxy: {url}"
    );
    assert!(
        url.ends_with("/ledger"),
        "and the scenario's database: {url}"
    );
    assert!(
        url.contains(port),
        "PGHOST and PGPORT have to agree with the URL, since a service reads \
         one or the other: {url} against {port}"
    );

    let container_port = environment_address.rsplit_once(':').expect("host:port").1;

    assert_ne!(
        port, container_port,
        "the service must be given the proxy's port, not the container's. A service \
         that reached Postgres directly would produce a clean run that tested nothing"
    );
}

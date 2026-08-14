//! Starting the service under test.
//!
//! A process, started from a command line, told where to listen through its own
//! environment. That is the whole interface, and it is the language stance in
//! one module: a Go service and a Rust service are started identically, because
//! neither of them imports anything, links anything, or sets a build flag.
//!
//! # Why misorder picks the port
//!
//! The scenario does not name one. `mis fuzz --parallel 16` has sixteen copies
//! of the service up at once, and a port written in a file would have them
//! fighting over it. The failure that produces is the worst kind: it looks like
//! a flaky service, it moves when you add a seed, and nothing in the trace
//! explains it.
//!
//! The port is reserved by binding it and letting go, so there is a window in
//! which something else could take it. The alternative is the service telling
//! misorder which port it chose, and that needs an SDK in the service, which is
//! the one thing this design never does.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};

use crate::error::{Error, Result};
use crate::scenario::file::{RunSettings, System};

/// How often a starting service is checked for having opened its port.
///
/// Before the first fork exists, so it decides nothing and appears in no trace.
/// An adapter reading the clock would be a bug; the runner waiting for a
/// process to come up is the harness doing its job.
const READY_POLL: Duration = Duration::from_millis(10);

/// A running service under test.
#[derive(Debug)]
pub struct Service {
    child: Child,
    address: SocketAddr,
    command: String,
}

impl Service {
    /// Starts one `[[system]]` and waits for it to open its port.
    ///
    /// `injected` is the environment the proxies produced. It goes on top of
    /// the scenario's own `env` and cannot be overridden from it: a service
    /// pointed at the real dependency instead of the proxy produces a clean run
    /// that tested nothing.
    pub async fn start(
        system: &System,
        address: SocketAddr,
        injected: &[(String, String)],
        settings: &RunSettings,
    ) -> Result<Self> {
        let argv = split_command(&system.run)?;

        let (program, arguments) = argv
            .split_first()
            .ok_or_else(|| Error::Scenario("a [[system]] has an empty `run`".to_string()))?;

        let mut environment: BTreeMap<String, String> = system.env.clone();

        environment.insert(system.listen_env.clone(), address.port().to_string());

        for (name, value) in injected {
            environment.insert(name.clone(), value.clone());
        }

        let mut command = Command::new(program);

        command
            .args(arguments)
            .envs(&environment)
            // The service's own output is the user's production shape, and this
            // process never records that. Piping it here would put payloads in
            // misorder's logs by default, which is the one thing a buyer in
            // this segment checks for.
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // A run that panics must not leave a service behind holding the
            // port the next run is about to be given.
            .kill_on_drop(true);

        if let Some(directory) = &system.cwd {
            command.current_dir(directory);
        }

        let child = command.spawn().map_err(|error| {
            Error::Environment(format!("could not start `{}`: {error}", system.run))
        })?;

        let mut service = Self {
            child,
            address,
            command: system.run.clone(),
        };

        service.wait_until_listening(settings.ready_timeout).await?;

        Ok(service)
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Waits for the port to accept a connection.
    ///
    /// Driving a workload at a service that is not listening yet produces a
    /// failure that is entirely the harness's fault, and one false failure
    /// costs more trust than several missed real ones. A service that exits
    /// while starting is reported as itself rather than as a timeout, because
    /// "your service died" and "your service was slow" send someone to
    /// different places.
    async fn wait_until_listening(&mut self, timeout: Duration) -> Result<()> {
        let started = Instant::now();

        loop {
            if let Some(status) = self.child.try_wait().map_err(Error::Io)? {
                return Err(Error::Environment(format!(
                    "`{}` exited with {status} before it listened on {}",
                    self.command, self.address
                )));
            }

            if TcpStream::connect(self.address).await.is_ok() {
                tracing::debug!(command = %self.command, address = %self.address, "service is up");

                return Ok(());
            }

            if started.elapsed() >= timeout {
                return Err(Error::Timeout {
                    what: format!("`{}` never listened on {}", self.command, self.address),
                    elapsed: started.elapsed(),
                });
            }

            tokio::time::sleep(READY_POLL).await;
        }
    }

    /// Stops the service.
    ///
    /// Best effort, and never fails the run. A service that was slow to die is
    /// an annoyance; a run reported as failed because teardown was untidy is a
    /// false failure, and those are the expensive kind.
    pub async fn stop(mut self) {
        if let Err(error) = self.child.start_kill() {
            tracing::debug!(command = %self.command, %error, "could not signal the service");
        }

        if let Err(error) = self.child.wait().await {
            tracing::debug!(command = %self.command, %error, "could not reap the service");
        }
    }
}

/// Reserves a loopback port for a service to listen on.
///
/// Bound and released rather than guessed, so two runs on one machine cannot be
/// handed the same number. See the module docs for the window this leaves and
/// why the alternative is worse.
pub async fn reserve_port() -> Result<SocketAddr> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;

    listener.local_addr().map_err(Error::Io)
}

/// Splits a command line the way a shell would, without being one.
///
/// Quotes and nothing else. No expansion, no globbing, no pipelines: a scenario
/// that needed those would be running a shell script, and the scenario should
/// name the script rather than grow half a shell.
fn split_command(line: &str) -> Result<Vec<String>> {
    let mut argv = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut started = false;

    for character in line.chars() {
        match (quote, character) {
            (Some(open), c) if c == open => quote = None,
            (Some(_), c) => current.push(c),
            (None, c @ ('\'' | '"')) => {
                quote = Some(c);
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started {
                    argv.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            (None, c) => {
                current.push(c);
                started = true;
            }
        }
    }

    if quote.is_some() {
        return Err(Error::Scenario(format!("`{line}` has an unclosed quote")));
    }

    if started {
        argv.push(current);
    }

    if argv.is_empty() {
        return Err(Error::Scenario(
            "a [[system]] has an empty `run`".to_string(),
        ));
    }

    Ok(argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn system(run: &str) -> System {
        System {
            run: run.to_string(),
            ready_when: crate::scenario::file::Ready::Immediate,
            cwd: None,
            env: BTreeMap::new(),
            listen_env: "PORT".to_string(),
        }
    }

    #[test]
    fn a_plain_command_splits_on_whitespace() {
        assert_eq!(
            split_command("./target/debug/ledger --verbose").expect("split"),
            vec!["./target/debug/ledger", "--verbose"]
        );
    }

    #[test]
    fn a_quoted_argument_survives_its_spaces() {
        assert_eq!(
            split_command("./service --flag 'two words' \"and more\"").expect("split"),
            vec!["./service", "--flag", "two words", "and more"]
        );
    }

    #[test]
    fn an_empty_argument_is_kept() {
        assert_eq!(
            split_command("./service --name ''").expect("split"),
            vec!["./service", "--name", ""]
        );
    }

    #[test]
    fn an_unclosed_quote_is_refused_rather_than_guessed_at() {
        let error = split_command("./service 'oops").expect_err("unclosed");

        assert!(error.to_string().contains("unclosed"), "got {error}");
    }

    #[test]
    fn an_empty_command_is_refused() {
        assert!(split_command("   ").is_err());
    }

    #[tokio::test]
    async fn two_reservations_do_not_collide() {
        let first = reserve_port().await.expect("reserve");
        let second = reserve_port().await.expect("reserve");

        assert_ne!(first.port(), second.port());
        assert!(first.ip().is_loopback());
    }

    #[tokio::test]
    async fn a_service_that_never_listens_times_out_rather_than_hanging() {
        let address = reserve_port().await.expect("reserve");

        let settings = RunSettings {
            ready_timeout: Duration::from_millis(50),
            ..RunSettings::default()
        };

        // Sleeps rather than exiting, so this is the timeout path and not the
        // exited-early path.
        let error = Service::start(&system("sleep 30"), address, &[], &settings)
            .await
            .expect_err("never listens");

        assert!(matches!(error, Error::Timeout { .. }), "got {error:?}");
    }

    #[tokio::test]
    async fn a_service_that_dies_on_startup_says_so_rather_than_timing_out() {
        let address = reserve_port().await.expect("reserve");

        let settings = RunSettings {
            ready_timeout: Duration::from_secs(30),
            ..RunSettings::default()
        };

        let error = Service::start(&system("false"), address, &[], &settings)
            .await
            .expect_err("exits");

        assert!(
            error.to_string().contains("exited"),
            "`your service died` and `your service was slow` send someone to \
             different places, got {error}"
        );
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_is_an_environment_error() {
        let address = reserve_port().await.expect("reserve");

        let error = Service::start(
            &system("./no-such-binary-anywhere"),
            address,
            &[],
            &RunSettings::default(),
        )
        .await
        .expect_err("no such binary");

        assert!(matches!(error, Error::Environment(_)), "got {error:?}");
    }
}

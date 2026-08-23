//! Starting the service under test.
//!
//! The service is an ordinary process. misorder spawns it, hands it a port to
//! listen on through its environment, waits for it to come up, and kills it
//! when the run ends. It links nothing, imports nothing, and is not told that
//! any of this is happening — which is the whole language stance in one
//! module: a Go service and a Rust service are started identically.
//!
//! # Why the port comes from here
//!
//! For an ingress run the proxy sits *in front* of the service, so the service
//! has to be listening somewhere the proxy can forward to. Letting the scenario
//! pin that port would work exactly once: `mis fuzz --parallel 16` runs sixteen
//! services at a time, and sixteen processes cannot share a port. So misorder
//! chooses a free one per run and sets it in the service's environment.
//!
//! The variable name is the scenario's to pick, because there is no convention
//! worth guessing at — `PORT` for most things, `HTTP_PORT`, `LISTEN_ADDR` for
//! others.

use std::net::SocketAddr;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::process::{Child, Command};

use crate::error::{Error, Result};
use crate::scenario::file::{Ready, System};

/// A running service under test.
pub struct Service {
    child: Child,
    /// Where it was told to listen, when the scenario asked for a port.
    address: Option<SocketAddr>,
    command: String,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("command", &self.command)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl Service {
    /// Spawns one system, with `extra` injected on top of its own environment.
    ///
    /// `extra` is where the dependency addresses arrive, and it is applied
    /// after the scenario's own `env` so it cannot be overridden there: a
    /// service pointed at the real dependency instead of the proxy produces a
    /// clean run that tested nothing.
    pub async fn start(
        system: &System,
        extra: &[(String, String)],
        inherit_output: bool,
    ) -> Result<Self> {
        let address = match &system.listen_env {
            Some(_) => Some(free_port()?),
            None => None,
        };

        let mut parts = system.run.split_whitespace();

        let program = parts.next().ok_or_else(|| {
            Error::Scenario("a [[system]] has an empty `run` command".to_string())
        })?;

        let mut command = Command::new(program);

        command.args(parts);

        // Inherited for a single run: the service's own logs are how someone
        // works out why their scenario did not come up, and swallowing them to
        // keep misorder's output tidy would trade the user's debugging for our
        // formatting.
        //
        // Discarded for a sweep. Sixteen services writing to one terminal do
        // not take turns - two `write` calls on the same fd interleave mid-line
        // and produce text that is not any of the things either of them wrote.
        // A sweep's output is the report, and the way to see one seed's logs is
        // to run that seed.
        if inherit_output {
            command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }

        if let Some(cwd) = &system.cwd {
            command.current_dir(cwd);
        }

        for (key, value) in &system.env {
            command.env(key, value);
        }

        for (key, value) in extra {
            command.env(key, value);
        }

        if let (Some(variable), Some(address)) = (&system.listen_env, address) {
            command.env(variable, address.port().to_string());
        }

        // Killed when the handle drops, so a panic between here and `stop`
        // does not leave a service running against a port the next run wants.
        command.kill_on_drop(true);

        let child = command.spawn().map_err(|error| {
            Error::Environment(format!(
                "could not start the system under test `{}`: {error}",
                system.run
            ))
        })?;

        Ok(Self {
            child,
            address,
            command: system.run.clone(),
        })
    }

    /// Where the service is listening, when misorder chose the port.
    pub fn address(&self) -> Option<SocketAddr> {
        self.address
    }

    /// Blocks until the service is ready, or the timeout expires.
    ///
    /// Starting the workload before the service is listening produces a failure
    /// that is entirely the harness's fault, and one invented failure costs more
    /// trust than several missed real ones. That is why an unready service is a
    /// [`Error::Environment`] — exit code 1, not a finding.
    pub async fn await_ready(&mut self, ready: Ready, timeout: Duration) -> Result<()> {
        match ready {
            Ready::Immediate => Ok(()),
            Ready::HttpListening => self.await_listening(timeout).await,
            // The proxy-observed readiness signals are answered by the run
            // loop, which is the only thing that sees proxy events. Reaching
            // them here means an ingress scenario asked for one, and the honest
            // answer is which ones do apply.
            other => Err(Error::Scenario(format!(
                "`ready_when = \"{other}\"` is detected from proxy traffic, which an ingress \
                 scenario has none of before its workload starts; use \"http_listening\" or \
                 \"immediate\""
            ))),
        }
    }

    /// Polls the service's port until something accepts.
    async fn await_listening(&mut self, timeout: Duration) -> Result<()> {
        let Some(address) = self.address else {
            return Err(Error::Scenario(
                "`ready_when = \"http_listening\"` needs to know which port the service listens \
                 on; give the [[system]] a `listen_env` and misorder will choose one and set it"
                    .to_string(),
            ));
        };

        let deadline = Instant::now() + timeout;

        loop {
            // Asked first, so a service that exited immediately is reported as
            // having exited rather than as having failed to listen. The two
            // have completely different fixes and the message is most of the
            // value.
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(Error::Environment(format!(
                    "the system under test `{}` exited with {status} before it started listening",
                    self.command
                )));
            }

            if TcpStream::connect(address).await.is_ok() {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(Error::Environment(format!(
                    "the system under test `{}` was not listening on {address} within {timeout:?}",
                    self.command
                )));
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Stops the service.
    ///
    /// Best effort and never fails the run, for the same reason dependency
    /// teardown is: a run reported as failed because cleanup was slow is a
    /// false failure, and those are the expensive kind.
    pub async fn stop(mut self) {
        if let Err(error) = self.child.kill().await {
            tracing::debug!(%error, command = %self.command, "could not stop the system under test");
        }
    }
}

/// A port nothing is listening on.
///
/// Bound and immediately released, so there is a window in which something else
/// could take it. That race is unavoidable without handing the socket to the
/// child, which is not portable, and the alternative — a fixed port in the
/// scenario — fails every time rather than rarely.
fn free_port() -> Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| {
        Error::Environment(format!(
            "could not find a free port for the service: {error}"
        ))
    })?;

    listener
        .local_addr()
        .map_err(|error| Error::Environment(format!("could not read the chosen port: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn system(run: &str) -> System {
        System {
            run: run.to_string(),
            ready_when: Ready::Immediate,
            cwd: None,
            env: Default::default(),
            listen_env: None,
        }
    }

    /// What `free_port` actually promises: a real loopback port, off the
    /// ephemeral allocator.
    ///
    /// Deliberately not "and it can be bound again". The port is released
    /// before it is returned, so between that and any later bind, anything on
    /// the machine may take it — including another test in this suite, which is
    /// how the stricter version of this assertion failed. That race is
    /// documented on the function rather than tested for, because a test that
    /// asserts a race does not happen is a test that fails on a busy machine
    /// and teaches everyone to re-run the suite.
    #[test]
    fn a_free_port_is_a_real_loopback_port() {
        let address = free_port().expect("a free port");

        assert!(address.ip().is_loopback());
        assert!(address.port() > 0, "the ephemeral port was not resolved");
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_blames_the_scenario_clearly() {
        let error = Service::start(&system("definitely-not-a-real-binary-xyz"), &[], false)
            .await
            .expect_err("no such program");

        assert!(matches!(error, Error::Environment(_)), "got {error:?}");
        assert!(
            error
                .to_string()
                .contains("definitely-not-a-real-binary-xyz"),
            "the message names the command: {error}"
        );
    }

    #[tokio::test]
    async fn an_empty_run_command_is_a_scenario_error() {
        let error = Service::start(&system("   "), &[], false)
            .await
            .expect_err("nothing to run");

        assert!(matches!(error, Error::Scenario(_)), "got {error:?}");
    }

    /// A service that exits at once is reported as having exited, not as
    /// having failed to listen. Same symptom from the outside, completely
    /// different fix.
    #[tokio::test]
    async fn a_service_that_exits_is_reported_as_exited() {
        let mut spec = system("true");
        spec.listen_env = Some("PORT".to_string());
        spec.ready_when = Ready::HttpListening;

        let mut service = Service::start(&spec, &[], false).await.expect("spawn true");

        let error = service
            .await_ready(Ready::HttpListening, Duration::from_secs(2))
            .await
            .expect_err("true exits");

        assert!(
            error.to_string().contains("exited"),
            "expected an exit, got {error}"
        );
    }

    #[tokio::test]
    async fn http_listening_without_a_listen_env_says_what_to_add() {
        let mut service = Service::start(&system("sleep 5"), &[], false)
            .await
            .expect("spawn sleep");

        let error = service
            .await_ready(Ready::HttpListening, Duration::from_millis(50))
            .await
            .expect_err("no port");

        assert!(error.to_string().contains("listen_env"), "got {error}");

        service.stop().await;
    }

    #[tokio::test]
    async fn a_proxy_observed_readiness_signal_is_refused_for_ingress() {
        let mut service = Service::start(&system("sleep 5"), &[], false)
            .await
            .expect("spawn sleep");

        let error = service
            .await_ready(Ready::PostgresConnected, Duration::from_millis(50))
            .await
            .expect_err("not an ingress signal");

        assert!(matches!(error, Error::Scenario(_)), "got {error:?}");
        assert!(error.to_string().contains("http_listening"), "got {error}");

        service.stop().await;
    }
}

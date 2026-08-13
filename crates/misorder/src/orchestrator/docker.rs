//! Talking to the Docker daemon.
//!
//! One place, so that replacing it with a hand-rolled socket client, or
//! pointing it at Podman, is a change here and nowhere else. Nothing in this
//! module appears in misorder's public API.

use crate::error::{Error, Result};
use crate::orchestrator::{Environment, default_images};
use crate::scenario::file::{Deps, RunSettings};

/// A connection to the local Docker daemon.
#[derive(Clone)]
pub struct Client {
    docker: bollard::Docker,
}

impl Client {
    /// Connects using the usual local defaults: `DOCKER_HOST`, then the unix
    /// socket, then the named pipe on Windows.
    ///
    /// The error says what to do about it. "No such file or directory" as the
    /// first thing a new user sees, when the actual problem is that Docker is
    /// not running, is a bad first five minutes.
    pub async fn connect() -> Result<Self> {
        let docker = bollard::Docker::connect_with_local_defaults().map_err(|error| {
            Error::Environment(format!(
                "cannot reach the Docker daemon ({error}); misorder starts real dependencies, \
                 so Docker or a compatible daemon has to be running"
            ))
        })?;

        Ok(Self { docker })
    }

    /// Whether the daemon is actually answering.
    ///
    /// Separate from [`Client::connect`], which only builds a client and
    /// succeeds against a socket nothing is listening on.
    pub async fn ping(&self) -> Result<()> {
        self.docker.ping().await.map_err(|error| {
            Error::Environment(format!("the Docker daemon did not answer: {error}"))
        })?;

        Ok(())
    }

    /// Starts every dependency the scenario declared.
    pub async fn start_declared(&self, deps: &Deps, settings: &RunSettings) -> Result<Environment> {
        self.ping().await?;

        for name in deps.declared() {
            let image = default_images().get(name).copied().unwrap_or("unknown");

            tracing::debug!(
                dependency = name,
                image,
                ready_timeout = ?settings.ready_timeout,
                "would pull and start"
            );
        }

        Err(Error::Unsupported(
            "starting containers is not implemented yet".to_string(),
        ))
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

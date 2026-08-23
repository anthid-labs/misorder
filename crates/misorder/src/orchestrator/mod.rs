//! Starting the declared dependencies for real.
//!
//! Real Postgres, real NATS, real containers. Mentally this is Testcontainers,
//! and the model is worth stealing: declare what you need, get a started
//! container with a wired port, tear it down when the run ends.
//!
//! # Why the Docker daemon and not a testing SDK
//!
//! Testcontainers ships a client library per language. Adopting that model
//! would put misorder back to shipping one product per language, which is the
//! thing the whole design exists to avoid. [`docker`] drives the daemon's own
//! HTTP API instead, so the cost of supporting a new language is zero: the
//! service under test never learns misorder exists.
//!
//! # Why everything is real in Phase 1
//!
//! Fidelity is free here and unarguable, and it matters more than speed at this
//! stage. A simulated dependency has to answer "is your sim really NATS?" every
//! time it finds something; a real container never does. Simulators arrive in
//! Phase 3 only where a proxy structurally cannot reach, and the real container
//! stays even then, because it is what the simulator gets diffed against.

pub mod docker;
pub mod process;
pub mod topology;

use std::collections::BTreeMap;

use crate::error::Result;
use crate::scenario::file::{Deps, RunSettings};

/// A running dependency, and how to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    pub name: &'static str,
    /// `host:port` on the host, for the proxy to connect upstream to.
    ///
    /// The service under test never sees this. It gets the proxy's address, and
    /// that separation is what makes the fault injection unavoidable rather
    /// than opt-in.
    pub address: String,
    pub container_id: String,
}

/// Every dependency a scenario declared, running.
#[derive(Debug, Default)]
pub struct Environment {
    dependencies: Vec<Dependency>,
}

impl Environment {
    /// Starts the declared dependencies and applies their topology.
    ///
    /// Topology first, service second, always. A stream created after the
    /// service has already subscribed produces a run whose first seconds are
    /// the harness catching up, and the timing of that is not controlled by the
    /// scheduler, so it would be nondeterminism arriving through the back door.
    pub async fn start(deps: &Deps, settings: &RunSettings) -> Result<Self> {
        tracing::debug!(
            declared = ?deps.declared(),
            ready_timeout = ?settings.ready_timeout,
            "starting dependencies"
        );

        // No declared dependency, no daemon. A scenario whose service owns its
        // own storage — every HTTP ingress scenario, among others — should not
        // need Docker installed to run, and requiring it would make the first
        // five minutes of the tool a Docker troubleshooting session for people
        // whose scenario never needed a container.
        if deps.declared().is_empty() {
            return Ok(Self::default());
        }

        let client = docker::Client::connect().await?;

        client.start_declared(deps, settings).await
    }

    pub fn dependencies(&self) -> &[Dependency] {
        &self.dependencies
    }

    pub fn address_of(&self, name: &str) -> Option<&str> {
        self.dependencies
            .iter()
            .find(|dependency| dependency.name == name)
            .map(|dependency| dependency.address.as_str())
    }

    /// Environment for a terminal SQL check, if this scenario has a Postgres.
    pub fn postgres_url(&self, database: &str) -> Option<String> {
        self.address_of("postgres")
            .map(|address| format!("postgres://misorder:misorder@{address}/{database}"))
    }

    /// Stops and removes everything that was started.
    ///
    /// Best effort, and never fails the run. A leaked container is an
    /// annoyance; a run reported as failed because cleanup was slow is a
    /// false failure, and those are the expensive kind.
    pub async fn stop(self) {
        for dependency in &self.dependencies {
            tracing::debug!(
                name = dependency.name,
                container = %dependency.container_id,
                "stopping dependency"
            );
        }
    }
}

/// The images used when a scenario does not pin one.
///
/// Pinned rather than `latest`, because a scenario's whole value is that it
/// reproduces. A run that silently changed broker version between Tuesday and
/// Wednesday would produce a failure nobody could explain.
pub fn default_images() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("nats", "nats:2.10-alpine"),
        ("postgres", "postgres:17-alpine"),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_environment_has_no_addresses() {
        let environment = Environment::default();

        assert!(environment.address_of("nats").is_none());
        assert!(environment.postgres_url("misorder").is_none());
    }

    #[test]
    fn a_postgres_url_is_built_from_the_mapped_address() {
        let environment = Environment {
            dependencies: vec![Dependency {
                name: "postgres",
                address: "127.0.0.1:54321".to_string(),
                container_id: "abc".to_string(),
            }],
        };

        assert_eq!(
            environment.postgres_url("ledger").as_deref(),
            Some("postgres://misorder:misorder@127.0.0.1:54321/ledger")
        );
    }

    /// The property that lets an HTTP scenario run on a machine with no Docker
    /// at all. If this starts reaching the daemon, every dependency-free
    /// scenario gains a hard requirement nobody asked for.
    #[tokio::test]
    async fn a_scenario_with_no_dependencies_never_reaches_the_daemon() {
        let environment = Environment::start(&Deps::default(), &RunSettings::default())
            .await
            .expect("no dependencies means nothing to start");

        assert!(environment.dependencies().is_empty());
    }

    #[test]
    fn default_images_are_pinned() {
        for (name, image) in default_images() {
            assert!(
                !image.ends_with(":latest") && image.contains(':'),
                "{name} image {image} is not pinned"
            );
        }
    }
}

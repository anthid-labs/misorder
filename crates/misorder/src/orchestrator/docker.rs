//! Talking to the Docker daemon.
//!
//! One place, so that replacing it with a hand-rolled socket client, or
//! pointing it at Podman, is a change here and nowhere else. Nothing in this
//! module appears in misorder's public API.
//!
//! # What a started container is allowed to be
//!
//! Ephemeral, loopback-only, and pinned. Every port is published to
//! `127.0.0.1` on a port the daemon picks, so two sweeps on one machine do not
//! collide and nothing a run starts is reachable from outside it. Every image
//! is a pinned tag rather than `latest`, because a scenario's whole value is
//! that it reproduces, and a run that silently changed server version between
//! Tuesday and Wednesday would produce a failure nobody could explain.
//!
//! # The decisions are separate from the daemon
//!
//! [`Spec`] is what to start and is a pure function of the scenario, so the
//! part that gets a version, a port or a credential wrong is testable with no
//! daemon in the room. What is left here needs one, and the engine's tests
//! stay hermetic.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
};
use futures::StreamExt;

use crate::error::{Error, Result};
use crate::orchestrator::{Dependency, EXTERNAL, Environment, default_images, ready};
use crate::scenario::file::{Deps, RunSettings};

/// Marks every container as this tool's, so a leak is identifiable.
///
/// `docker ps --filter label=com.anthid.misorder` answers "what did a crashed
/// run leave behind", which is the question somebody has at the point they are
/// already annoyed.
const LABEL: &str = "com.anthid.misorder";

/// Names containers apart within one process.
///
/// A sweep runs seeds in parallel, so the pid alone is not unique and a name
/// collision would fail the second run with an error about the first.
static NEXT: AtomicU64 = AtomicU64::new(0);

/// What one dependency has to be started as.
///
/// A pure function of the scenario, so everything that can be wrong about a
/// container before the daemon is involved is wrong here, where a test can see
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub name: &'static str,
    pub image: String,
    /// The port inside the container.
    pub port: u16,
    pub env: Vec<String>,
    pub cmd: Vec<String>,
}

impl Spec {
    /// `5432/tcp`, as the daemon keys a port map by.
    pub fn key(&self) -> String {
        format!("{}/tcp", self.port)
    }
}

/// What to start for a declared dependency, or why it cannot be started.
pub fn spec(name: &str, deps: &Deps) -> Result<Spec> {
    let chosen = |image: &Option<String>| -> Result<String> {
        if let Some(image) = image {
            return Ok(image.clone());
        }

        default_images()
            .get(name)
            .map(|image| (*image).to_string())
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "misorder has no default image for `{name}`, so a scenario that declares one \
                     has to give it an `address` or an `image`"
                ))
            })
    };

    match name {
        "postgres" => {
            let postgres = deps.postgres.as_ref().ok_or_else(|| {
                Error::Internal("a postgres spec was built from a scenario without one".to_string())
            })?;

            Ok(Spec {
                name: "postgres",
                image: chosen(&postgres.image)?,
                port: 5432,
                // The scenario's, not invented here, because the same three
                // values build the `DATABASE_URL` the service reads and the
                // connection the terminal SQL check makes. A container created
                // with one identity and reached with another is a run that
                // fails on its first statement.
                env: vec![
                    format!("POSTGRES_USER={}", postgres.user),
                    format!("POSTGRES_PASSWORD={}", postgres.password),
                    format!("POSTGRES_DB={}", postgres.database),
                ],
                cmd: Vec::new(),
            })
        }

        "nats" => {
            let nats = deps.nats.as_ref().ok_or_else(|| {
                Error::Internal("a nats spec was built from a scenario without one".to_string())
            })?;

            Ok(Spec {
                name: "nats",
                image: chosen(&nats.image)?,
                port: 4222,
                env: Vec::new(),
                // JetStream is off by default in this image, and every stream a
                // scenario declares needs it. Without this the topology step
                // fails against a server that came up perfectly.
                cmd: vec!["-js".to_string()],
            })
        }

        "redis" => {
            let redis = deps.redis.as_ref().ok_or_else(|| {
                Error::Internal("a redis spec was built from a scenario without one".to_string())
            })?;

            Ok(Spec {
                name: "redis",
                image: chosen(&redis.image)?,
                port: 6379,
                env: Vec::new(),
                cmd: Vec::new(),
            })
        }

        other => Err(Error::Unsupported(format!(
            "misorder does not know how to start `{other}`"
        ))),
    }
}

/// Where the daemon published a container port, read back from an inspection.
///
/// Its own function because the shape is three levels of optional and the
/// failure is silent: a missing binding reads as "no ports" rather than as an
/// error, and a run would then proxy to an address nothing is listening on.
pub fn published(ports: Option<&bollard::models::PortMap>, key: &str) -> Option<String> {
    let binding = ports?.get(key)?.as_ref()?.first()?;
    let port = binding.host_port.as_ref()?;

    if port.is_empty() {
        return None;
    }

    // The daemon reports `0.0.0.0` for a binding it made on every interface,
    // which is not an address to connect to. The proxy dials loopback, which is
    // also the only interface anything here publishes on.
    let host = match binding.host_ip.as_deref() {
        Some("") | Some("0.0.0.0") | Some("::") | None => "127.0.0.1",
        Some(host) => host,
    };

    Some(format!("{host}:{port}"))
}

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

    /// Starts every dependency the scenario declared and does not already have.
    ///
    /// A dependency with an `address` is somebody else's and is carried through
    /// untouched. Everything else is pulled, started, waited for, and owned:
    /// if any one of them fails, the ones already up are removed before the
    /// error is returned. A run that failed and leaked a Postgres makes the
    /// next run fail too, and the second failure is the one that gets reported.
    pub async fn start_declared(&self, deps: &Deps, settings: &RunSettings) -> Result<Environment> {
        self.ping().await?;

        let external: HashMap<&str, &str> = deps.external().into_iter().collect();
        let mut started = Vec::new();

        for name in deps.declared() {
            if let Some(address) = external.get(name) {
                started.push(Dependency {
                    name,
                    address: (*address).to_string(),
                    container_id: EXTERNAL.to_string(),
                });

                continue;
            }

            match self.start_one(&spec(name, deps)?, settings).await {
                Ok(dependency) => started.push(dependency),
                Err(error) => {
                    for dependency in &started {
                        self.remove(&dependency.container_id).await;
                    }

                    return Err(error);
                }
            }
        }

        Ok(Environment::owning(started, self.clone()))
    }

    /// Pulls, creates, starts and waits for one dependency.
    async fn start_one(&self, spec: &Spec, settings: &RunSettings) -> Result<Dependency> {
        self.ensure_image(&spec.image).await?;

        let name = format!(
            "misorder-{}-{}-{}",
            spec.name,
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );

        let options = CreateContainerOptionsBuilder::default().name(&name).build();

        let host_config = HostConfig {
            port_bindings: Some(HashMap::from([(
                spec.key(),
                Some(vec![PortBinding {
                    // Loopback, always. A dependency a run started is nobody
                    // else's to reach, and publishing on every interface would
                    // put an unauthenticated Postgres on the network of
                    // whoever ran the tests.
                    host_ip: Some("127.0.0.1".to_string()),
                    // Empty means the daemon picks, which is what lets sixteen
                    // seeds run at once without agreeing on port numbers.
                    host_port: Some(String::new()),
                }]),
            )])),
            ..Default::default()
        };

        let config = ContainerCreateBody {
            image: Some(spec.image.clone()),
            exposed_ports: Some(vec![spec.key()]),
            env: Some(spec.env.clone()),
            cmd: (!spec.cmd.is_empty()).then(|| spec.cmd.clone()),
            labels: Some(HashMap::from([(LABEL.to_string(), spec.name.to_string())])),
            host_config: Some(host_config),
            ..Default::default()
        };

        let created = self
            .docker
            .create_container(Some(options), config)
            .await
            .map_err(|error| {
                Error::Environment(format!(
                    "could not create a {} container: {error}",
                    spec.name
                ))
            })?;

        let id = created.id;

        if let Err(error) = self.docker.start_container(&id, None).await {
            self.remove(&id).await;

            return Err(Error::Environment(format!(
                "could not start {}: {error}",
                spec.name
            )));
        }

        let address = match self.address_of(&id, spec).await {
            Ok(address) => address,
            Err(error) => {
                self.remove(&id).await;

                return Err(error);
            }
        };

        if let Err(error) = ready::wait(spec.name, &address, settings.ready_timeout).await {
            self.remove(&id).await;

            return Err(error);
        }

        tracing::debug!(
            dependency = spec.name,
            image = %spec.image,
            container = %id,
            address,
            "started dependency"
        );

        Ok(Dependency {
            name: spec.name,
            address,
            container_id: id,
        })
    }

    /// Reads back where the daemon published the container's port.
    async fn address_of(&self, id: &str, spec: &Spec) -> Result<String> {
        let inspected = self
            .docker
            .inspect_container(id, None)
            .await
            .map_err(|error| {
                Error::Environment(format!("could not inspect {}: {error}", spec.name))
            })?;

        let ports = inspected
            .network_settings
            .as_ref()
            .and_then(|settings| settings.ports.as_ref());

        published(ports, &spec.key()).ok_or_else(|| {
            Error::Environment(format!(
                "{} started but the daemon published no host port for {}",
                spec.name,
                spec.key()
            ))
        })
    }

    /// Pulls an image, unless the daemon already has it.
    ///
    /// Checked first rather than pulled unconditionally, because a pull is the
    /// one step here that needs the network, and a sweep on a machine with the
    /// image already local should not need one.
    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }

        let (repository, tag) = image.rsplit_once(':').unwrap_or((image, "latest"));

        let options = CreateImageOptionsBuilder::default()
            .from_image(repository)
            .tag(tag)
            .build();

        let mut pulling = self.docker.create_image(Some(options), None, None);

        tracing::debug!(image, "pulling image");

        while let Some(progress) = pulling.next().await {
            progress.map_err(|error| {
                Error::Environment(format!("could not pull `{image}`: {error}"))
            })?;
        }

        Ok(())
    }

    /// Stops and deletes a container, best effort.
    ///
    /// Never fails the run. A leaked container is an annoyance; a run reported
    /// as failed because cleanup was slow is a false failure, and those are the
    /// expensive kind.
    pub async fn remove(&self, id: &str) {
        let options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();

        if let Err(error) = self.docker.remove_container(id, Some(options)).await {
            tracing::warn!(container = id, %error, "could not remove a container");
        }
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::scenario::file::{Nats, Postgres, Redis};

    fn deps() -> Deps {
        Deps {
            postgres: Some(Postgres {
                database: "ledger".to_string(),
                ..Postgres::default()
            }),
            nats: Some(Nats::default()),
            redis: Some(Redis::default()),
        }
    }

    #[test]
    fn a_postgres_is_started_with_the_credentials_its_url_is_built_from() {
        let spec = spec("postgres", &deps()).expect("a declared postgres has a spec");

        assert!(spec.env.contains(&"POSTGRES_USER=misorder".to_string()));
        assert!(spec.env.contains(&"POSTGRES_PASSWORD=misorder".to_string()));
        assert!(
            spec.env.contains(&"POSTGRES_DB=ledger".to_string()),
            "the scenario's database, not the default: {:?}",
            spec.env
        );
    }

    /// Every stream a scenario declares needs JetStream, and the image does not
    /// start it. Without the flag the container comes up perfectly and the
    /// topology step fails against it.
    #[test]
    fn a_nats_is_started_with_jetstream() {
        let spec = spec("nats", &deps()).expect("a declared nats has a spec");

        assert_eq!(spec.cmd, vec!["-js".to_string()]);
    }

    #[test]
    fn a_scenario_image_wins_over_the_default() {
        let deps = Deps {
            postgres: Some(Postgres {
                image: Some("timescale/timescaledb:2.17.2-pg17".to_string()),
                ..Postgres::default()
            }),
            ..Deps::default()
        };

        assert_eq!(
            spec("postgres", &deps).expect("spec").image,
            "timescale/timescaledb:2.17.2-pg17"
        );
    }

    #[test]
    fn a_dependency_misorder_cannot_start_says_so_rather_than_guessing_an_image() {
        let error = spec("clickhouse", &deps()).expect_err("there is no spec for it");

        assert!(matches!(error, Error::Unsupported(_)), "{error}");
    }

    #[test]
    fn a_published_port_is_read_back_as_a_loopback_address() {
        let ports = HashMap::from([(
            "5432/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: Some("54321".to_string()),
            }]),
        )]);

        assert_eq!(
            published(Some(&ports), "5432/tcp").as_deref(),
            Some("127.0.0.1:54321"),
            "`0.0.0.0` is what the daemon reports, not somewhere to connect"
        );
    }

    /// The silent one. A container that came up with no binding reads as "no
    /// ports" rather than as an error, and a run would then put a proxy in
    /// front of an address nothing is listening on.
    #[test]
    fn a_container_with_no_binding_has_no_address() {
        assert_eq!(published(None, "5432/tcp"), None);

        let empty = HashMap::from([("5432/tcp".to_string(), None)]);
        assert_eq!(published(Some(&empty), "5432/tcp"), None);

        let unassigned = HashMap::from([(
            "5432/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some(String::new()),
            }]),
        )]);
        assert_eq!(published(Some(&unassigned), "5432/tcp"), None);
    }
}

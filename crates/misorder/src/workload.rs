//! Driving traffic at the service under test.
//!
//! Deliberately thin. The workload's job is to get the system into the state
//! where the interesting orderings exist, not to be a load generator: the
//! failures this tool is for need one order and a broker that misbehaves, not
//! ten thousand orders.
//!
//! # The workload is scheduled too
//!
//! Each step's traffic crosses a proxy, so the delays and drops applied to it
//! come from the same scheduler as everything else. A workload that published
//! on its own timing would be a second source of nondeterminism, and the
//! trace would no longer describe the run.

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::error::{Error, Result};
use crate::event::{Event, Lifecycle};
use crate::orchestrator::Environment;
use crate::proxy::EventSink;
use crate::scenario::file::Step;

/// Bytes of response the driver will read before it stops caring.
///
/// The answers are drained rather than examined: what the service replied is
/// already an event, recorded by the proxy that saw it. Draining exists so the
/// proxy is never blocked writing into a socket nobody is reading.
const MAX_DRAIN: u64 = 8 * 1024 * 1024;

/// Runs a scenario's workload steps in order.
#[derive(Debug)]
pub struct Driver<'a> {
    environment: &'a Environment,
    events: &'a EventSink,
    ingress: Option<SocketAddr>,
}

impl<'a> Driver<'a> {
    pub fn new(environment: &'a Environment, events: &'a EventSink) -> Self {
        Self {
            environment,
            events,
            ingress: None,
        }
    }

    /// Where `post` steps go.
    ///
    /// The ingress proxy, never the service directly. A driver that posted
    /// straight at the service would produce a clean run that explored no
    /// ordering at all, which is the same failure as a scenario that permits no
    /// faults: it passes, and it tested nothing.
    pub fn through_ingress(mut self, ingress: SocketAddr) -> Self {
        self.ingress = Some(ingress);
        self
    }

    /// Runs every step, then reports the workload complete.
    ///
    /// [`Lifecycle::WorkloadComplete`] is what separates "the system is still
    /// working" from "the system never settled", and `eventually_quiescent`
    /// declines to fire without it. A driver that returned early without
    /// emitting it would turn its own failure into the service's.
    pub async fn run(&self, steps: &[Step], at: Duration) -> Result<()> {
        // One connection for every post in the workload, opened when the first
        // one needs it. That is the contract the ingress proxy is written to:
        // a reorder can only mean something if two requests are in flight at
        // once, and a driver that opened a connection per post would leave
        // every reorder with nothing to swap with.
        //
        // The cost is real and is the right trade. A `connection_drop` at that
        // connection's accept fork costs the rest of the workload, so those
        // seeds explore less. Reconnecting instead was tried and reverted: the
        // driver can only notice the close through a write error, the kernel
        // decides how many writes land in the buffer before the FIN arrives, so
        // reconnecting made the number of forks depend on the kernel and one
        // seed stopped producing one run. A workload that explores less is a
        // cost. A seed that means two different things is the end of the tool.
        let mut posts: Option<Posts> = None;

        for step in steps {
            match step {
                Step::Wait(duration) => tokio::time::sleep(*duration).await,
                Step::Publish { subject, payload } => {
                    let address = self.environment.address_of("nats").ok_or_else(|| {
                        Error::Scenario(format!(
                            "a workload step publishes to {subject}, but the scenario declares no \
                             [deps.nats] block"
                        ))
                    })?;

                    tracing::debug!(address, subject, bytes = payload.len(), "would publish");

                    return Err(Error::Unsupported(
                        "publishing a workload step is not implemented yet".to_string(),
                    ));
                }
                Step::Post { path, body } => {
                    let ingress = self.ingress.ok_or_else(|| {
                        Error::Internal(format!(
                            "a workload step posts to {path}, but no ingress proxy was started"
                        ))
                    })?;

                    if posts.is_none() {
                        posts = Some(Posts::open(ingress).await?);
                    }

                    posts
                        .as_mut()
                        .expect("just opened")
                        .send(path, body)
                        .await?;
                }
            }
        }

        // Finished before the workload is called complete, because the half
        // close is what releases a request the schedule deferred, and a run
        // that went quiescent with one still held would report the harness's
        // own hold as a service that never answered.
        if let Some(posts) = posts {
            posts.finish().await?;
        }

        self.events
            .emit_lifecycle(at, Event::Lifecycle(Lifecycle::WorkloadComplete));

        Ok(())
    }
}

/// The connection every `post` step goes down.
#[derive(Debug)]
struct Posts {
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
    host: String,
    /// Whether the far end is still there.
    open: bool,
}

impl Posts {
    async fn open(ingress: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(ingress).await.map_err(|error| {
            Error::Environment(format!(
                "could not reach the ingress proxy on {ingress}: {error}"
            ))
        })?;

        let (read, write) = stream.into_split();

        Ok(Self {
            read,
            write,
            host: ingress.to_string(),
            open: true,
        })
    }

    /// Writes one request and does not wait for its answer.
    async fn send(&mut self, path: &str, body: &[u8]) -> Result<()> {
        if !self.open {
            tracing::debug!(path, "not posted: the ingress connection is already closed");

            return Ok(());
        }

        tracing::debug!(path, bytes = body.len(), "posting");

        match self.write(path, body).await {
            Ok(()) => Ok(()),
            Err(error) if closed_by_the_schedule(&error) => {
                // The proxy closed this connection because the schedule told it
                // to. That is a permitted fault, and what it means is that these
                // deliveries did not arrive, which is the thing under test.
                // Failing here would report misorder's own injected fault as a
                // harness error, which is exit code 1 for something that was
                // working exactly as asked.
                tracing::debug!(path, "the ingress connection was closed by the schedule");

                self.open = false;

                Ok(())
            }
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn write(&mut self, path: &str, body: &[u8]) -> std::io::Result<()> {
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n",
            self.host,
            body.len()
        );

        self.write.write_all(head.as_bytes()).await?;
        self.write.write_all(body).await?;
        self.write.flush().await
    }

    /// Stops sending, then reads until the far end is done.
    async fn finish(mut self) -> Result<()> {
        if self.open
            && let Err(error) = self.write.shutdown().await
            && !closed_by_the_schedule(&error)
        {
            return Err(Error::Io(error));
        }

        let mut drained = Vec::new();

        match self.read.take(MAX_DRAIN).read_to_end(&mut drained).await {
            Ok(_) => {}
            Err(error) if closed_by_the_schedule(&error) => {}
            Err(error) => return Err(Error::Io(error)),
        }

        tracing::debug!(bytes = drained.len(), "drained the answers");

        Ok(())
    }
}

/// Whether this is the far end going away rather than something wrong.
///
/// The far end is misorder's own ingress proxy, so it goes away for exactly one
/// reason: the schedule said `connection_drop`. Every one of these kinds is
/// that, seen from the writing side.
fn closed_by_the_schedule(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_wait_step_needs_no_dependency() {
        let environment = Environment::default();
        let (events, mut receiver) = EventSink::new();
        let driver = Driver::new(&environment, &events);

        driver
            .run(&[Step::Wait(Duration::from_millis(1))], Duration::ZERO)
            .await
            .expect("wait runs");

        let observed = receiver.recv().await.expect("completion event");

        assert!(matches!(
            observed.event,
            Event::Lifecycle(Lifecycle::WorkloadComplete)
        ));
    }

    #[tokio::test]
    async fn publishing_without_a_nats_block_blames_the_scenario() {
        let environment = Environment::default();
        let (events, _receiver) = EventSink::new();
        let driver = Driver::new(&environment, &events);

        let error = driver
            .run(
                &[Step::Publish {
                    subject: "ledger.order".to_string(),
                    payload: Vec::new(),
                }],
                Duration::ZERO,
            )
            .await
            .expect_err("no nats");

        assert!(matches!(error, Error::Scenario(_)), "got {error:?}");
        assert!(error.to_string().contains("[deps.nats]"), "got {error}");
    }
}

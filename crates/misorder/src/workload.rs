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

use std::net::SocketAddr;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::event::{Event, Lifecycle};
use crate::orchestrator::Environment;
use crate::proxy::EventSink;
use crate::scenario::file::Step;

/// Runs a scenario's workload steps in order.
#[derive(Debug)]
pub struct Driver<'a> {
    environment: &'a Environment,
    events: &'a EventSink,
    /// Where HTTP steps post, which is the ingress proxy rather than the
    /// service. `None` for a scenario with no `post` step.
    ingress: Option<SocketAddr>,
    /// Whether the scenario declares any JetStream stream.
    ///
    /// Decides how a `publish` step is sent, and the difference is worth the
    /// field. A JetStream publish waits for the server's `PubAck`, so a subject
    /// no stream covers is reported as the scenario error it is. A core publish
    /// has no such answer: it would succeed against a server that stored
    /// nothing, and the run would check its invariants against a system that
    /// was never given any work.
    streams: bool,
}

impl<'a> Driver<'a> {
    pub fn new(environment: &'a Environment, events: &'a EventSink) -> Self {
        Self {
            environment,
            events,
            ingress: None,
            streams: false,
        }
    }

    /// Whether a stream should capture what `publish` steps send.
    pub fn with_streams(mut self, streams: bool) -> Self {
        self.streams = streams;
        self
    }

    /// Where `post` steps go.
    pub fn with_ingress(mut self, ingress: Option<SocketAddr>) -> Self {
        self.ingress = ingress;
        self
    }

    /// Runs every step, then reports the workload complete.
    ///
    /// [`Lifecycle::WorkloadComplete`] is what separates "the system is still
    /// working" from "the system never settled", and `eventually_quiescent`
    /// declines to fire without it. A driver that returned early without
    /// emitting it would turn its own failure into the service's.
    pub async fn run(&self, steps: &[Step], at: Duration) -> Result<()> {
        let mut index = 0;

        while index < steps.len() {
            // Consecutive posts go out together on one connection. See
            // [`Driver::post_batch`] for why that is a requirement rather than
            // an optimisation.
            if matches!(steps[index], Step::Post { .. }) {
                let start = index;

                while index < steps.len() && matches!(steps[index], Step::Post { .. }) {
                    index += 1;
                }

                self.post_batch(&steps[start..index]).await?;

                continue;
            }

            self.step(&steps[index]).await?;
            index += 1;
        }

        self.events
            .emit_lifecycle(at, Event::Lifecycle(Lifecycle::WorkloadComplete));

        Ok(())
    }

    /// Posts a run of consecutive steps on one connection, then stops sending.
    ///
    /// # Why they go together
    ///
    /// [`Decision::Reorder`](crate::trace::Decision::Reorder) means "let the
    /// fork after this one go first", and on one connection that only has
    /// meaning if two requests can be in flight at once. A driver that waited
    /// for each response before sending the next would give every reorder
    /// nothing to swap with, and a scenario that permitted `reorder` would
    /// quietly explore no reorderings at all — the worst kind of gap, because
    /// the scenario reads as thorough and the sweep reports nothing.
    ///
    /// The half-close at the end is the other half of the same contract: a
    /// request the schedule deferred is released when a later one overtakes it,
    /// or when the client stops sending. Without the shutdown, a reorder on the
    /// final request would wait for a successor that never arrives.
    ///
    /// A `wait` step between two posts therefore splits them into two
    /// connections, which is worth knowing when writing a scenario: `wait` is
    /// for letting the system settle, not for sequencing, and using it between
    /// events costs the reordering that would have found the bug.
    ///
    /// # Why the responses are drained rather than counted
    ///
    /// A request the schedule dropped is never answered. Counting responses
    /// would hang on exactly the runs this tool exists to explore, so the read
    /// side runs to end-of-file and the answers are not inspected: what the
    /// service did with a delivery is an observation the proxy already made.
    async fn post_batch(&self, steps: &[Step]) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let ingress = self.ingress.ok_or_else(|| {
            Error::Internal(
                "a workload step posts, but no ingress proxy was bound for it".to_string(),
            )
        })?;

        let stream = tokio::net::TcpStream::connect(ingress)
            .await
            .map_err(|error| {
                Error::Environment(format!(
                    "the workload driver could not reach the ingress proxy at {ingress}: {error}"
                ))
            })?;

        let (mut read, mut write) = stream.into_split();

        for step in steps {
            let Step::Post { path, body } = step else {
                continue;
            };

            let request = format!(
                "POST {path} HTTP/1.1\r\nHost: misorder\r\ncontent-type: \
                 application/json\r\ncontent-length: {}\r\n\r\n",
                body.len()
            );

            // A write that fails here is very likely the schedule doing its
            // job: `connection_drop` closes this connection, and every
            // subsequent write then fails with a broken pipe. That is a
            // delivery the vendor will retry, not a harness fault, and
            // reporting it as one would turn the most ordinary fault in the
            // vocabulary into a run that "could not complete" - a sweep of
            // 10,000 seeds would report a coverage hole made entirely of
            // working fault injection.
            //
            // Nothing is lost by stopping: the connection is gone, so no
            // further request on it could reach the service anyway.
            if write.write_all(request.as_bytes()).await.is_err()
                || write.write_all(body).await.is_err()
            {
                tracing::debug!(path, "the ingress connection closed mid-workload");

                return Ok(());
            }
        }

        // Same reasoning: a half-close on a connection the schedule already
        // closed is not a failure.
        let _ = write.shutdown().await;

        let mut answers = Vec::new();
        let _ = read.read_to_end(&mut answers).await;

        Ok(())
    }

    /// Sends one message and, where a stream should hold it, waits to be told
    /// it was stored.
    async fn publish(&self, address: &str, subject: &str, payload: Vec<u8>) -> Result<()> {
        let client = async_nats::connect(address).await.map_err(|error| {
            Error::Environment(format!(
                "the workload driver could not reach nats at {address}: {error}"
            ))
        })?;

        if !self.streams {
            client
                .publish(subject.to_string(), payload.into())
                .await
                .map_err(|error| {
                    Error::Environment(format!("publishing to {subject} failed: {error}"))
                })?;

            // Without this the message is still in a client buffer when the
            // connection is dropped at the end of this function, and a workload
            // that published nothing would look like one that did.
            client.flush().await.map_err(|error| {
                Error::Environment(format!("flushing a publish to {subject} failed: {error}"))
            })?;

            return Ok(());
        }

        // Bound rather than used as a temporary. The context owns the client,
        // and letting it drop at the end of this statement closes the
        // connection out from under the `PubAck` that has not arrived yet: the
        // publish then fails with a broken pipe that reads exactly like a
        // mis-declared subject.
        let jetstream = async_nats::jetstream::new(client);

        let acking = jetstream
            .publish(subject.to_string(), payload.into())
            .await
            .map_err(|error| {
                Error::Environment(format!("publishing to {subject} failed: {error}"))
            })?;

        // The `PubAck` is what makes a mis-declared scenario an error instead
        // of a quiet pass. A subject no stream covers gets no responder here,
        // and that is a sentence someone can act on.
        acking.await.map_err(|error| {
            Error::Scenario(format!(
                "nats stored nothing for a workload publish to {subject}: {error}. No declared \
                 stream has a subject matching it."
            ))
        })?;

        Ok(())
    }

    async fn step(&self, step: &Step) -> Result<()> {
        match step {
            Step::Wait(duration) => {
                tokio::time::sleep(*duration).await;
                Ok(())
            }
            Step::Publish { subject, payload } => {
                let address = self.environment.address_of("nats").ok_or_else(|| {
                    Error::Scenario(format!(
                        "a workload step publishes to {subject}, but the scenario declares no \
                         [deps.nats] block"
                    ))
                })?;

                tracing::debug!(address, subject, bytes = payload.len(), "publishing");

                // Straight to the dependency, never through the proxy. The
                // driver stands in for the vendor, so its own publish is not
                // the traffic under test: what the schedule perturbs is the
                // delivery of this message to the service, which crosses the
                // proxy on its way back out.
                self.publish(address, subject, payload.clone()).await
            }
            // Handled in `run`, which groups consecutive posts onto one
            // connection. Reaching here means a single post was routed the
            // wrong way, which is a bug in this module rather than in a
            // scenario.
            Step::Post { path, .. } => Err(Error::Internal(format!(
                "a post to {path} reached the single-step path instead of the batch"
            ))),
        }
    }
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

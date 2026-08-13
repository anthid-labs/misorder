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
}

impl<'a> Driver<'a> {
    pub fn new(environment: &'a Environment, events: &'a EventSink) -> Self {
        Self {
            environment,
            events,
        }
    }

    /// Runs every step, then reports the workload complete.
    ///
    /// [`Lifecycle::WorkloadComplete`] is what separates "the system is still
    /// working" from "the system never settled", and `eventually_quiescent`
    /// declines to fire without it. A driver that returned early without
    /// emitting it would turn its own failure into the service's.
    pub async fn run(&self, steps: &[Step], at: Duration) -> Result<()> {
        for step in steps {
            self.step(step).await?;
        }

        self.events
            .emit_lifecycle(at, Event::Lifecycle(Lifecycle::WorkloadComplete));

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

                tracing::debug!(address, subject, bytes = payload.len(), "would publish");

                Err(Error::Unsupported(
                    "publishing a workload step is not implemented yet".to_string(),
                ))
            }
            Step::Post { path, body } => {
                tracing::debug!(path, bytes = body.len(), "would post");

                Err(Error::Unsupported(
                    "posting a workload step is not implemented yet".to_string(),
                ))
            }
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

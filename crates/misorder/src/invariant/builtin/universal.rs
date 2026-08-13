//! Invariants that hold whatever the dependencies are.

use async_trait::async_trait;

use crate::error::Result;
use crate::event::{Event, Lifecycle, Observed};
use crate::invariant::{CheckContext, Invariant, Violation};

/// The system stops doing work once the workload is done.
///
/// The cheapest invariant and often the first to fire, because most of the
/// failures this tool is built for end the same way: something is still
/// retrying. A service that never settles has a loop, a stuck poller, or a
/// backoff that does not back off, and none of those need a domain assertion to
/// recognise.
///
/// Phase 1 infers quiescence from an idle window with no proxied traffic. That
/// makes this invariant only as good as the window: too short, and a service
/// doing real work looks stuck. `run.quiesce_after` is therefore deliberately
/// generous, and the failure direction is a missed bug rather than an invented
/// one.
#[derive(Debug, Default)]
pub struct EventuallyQuiescent {
    workload_complete: bool,
    quiescent: bool,
}

#[async_trait]
impl Invariant for EventuallyQuiescent {
    fn name(&self) -> &str {
        "eventually_quiescent"
    }

    fn describe(&self) -> &str {
        "the system stops doing work once the workload is done"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        match &observed.event {
            Event::Lifecycle(Lifecycle::WorkloadComplete) => {
                self.workload_complete = true;
            }
            Event::Lifecycle(Lifecycle::Quiescent) => {
                self.quiescent = true;
            }
            _ => {}
        }

        None
    }

    async fn finish(&mut self, context: &CheckContext) -> Result<Option<Violation>> {
        // A run whose workload never finished was cut short by the harness, and
        // reporting that as the service failing to settle would be blaming the
        // wrong party. It is reported as a timeout instead, by the runner.
        if !self.workload_complete || self.quiescent {
            return Ok(None);
        }

        Ok(Some(Violation {
            invariant: self.name().to_string(),
            detail: format!(
                "the workload finished but the system was still active {:?} later",
                context.elapsed
            ),
            at: context.elapsed,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn lifecycle(event: Lifecycle) -> Observed {
        Observed::new(Duration::from_millis(1), Event::Lifecycle(event))
    }

    #[tokio::test]
    async fn settling_after_the_workload_is_clean() {
        let mut check = EventuallyQuiescent::default();

        check.observe(&lifecycle(Lifecycle::WorkloadComplete));
        check.observe(&lifecycle(Lifecycle::Quiescent));

        assert!(
            check
                .finish(&CheckContext::default())
                .await
                .expect("check")
                .is_none()
        );
    }

    #[tokio::test]
    async fn never_settling_is_a_violation() {
        let mut check = EventuallyQuiescent::default();

        check.observe(&lifecycle(Lifecycle::WorkloadComplete));

        assert!(
            check
                .finish(&CheckContext::default())
                .await
                .expect("check")
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_run_cut_short_is_not_blamed_on_the_service() {
        let mut check = EventuallyQuiescent::default();

        assert!(
            check
                .finish(&CheckContext::default())
                .await
                .expect("check")
                .is_none(),
            "no WorkloadComplete means the harness stopped it, not the service"
        );
    }
}

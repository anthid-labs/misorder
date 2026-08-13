//! HTTP and webhook semantics.
//!
//! One adapter covers Stripe, Plaid and a thousand REST vendors at once, and
//! these two invariants are why it pays for itself the day it is turned on:
//! every one of those vendors documents idempotency keys, and every one of them
//! has a customer who found out the hard way that a retry returned something
//! else.

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::event::{ConnectionId, Event, HttpEvent, Observed};
use crate::invariant::{CheckContext, Invariant, Violation};

/// Every accepted request gets a response.
///
/// A request that is neither answered nor explicitly failed is the worst
/// outcome for a payment: the caller does not know whether it happened, and
/// neither retrying nor not retrying is safe.
#[derive(Debug, Default)]
pub struct EveryRequestReachesTerminalState {
    /// Requests in flight, oldest first, per connection.
    pending: HashMap<ConnectionId, Vec<String>>,
}

#[async_trait]
impl Invariant for EveryRequestReachesTerminalState {
    fn name(&self) -> &str {
        "every_request_reaches_terminal_state"
    }

    fn describe(&self) -> &str {
        "every accepted request gets a response or an explicit failure"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let connection = observed.connection?;

        match &observed.event {
            Event::Http(HttpEvent::Request { method, path, .. }) => {
                self.pending
                    .entry(connection)
                    .or_default()
                    .push(format!("{method} {path}"));
                None
            }
            Event::Http(HttpEvent::Response { .. }) => {
                let pending = self.pending.get_mut(&connection)?;
                if !pending.is_empty() {
                    pending.remove(0);
                }
                None
            }
            // Closing with requests in flight is the terminal-state failure,
            // and it is reported here rather than at `finish` so the violation
            // carries the moment it happened.
            Event::Http(HttpEvent::ConnectionClosed) => {
                let pending = self.pending.remove(&connection)?;
                let stranded = pending.first()?;

                Some(Violation {
                    invariant: self.name().to_string(),
                    detail: format!(
                        "{connection} closed with {} request(s) unanswered, starting with \
                         {stranded}",
                        pending.len()
                    ),
                    at: observed.at,
                })
            }
            _ => None,
        }
    }

    async fn finish(
        &mut self,
        context: &CheckContext,
    ) -> Result<Option<Violation>, crate::error::Error> {
        let stranded: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| !pending.is_empty())
            .map(|(connection, pending)| format!("{connection} ({})", pending.join(", ")))
            .collect();

        if stranded.is_empty() {
            return Ok(None);
        }

        Ok(Some(Violation {
            invariant: self.name().to_string(),
            detail: format!(
                "the run went quiescent with requests still unanswered on {}",
                stranded.join("; ")
            ),
            at: context.elapsed,
        }))
    }
}

/// A retried idempotency key returns the response the first attempt got.
///
/// Not "returns success". The contract vendors document is that the *same*
/// response comes back, and the divergence that hurts is a second call
/// returning a different resource id, because the caller then has two records
/// for one payment.
#[derive(Debug, Default)]
pub struct IdempotentRetryReturnsSameResponse {
    /// The response each key got the first time.
    answered: HashMap<String, (u16, Bytes)>,
    /// The key of the request currently in flight, per connection.
    in_flight: HashMap<ConnectionId, String>,
}

#[async_trait]
impl Invariant for IdempotentRetryReturnsSameResponse {
    fn name(&self) -> &str {
        "idempotent_retry_returns_same_response"
    }

    fn describe(&self) -> &str {
        "a retried idempotency key returns the response the first attempt got"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let connection = observed.connection?;

        match &observed.event {
            Event::Http(HttpEvent::Request {
                idempotency_key, ..
            }) => {
                if let Some(key) = idempotency_key {
                    self.in_flight.insert(connection, key.clone());
                }
                None
            }
            Event::Http(HttpEvent::Response { status, body }) => {
                let key = self.in_flight.remove(&connection)?;

                match self.answered.get(&key) {
                    None => {
                        self.answered.insert(key, (*status, body.clone()));
                        None
                    }
                    Some((first_status, first_body)) => {
                        if first_status == status && first_body == body {
                            return None;
                        }

                        Some(Violation {
                            invariant: self.name().to_string(),
                            detail: format!(
                                "idempotency key {key} first returned {first_status} and then \
                                 returned {status}; the caller now has two outcomes for one \
                                 request"
                            ),
                            at: observed.at,
                        })
                    }
                }
            }
            Event::Http(HttpEvent::ConnectionClosed) => {
                self.in_flight.remove(&connection);
                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(millis: u64, connection: u64, event: HttpEvent) -> Observed {
        Observed::on(
            Duration::from_millis(millis),
            ConnectionId(connection),
            Event::Http(event),
        )
    }

    fn request(key: Option<&str>) -> HttpEvent {
        HttpEvent::Request {
            method: "POST".to_string(),
            path: "/v1/charges".to_string(),
            idempotency_key: key.map(str::to_string),
            body: Bytes::new(),
        }
    }

    fn response(status: u16, body: &str) -> HttpEvent {
        HttpEvent::Response {
            status,
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[test]
    fn closing_with_a_request_in_flight_is_a_violation() {
        let mut check = EveryRequestReachesTerminalState::default();

        check.observe(&at(0, 1, request(None)));

        let violation = check
            .observe(&at(1, 1, HttpEvent::ConnectionClosed))
            .expect("should fire");

        assert!(violation.detail.contains("POST /v1/charges"), "{violation}");
    }

    #[test]
    fn an_answered_request_closes_cleanly() {
        let mut check = EveryRequestReachesTerminalState::default();

        check.observe(&at(0, 1, request(None)));
        check.observe(&at(1, 1, response(200, "{}")));

        assert!(
            check
                .observe(&at(2, 1, HttpEvent::ConnectionClosed))
                .is_none()
        );
    }

    #[tokio::test]
    async fn going_quiescent_with_a_request_in_flight_is_a_violation() {
        let mut check = EveryRequestReachesTerminalState::default();

        check.observe(&at(0, 1, request(None)));

        let violation = check
            .finish(&CheckContext::default())
            .await
            .expect("check runs")
            .expect("should fire");

        assert!(violation.detail.contains("unanswered"), "{violation}");
    }

    #[test]
    fn the_same_key_returning_the_same_response_is_correct() {
        let mut check = IdempotentRetryReturnsSameResponse::default();

        check.observe(&at(0, 1, request(Some("key-1"))));
        check.observe(&at(1, 1, response(200, r#"{"id":"ch_1"}"#)));
        check.observe(&at(2, 2, request(Some("key-1"))));

        assert!(
            check
                .observe(&at(3, 2, response(200, r#"{"id":"ch_1"}"#)))
                .is_none()
        );
    }

    #[test]
    fn the_same_key_returning_a_different_resource_is_a_violation() {
        let mut check = IdempotentRetryReturnsSameResponse::default();

        check.observe(&at(0, 1, request(Some("key-1"))));
        check.observe(&at(1, 1, response(200, r#"{"id":"ch_1"}"#)));
        check.observe(&at(2, 2, request(Some("key-1"))));

        let violation = check
            .observe(&at(3, 2, response(200, r#"{"id":"ch_2"}"#)))
            .expect("should fire");

        assert!(violation.detail.contains("key-1"), "{violation}");
    }

    #[test]
    fn a_request_without_a_key_is_not_tracked() {
        let mut check = IdempotentRetryReturnsSameResponse::default();

        check.observe(&at(0, 1, request(None)));

        assert!(check.observe(&at(1, 1, response(500, "boom"))).is_none());
    }
}

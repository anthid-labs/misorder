//! Postgres session semantics.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::event::{ConnectionId, Event, Observed, PostgresEvent};
use crate::invariant::{Invariant, Violation};

/// A connection that reported an error does not then commit.
///
/// Per connection, which is the only way this means anything: an error on one
/// pooled connection says nothing about a commit on another, and a check that
/// conflated them would fire on every healthy service that uses a pool.
///
/// The failure it catches is a service that swallows a `40001` serialization
/// failure, carries on issuing statements against a transaction the server has
/// already aborted, and commits. Postgres reports `25P02` for the statements,
/// and the commit silently becomes a rollback: the service believes it wrote
/// and nothing was written.
#[derive(Debug, Default)]
pub struct NoCommitAfterError {
    errored: HashMap<ConnectionId, String>,
}

#[async_trait]
impl Invariant for NoCommitAfterError {
    fn name(&self) -> &str {
        "no_commit_after_error"
    }

    fn describe(&self) -> &str {
        "a connection that reported an error does not then commit"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let connection = observed.connection?;

        match &observed.event {
            Event::Postgres(PostgresEvent::Error { code, message }) => {
                self.errored
                    .insert(connection, format!("{code}: {message}"));
                None
            }
            // Both clear the error: a rollback is the correct response to it,
            // and a new transaction on a reset connection starts clean.
            Event::Postgres(PostgresEvent::Rollback | PostgresEvent::Begin) => {
                self.errored.remove(&connection);
                None
            }
            Event::Postgres(PostgresEvent::Disconnected) => {
                self.errored.remove(&connection);
                None
            }
            Event::Postgres(PostgresEvent::Commit) => {
                let error = self.errored.get(&connection)?;

                Some(Violation {
                    invariant: self.name().to_string(),
                    detail: format!(
                        "{connection} committed after {error}; Postgres turns that commit into a \
                         rollback, so the write the service believes it made did not happen"
                    ),
                    at: observed.at,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(millis: u64, connection: u64, event: PostgresEvent) -> Observed {
        Observed::on(
            Duration::from_millis(millis),
            ConnectionId(connection),
            Event::Postgres(event),
        )
    }

    fn serialization_failure() -> PostgresEvent {
        PostgresEvent::Error {
            code: "40001".to_string(),
            message: "could not serialize access".to_string(),
        }
    }

    #[test]
    fn committing_after_an_error_is_a_violation() {
        let mut check = NoCommitAfterError::default();

        check.observe(&at(0, 1, PostgresEvent::Begin));
        check.observe(&at(1, 1, serialization_failure()));

        let violation = check
            .observe(&at(2, 1, PostgresEvent::Commit))
            .expect("should fire");

        assert!(violation.detail.contains("40001"), "{violation}");
    }

    #[test]
    fn an_error_on_another_connection_is_not_this_connections_problem() {
        let mut check = NoCommitAfterError::default();

        check.observe(&at(0, 1, serialization_failure()));

        assert!(
            check.observe(&at(1, 2, PostgresEvent::Commit)).is_none(),
            "a pool would fire on every run"
        );
    }

    #[test]
    fn rolling_back_clears_the_error() {
        let mut check = NoCommitAfterError::default();

        check.observe(&at(0, 1, serialization_failure()));
        check.observe(&at(1, 1, PostgresEvent::Rollback));
        check.observe(&at(2, 1, PostgresEvent::Begin));

        assert!(check.observe(&at(3, 1, PostgresEvent::Commit)).is_none());
    }
}

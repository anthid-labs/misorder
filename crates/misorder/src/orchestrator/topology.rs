//! Putting a started dependency into the shape the scenario asked for.
//!
//! Streams and consumers for NATS, migrations for Postgres. Applied after the
//! container is up and before the service under test starts, so the service
//! never observes a half-built topology.
//!
//! # This is also where invariants get their configuration
//!
//! Applying a stream emits
//! [`NatsEvent::ConsumerConfigured`](crate::event::NatsEvent::ConsumerConfigured),
//! which is how `max_deliver_respected` learns what the limit is and how
//! `consumer_filter_excludes_dead_letter` learns what the filter is. The
//! alternative, letting invariants read the scenario directly, would make them
//! check the configuration that was *asked for* rather than the one the server
//! actually has.

use crate::error::{Error, Result};
use crate::event::NatsEvent;
use crate::proxy::EventSink;
use crate::scenario::file::{Postgres, Stream};

/// The consumer name a stream gets when the scenario does not choose one.
pub fn default_consumer_name(stream: &str) -> String {
    format!("{stream}_WORKER")
}

/// The filter a consumer gets when the scenario does not choose one.
///
/// The stream's first subject, which is the obvious default and also the one
/// that reproduces the dead-letter loop: `ledger.>` matches `ledger.dead_letter`.
/// Defaulting to something narrower would hide the bug the built-in invariant
/// exists to find, which would be choosing a pleasant demo over a true one.
pub fn default_filter_subject(stream: &Stream) -> Option<&str> {
    stream.subjects.first().map(String::as_str)
}

/// Creates a stream and its consumer, and reports the resulting configuration.
pub async fn apply_stream(
    address: &str,
    stream: &Stream,
    events: &EventSink,
    at: std::time::Duration,
) -> Result<()> {
    let consumer = stream
        .consumer
        .clone()
        .unwrap_or_else(|| default_consumer_name(&stream.name));

    let filter_subject = stream
        .filter_subject
        .clone()
        .or_else(|| default_filter_subject(stream).map(str::to_string))
        .ok_or_else(|| Error::Scenario(format!("stream `{}` has no subjects", stream.name)))?;

    tracing::debug!(
        address,
        stream = %stream.name,
        %consumer,
        %filter_subject,
        "would create stream and consumer"
    );

    // Emitted before the failure below so the shape is visible: the invariants
    // learn the topology from this event and from nowhere else.
    events.emit_lifecycle(
        at,
        crate::event::Event::Nats(NatsEvent::ConsumerConfigured {
            consumer,
            filter_subject,
            max_deliver: stream.max_deliver,
            ack_wait: stream.ack_wait,
        }),
    );

    Err(Error::Unsupported(
        "creating JetStream streams is not implemented yet".to_string(),
    ))
}

/// Applies `.sql` files in filename order.
///
/// Filename order, stated rather than left to the filesystem: `readdir` returns
/// entries in whatever order the filesystem feels like, and a migration set
/// that applied in a different order on CI than on a laptop would produce a
/// difference nobody would think to look for.
pub async fn apply_migrations(url: &str, postgres: &Postgres) -> Result<()> {
    let Some(directory) = &postgres.migrations else {
        return Ok(());
    };

    tracing::debug!(
        url,
        directory = %directory.display(),
        "would apply migrations in filename order"
    );

    Err(Error::Unsupported(
        "applying migrations is not implemented yet".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream() -> Stream {
        Stream {
            name: "LEDGER".to_string(),
            subjects: vec!["ledger.>".to_string()],
            max_deliver: 5,
            ack_wait: std::time::Duration::from_secs(30),
            discard: crate::scenario::file::Discard::Old,
            max_bytes: None,
            consumer: None,
            filter_subject: None,
        }
    }

    #[test]
    fn a_consumer_is_named_after_its_stream() {
        assert_eq!(default_consumer_name("LEDGER"), "LEDGER_WORKER");
    }

    #[test]
    fn the_default_filter_is_the_first_subject() {
        assert_eq!(default_filter_subject(&stream()), Some("ledger.>"));
    }

    #[tokio::test]
    async fn applying_a_stream_reports_its_configuration_before_anything_else() {
        let (events, mut receiver) = EventSink::new();

        let _ = apply_stream("127.0.0.1:4222", &stream(), &events, Default::default()).await;

        let observed = receiver.recv().await.expect("configuration event");

        match observed.event {
            crate::event::Event::Nats(NatsEvent::ConsumerConfigured {
                consumer,
                filter_subject,
                max_deliver,
                ..
            }) => {
                assert_eq!(consumer, "LEDGER_WORKER");
                assert_eq!(filter_subject, "ledger.>");
                assert_eq!(max_deliver, 5);
            }
            other => panic!("expected ConsumerConfigured, got {other:?}"),
        }
    }
}

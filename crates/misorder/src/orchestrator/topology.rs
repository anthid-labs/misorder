//! Putting a started dependency into the shape the scenario asked for.
//!
//! Streams and consumers for NATS, migrations for Postgres. Applied after the
//! container is up and before the service under test starts, so the service
//! never observes a half-built topology.
//!
//! # This is also where invariants get their configuration
//!
//! Applying a stream emits
//! [`NatsEvent::ConsumerConfigured`],
//! which is how `max_deliver_respected` learns what the limit is and how
//! `consumer_filter_excludes_dead_letter` learns what the filter is. The
//! alternative, letting invariants read the scenario directly, would make them
//! check the configuration that was *asked for* rather than the one the server
//! actually has.

//! # Adapters are optional, and a missing one is not a silent one
//!
//! The client libraries are behind their protocol's feature, so an embedder
//! that needs one adapter does not link the rest. What is *not* behind a
//! feature is the scenario vocabulary: a scenario declaring a stream parses in
//! every build. So a build with the adapter left out has to answer a scenario
//! that wants one, and it answers with [`Error::Unsupported`] naming the
//! feature. Skipping the topology instead would let a scenario come all the
//! way up and publish at a stream nothing had created, which is the exact
//! failure `apply_topology` was extended to close.

#[cfg(feature = "nats")]
use async_nats::jetstream;
#[cfg(feature = "nats")]
use async_nats::jetstream::consumer::pull;
#[cfg(feature = "nats")]
use async_nats::jetstream::stream::DiscardPolicy;

use crate::error::{Error, Result};
#[cfg(feature = "nats")]
use crate::event::NatsEvent;
use crate::proxy::EventSink;
#[cfg(feature = "nats")]
use crate::scenario::file::Discard;
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
///
/// The configuration reported is the one the **server** ended up with, read
/// back after the consumer exists rather than echoed from the scenario. That is
/// the difference between `max_deliver_respected` checking what the server does
/// against what the server was told, and it checking the scenario against
/// itself.
#[cfg(feature = "nats")]
pub async fn apply_stream(
    address: &str,
    stream: &Stream,
    events: &EventSink,
    at: std::time::Duration,
) -> Result<()> {
    let name = stream
        .consumer
        .clone()
        .unwrap_or_else(|| default_consumer_name(&stream.name));

    let filter_subject = stream
        .filter_subject
        .clone()
        .or_else(|| default_filter_subject(stream).map(str::to_string))
        .ok_or_else(|| Error::Scenario(format!("stream `{}` has no subjects", stream.name)))?;

    let context = connect(address).await?;

    let config = jetstream::stream::Config {
        name: stream.name.clone(),
        subjects: stream.subjects.clone(),
        max_bytes: stream.max_bytes.map(|bytes| bytes as i64).unwrap_or(-1),
        discard: match stream.discard {
            Discard::Old => DiscardPolicy::Old,
            Discard::New => DiscardPolicy::New,
        },
        ..Default::default()
    };

    // Created, then updated if it is already there. Not `get_or_create_stream`,
    // which asks for the stream first and hands back whatever is running with
    // the requested config discarded: on a server where a previous run left the
    // stream behind, every setting in the scenario would be silently ignored
    // and the run would explore a topology nobody wrote down.
    if context.create_stream(config.clone()).await.is_err() {
        jetstream_update(&context, &config).await?;
    }

    let handle = context
        .get_stream(&stream.name)
        .await
        .map_err(|error| Error::Environment(format!("stream `{}`: {error}", stream.name)))?;

    // A durable pull consumer, because that is what the scenario's fields map
    // onto with nothing left to guess. A service that binds its own push
    // consumer is unaffected: this one holds its own ack floor and delivers to
    // nobody.
    let mut consumer = handle
        .create_consumer(pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: filter_subject.clone(),
            max_deliver: stream.max_deliver as i64,
            ack_wait: stream.ack_wait,
            ..Default::default()
        })
        .await
        .map_err(|error| Error::Environment(format!("consumer `{name}`: {error}")))?;

    let info = consumer
        .info()
        .await
        .map_err(|error| Error::Environment(format!("consumer `{name}`: {error}")))?;

    tracing::debug!(
        address,
        stream = %stream.name,
        consumer = %name,
        filter = %info.config.filter_subject,
        "created stream and consumer"
    );

    // Emitted from the server's answer. The invariants learn the topology from
    // this event and from nowhere else.
    events.emit_lifecycle(
        at,
        crate::event::Event::Nats(NatsEvent::ConsumerConfigured {
            consumer: info
                .config
                .durable_name
                .clone()
                .unwrap_or_else(|| name.clone()),
            filter_subject: info.config.filter_subject.clone(),
            // Negative means unlimited on the wire. Reported as the largest
            // count rather than clamped to zero, so `max_deliver_respected`
            // stays quiet on a stream that has no limit instead of firing on
            // every delivery.
            max_deliver: u32::try_from(info.config.max_deliver).unwrap_or(u32::MAX),
            ack_wait: info.config.ack_wait,
        }),
    );

    Ok(())
}

/// Connects and returns a JetStream context.
///
/// The answer a build without the `nats` feature gives a scenario that declares
/// a stream.
///
/// Named and shaped exactly like the real one, so the caller has no branch: the
/// decision is made once, here, by whether the adapter was compiled in.
#[cfg(not(feature = "nats"))]
pub async fn apply_stream(
    _address: &str,
    stream: &Stream,
    _events: &EventSink,
    _at: std::time::Duration,
) -> Result<()> {
    Err(Error::Unsupported(format!(
        "the scenario declares the stream `{}`, and this build of misorder has no nats \
         adapter in it. Rebuild with the `nats` feature.",
        stream.name
    )))
}

/// Straight to the dependency, never through a proxy. Topology is applied
/// before the service starts, so there is nothing for a fault to perturb and a
/// dropped connection here would be the harness failing rather than a run
/// finding something.
#[cfg(feature = "nats")]
pub async fn connect(address: &str) -> Result<jetstream::Context> {
    let client = async_nats::connect(address).await.map_err(|error| {
        Error::Environment(format!(
            "nats did not accept a connection on {address}: {error}"
        ))
    })?;

    Ok(jetstream::new(client))
}

#[cfg(feature = "nats")]
async fn jetstream_update(
    context: &jetstream::Context,
    config: &jetstream::stream::Config,
) -> Result<()> {
    context.update_stream(config).await.map_err(|error| {
        Error::Environment(format!(
            "stream `{}` exists and could not be updated to the scenario's configuration: {error}",
            config.name
        ))
    })?;

    Ok(())
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

    /// The configuration event describes what the server ended up with, so a
    /// server that was never reached produces no event at all.
    ///
    /// This is the property the emission order exists for. Reporting the
    /// scenario's own numbers here instead would hand `max_deliver_respected` a
    /// limit nothing is enforcing, and it would then pass every run against a
    /// stream that does not exist.
    #[cfg(feature = "nats")]
    #[tokio::test]
    async fn a_stream_that_cannot_be_reached_reports_nothing() {
        let (events, mut receiver) = EventSink::new();

        // A port nothing is listening on. Bound so a wrong address is an error
        // someone can read rather than a run that never returns.
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            apply_stream("127.0.0.1:1", &stream(), &events, Default::default()),
        )
        .await
        .expect("connecting to a closed port has to fail rather than hang")
        .expect_err("there is no server there");

        assert!(
            matches!(error, Error::Environment(_)),
            "an unreachable dependency is the environment's fault, not the scenario's: {error}"
        );

        assert!(
            receiver.try_recv().is_err(),
            "a consumer that was never created must not be reported as configured"
        );
    }
}

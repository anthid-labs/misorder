// Only in a build with the adapter, because the whole file drives it. The
// dev-dependency on `async-nats` is unconditional, but `misorder::proxy::nats`
// is not, and without this the crate fails to build its tests under any
// feature set that leaves the adapter out - which is the set an embedder
// taking one protocol uses.
#![cfg(feature = "nats")]
//! The NATS loop, against a real server.
//!
//! Skipped unless `MISORDER_TEST_NATS_URL` names one. Deliberately not
//! `NATS_URL`: on a developer machine that variable is very often a tunnel to a
//! production cluster, and a test suite that read the obvious name would create
//! streams there the first time somebody ran it without thinking about it.
//!
//! ```bash
//! docker run -d --rm -p 14222:4222 nats:2.10-alpine -js
//! MISORDER_TEST_NATS_URL=127.0.0.1:14222 cargo test -p misorder --test nats_live
//! ```
//!
//! What this proves that the unit tests cannot: a real `async-nats` client,
//! speaking real JetStream, reaches a real server through the adapter and does
//! not notice it is there. The unit tests drive hand-written frames, so they
//! would still pass against a codec that a real client refused to talk to.

use std::sync::Arc;
use std::time::Duration;

use misorder::event::{Event, NatsEvent};
use misorder::orchestrator::topology::apply_stream;
use misorder::proxy::nats::NatsAdapter;
use misorder::proxy::{Adapter, EventSink, ProxyContext};
use misorder::scenario::file::{Discard, Stream};
use misorder::schedule::{DecisionSource, Scheduler};
use misorder::trace::{Decision, DecisionPoint, PointKind, Recorder};
use tokio_util::sync::CancellationToken;

/// The server to test against, or `None` to skip.
fn upstream() -> Option<String> {
    std::env::var("MISORDER_TEST_NATS_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

/// Answers one chosen fork and stays neutral everywhere else.
struct At {
    kind: PointKind,
    ordinal: u64,
    decision: Decision,
}

impl DecisionSource for At {
    fn decide(&self, point: &DecisionPoint) -> Decision {
        if point.key.kind == self.kind && point.key.ordinal == self.ordinal {
            self.decision
        } else {
            Decision::NEUTRAL
        }
    }
}

fn neutral() -> Arc<dyn DecisionSource> {
    Arc::new(At {
        kind: PointKind::Statement,
        ordinal: u64::MAX,
        decision: Decision::NEUTRAL,
    })
}

/// A stream named for this test, so two runs against a shared server do not
/// read each other's messages.
fn stream(name: &str) -> Stream {
    Stream {
        name: name.to_string(),
        subjects: vec![format!("{name}.>")],
        max_deliver: 3,
        ack_wait: Duration::from_secs(2),
        discard: Discard::Old,
        max_bytes: Some(1024 * 1024),
        consumer: Some(format!("{name}_WORKER")),
        filter_subject: Some(format!("{name}.>")),
    }
}

/// Deletes the stream, so a run starts from nothing.
///
/// The property misorder warns about, applied to misorder's own tests: this
/// server is one somebody else started, so it is not reset between runs. Without
/// this the second run of a test fetches the message the first one left behind,
/// already on delivery two, and the assertion fails describing a bug that is not
/// there.
async fn reset(upstream: &str, name: &str) {
    let Ok(client) = async_nats::connect(upstream).await else {
        return;
    };

    // Absent is the expected case on a clean server, so a failure here is not
    // worth reporting: what matters is that it is gone afterwards.
    let _ = async_nats::jetstream::new(client).delete_stream(name).await;
}

struct Live {
    proxy: String,
    events: tokio::sync::mpsc::UnboundedReceiver<misorder::event::Observed>,
    cancel: CancellationToken,
    serving: tokio::task::JoinHandle<misorder::error::Result<()>>,
}

async fn start(upstream: &str, source: Arc<dyn DecisionSource>) -> Live {
    let mut adapter = NatsAdapter::new();
    let endpoint = adapter.bind(upstream).await.expect("bind the proxy");

    let (events, receiver) = EventSink::new();
    let cancel = CancellationToken::new();
    let scheduler = Scheduler::new(source, Recorder::new(0, "nats_live"));
    let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

    let serving = tokio::spawn(async move { adapter.serve(context).await });

    Live {
        proxy: format!("nats://{}", endpoint.listen),
        events: receiver,
        cancel,
        serving,
    }
}

impl Live {
    async fn drain(&mut self) -> Vec<Event> {
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut out = Vec::new();
        while let Ok(observed) = self.events.try_recv() {
            out.push(observed.event);
        }

        out
    }

    async fn stop(self) {
        self.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.serving).await;
    }
}

/// A real client, through the proxy, end to end.
///
/// The stream is created and its configuration read back, a message is
/// published straight to the server, and a pull consumer fetches and acks it
/// across the proxy. Every one of those steps is a place a wrong codec would
/// stop the client dead rather than merely produce a wrong event.
#[tokio::test]
async fn a_real_client_reaches_jetstream_through_the_adapter() {
    let Some(upstream) = upstream() else {
        eprintln!("skipped: set MISORDER_TEST_NATS_URL to run this");
        return;
    };

    reset(&upstream, "MISORDER_LIVE_OK").await;

    let declared = stream("MISORDER_LIVE_OK");
    let (events, mut configured) = EventSink::new();

    apply_stream(&upstream, &declared, &events, Duration::ZERO)
        .await
        .expect("the stream and its consumer are created");

    let reported = configured.try_recv().expect("a configuration event");

    assert!(
        matches!(
            reported.event,
            Event::Nats(NatsEvent::ConsumerConfigured { ref consumer, max_deliver, .. })
                if consumer == "MISORDER_LIVE_OK_WORKER" && max_deliver == 3
        ),
        "the server's own answer is what gets reported: {:?}",
        reported.event
    );

    // Published straight to the server: the workload driver stands in for the
    // vendor, so its own publish is not the traffic under test.
    let producer = async_nats::connect(&upstream)
        .await
        .expect("connect direct");
    async_nats::jetstream::new(producer)
        .publish("MISORDER_LIVE_OK.one", "hello".into())
        .await
        .expect("publish")
        .await
        .expect("the server stored it");

    let mut live = start(&upstream, neutral()).await;

    let client = async_nats::connect(&live.proxy)
        .await
        .expect("a real client has to be able to talk to the proxy");

    let consumer = async_nats::jetstream::new(client)
        .get_stream("MISORDER_LIVE_OK")
        .await
        .expect("stream")
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>("MISORDER_LIVE_OK_WORKER")
        .await
        .expect("consumer");

    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch across the proxy");

    let message = tokio::time::timeout(
        Duration::from_secs(10),
        futures::StreamExt::next(&mut batch),
    )
    .await
    .expect("the delivery has to cross the proxy")
    .expect("a message")
    .expect("a readable message");

    assert_eq!(message.payload, "hello");

    message.ack().await.expect("ack across the proxy");

    let observed = live.drain().await;

    assert!(
        observed.iter().any(|event| matches!(
            event,
            Event::Nats(NatsEvent::Delivered { subject, consumer, num_delivered, payload })
                if subject == "MISORDER_LIVE_OK.one"
                    && consumer == "MISORDER_LIVE_OK_WORKER"
                    && *num_delivered == 1
                    && payload == "hello"
        )),
        "the delivery has to be observed with the server's own delivery count: {observed:?}"
    );

    assert!(
        observed.iter().any(|event| matches!(
            event,
            Event::Nats(NatsEvent::Acked { consumer, subject })
                if consumer == "MISORDER_LIVE_OK_WORKER" && subject == "MISORDER_LIVE_OK.one"
        )),
        "the ack has to be correlated back to the subject it settles: {observed:?}"
    );

    live.stop().await;
}

/// A swallowed ack produces the server's own redelivery.
///
/// This is the whole point of the adapter in one test: nothing here forges a
/// redelivery, and `num_delivered` is the real server's count. A harness that
/// invented either would be checking its own bookkeeping.
#[tokio::test]
async fn a_swallowed_ack_makes_the_server_redeliver() {
    let Some(upstream) = upstream() else {
        eprintln!("skipped: set MISORDER_TEST_NATS_URL to run this");
        return;
    };

    reset(&upstream, "MISORDER_LIVE_REDELIVER").await;

    let declared = stream("MISORDER_LIVE_REDELIVER");
    let (events, _configured) = EventSink::new();

    apply_stream(&upstream, &declared, &events, Duration::ZERO)
        .await
        .expect("stream created");

    let producer = async_nats::connect(&upstream)
        .await
        .expect("connect direct");
    async_nats::jetstream::new(producer)
        .publish("MISORDER_LIVE_REDELIVER.one", "hello".into())
        .await
        .expect("publish")
        .await
        .expect("stored");

    // The first ack this connection sends is swallowed. The client believes it
    // acked; the server never hears it and redelivers once `ack_wait` expires.
    let live = start(
        &upstream,
        Arc::new(At {
            kind: PointKind::Ack,
            ordinal: 0,
            decision: Decision::Drop,
        }),
    )
    .await;

    let client = async_nats::connect(&live.proxy).await.expect("connect");
    let consumer = async_nats::jetstream::new(client)
        .get_stream("MISORDER_LIVE_REDELIVER")
        .await
        .expect("stream")
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(
            "MISORDER_LIVE_REDELIVER_WORKER",
        )
        .await
        .expect("consumer");

    let mut counts = Vec::new();

    // Two fetches: the first is delivery 1 and its ack is swallowed, the second
    // is the server redelivering after `ack_wait`.
    for _ in 0..2 {
        let mut batch = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(8))
            .messages()
            .await
            .expect("fetch");

        if let Ok(Some(Ok(message))) = tokio::time::timeout(
            Duration::from_secs(12),
            futures::StreamExt::next(&mut batch),
        )
        .await
        {
            counts.push(message.info().expect("jetstream metadata").delivered);
            let _ = message.ack().await;
        }
    }

    live.stop().await;

    assert_eq!(
        counts,
        vec![1, 2],
        "the swallowed ack has to produce the server's own second delivery, not a forged one"
    );
}

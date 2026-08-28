//! A consumer whose filter matches its own dead letter subject.
//!
//! The system under test for
//! [`examples/dead_letter_loop.toml`](../../../examples/dead_letter_loop.toml).
//! It exists to be run by misorder, not to be depended on.
//!
//! # What is wrong with it
//!
//! Nothing, read one piece at a time. The consumer takes `ledger.>`. A message
//! it cannot handle goes to `ledger.dead_letter` so a human can look at it
//! later. Both of those are the ordinary thing to write.
//!
//! `ledger.>` matches `ledger.dead_letter`. So the dead letter comes straight
//! back to the consumer that produced it, fails again, and is dead-lettered
//! again, forever.
//!
//! `max_deliver` does not stop it, and that is the part worth sitting with:
//! every republish is a **new message** with a fresh delivery count, so the
//! server's own limit is never reached. Every component is behaving exactly as
//! documented and the system does not stop.
//!
//! # Why no vendor is involved
//!
//! Not every ordering bug comes from someone else's system. This one is a
//! service's own subject layout, and it is invisible in code review because
//! the two lines that combine to cause it are in different files.
//!
//! # Configuration
//!
//! `NATS_URL`, which misorder sets to its proxy. Nothing else.

use std::time::Duration;

use futures::StreamExt;

/// The stream misorder created from the scenario.
const STREAM: &str = "LEDGER";

/// The durable misorder created from the scenario.
const CONSUMER: &str = "LEDGER_WORKER";

/// Where a message this worker cannot handle is sent.
///
/// Under `ledger.`, which is the whole bug: the consumer's own filter is
/// `ledger.>`, and nothing about either line looks wrong beside the other.
const DEAD_LETTER: &str = "ledger.dead_letter";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("NATS_URL").expect("misorder sets NATS_URL to its proxy");

    let client = async_nats::connect(&url).await?;
    let jetstream = async_nats::jetstream::new(client);

    let consumer = jetstream
        .get_stream(STREAM)
        .await?
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(CONSUMER)
        .await?;

    eprintln!("worker attached to {CONSUMER} on {STREAM} via {url}");

    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(2))
            .messages()
            .await?;

        let Some(Ok(message)) = batch.next().await else {
            continue;
        };

        // Handled, for a definition of handled that sends it somewhere else.
        // The payload is republished unchanged, which is what lets misorder see
        // the same message going round: the identity it tracks is the body.
        jetstream
            .publish(DEAD_LETTER.to_string(), message.payload.clone())
            .await?
            .await?;

        // Acked, correctly. The original is settled and the server is happy.
        // The copy that just went to `ledger.dead_letter` is a different
        // message with its own delivery count, and it is already on its way
        // back to this consumer.
        message.ack().await?;
    }
}

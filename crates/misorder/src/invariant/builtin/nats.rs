//! NATS and JetStream semantics.
//!
//! Everything here is checkable from the event stream alone, which is why these
//! are free to the user: the proxy already saw all of it.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::event::{Event, NatsEvent, Observed};
use crate::invariant::{Invariant, Violation};

/// NATS subject matching, including wildcards.
///
/// `*` matches exactly one token, `>` matches one or more and must be last.
/// Written out rather than delegated to a client library, because the built-in
/// that matters most depends on getting this exactly right: `ledger.>` matching
/// `ledger.dead_letter` *is* the dead-letter redelivery loop, and a matcher
/// that was merely approximately correct would miss it or invent it.
pub fn subject_matches(filter: &str, subject: &str) -> bool {
    let filter: Vec<&str> = filter.split('.').collect();
    let subject: Vec<&str> = subject.split('.').collect();

    for (index, token) in filter.iter().enumerate() {
        match *token {
            ">" => return index + 1 == filter.len() && subject.len() > index,
            "*" => {
                if index >= subject.len() {
                    return false;
                }
            }
            literal => {
                if subject.get(index) != Some(&literal) {
                    return false;
                }
            }
        }
    }

    filter.len() == subject.len()
}

/// A message is never delivered more times than the stream allows.
///
/// Checks the server's own `num_delivered` against the configured
/// `max_deliver`, rather than counting deliveries here. Counting here would
/// compare misorder's bookkeeping against misorder's bookkeeping and pass
/// whatever the server actually did.
#[derive(Debug, Default)]
pub struct MaxDeliverRespected {
    configured: HashMap<String, u32>,
}

#[async_trait]
impl Invariant for MaxDeliverRespected {
    fn name(&self) -> &str {
        "max_deliver_respected"
    }

    fn describe(&self) -> &str {
        "a message is never delivered more than the stream's max_deliver"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        match &observed.event {
            Event::Nats(NatsEvent::ConsumerConfigured {
                consumer,
                max_deliver,
                ..
            }) => {
                self.configured.insert(consumer.clone(), *max_deliver);
                None
            }
            Event::Nats(NatsEvent::Delivered {
                consumer,
                subject,
                num_delivered,
                ..
            }) => {
                let max = *self.configured.get(consumer)?;

                (*num_delivered > max).then(|| Violation {
                    invariant: self.name().to_string(),
                    detail: format!(
                        "{subject} reached consumer {consumer} on delivery {num_delivered}, \
                         but max_deliver is {max}"
                    ),
                    at: observed.at,
                })
            }
            _ => None,
        }
    }
}

/// An acknowledged message is not delivered again.
///
/// Identity is `(consumer, subject, payload)`. Not the subject alone: a second
/// message on the same subject is ordinary traffic, and flagging it would make
/// this invariant fire on every healthy scenario.
#[derive(Debug, Default)]
pub struct NoDeliveryAfterAck {
    acked: HashSet<(String, String, Bytes)>,
    last_delivered: HashMap<(String, String), Bytes>,
}

#[async_trait]
impl Invariant for NoDeliveryAfterAck {
    fn name(&self) -> &str {
        "no_delivery_after_ack"
    }

    fn describe(&self) -> &str {
        "an acknowledged message is not delivered again"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        match &observed.event {
            Event::Nats(NatsEvent::Delivered {
                consumer,
                subject,
                payload,
                ..
            }) => {
                let key = (consumer.clone(), subject.clone(), payload.clone());

                if self.acked.contains(&key) {
                    return Some(Violation {
                        invariant: self.name().to_string(),
                        detail: format!(
                            "{subject} was delivered to {consumer} again after being acked"
                        ),
                        at: observed.at,
                    });
                }

                self.last_delivered
                    .insert((consumer.clone(), subject.clone()), payload.clone());
                None
            }
            // An ack names the consumer and subject, not the payload, so the
            // payload comes from the delivery it is answering. That is the same
            // correspondence the server makes.
            Event::Nats(NatsEvent::Acked { consumer, subject }) => {
                let payload = self
                    .last_delivered
                    .get(&(consumer.clone(), subject.clone()))?
                    .clone();

                self.acked
                    .insert((consumer.clone(), subject.clone(), payload));
                None
            }
            _ => None,
        }
    }
}

/// The same payload does not keep coming back.
///
/// The failure this catches is not redelivery, which is correct behaviour, but
/// a *loop*: a service that republishes what it could not handle, onto a
/// subject its own consumer is subscribed to. `max_deliver` does not stop that,
/// because every republish is a new message with a fresh delivery count.
#[derive(Debug)]
pub struct NoInfiniteRedelivery {
    window: Duration,
    same_payload_max: usize,
    seen: HashMap<Bytes, Vec<Duration>>,
}

impl NoInfiniteRedelivery {
    pub fn new(window: Duration, same_payload_max: usize) -> Self {
        Self {
            window,
            same_payload_max,
            seen: HashMap::new(),
        }
    }
}

impl Default for NoInfiniteRedelivery {
    fn default() -> Self {
        Self::new(Duration::from_secs(300), 10)
    }
}

#[async_trait]
impl Invariant for NoInfiniteRedelivery {
    fn name(&self) -> &str {
        "no_infinite_redelivery"
    }

    fn describe(&self) -> &str {
        "the same payload does not recur past same_payload_max within window"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let (subject, payload) = match &observed.event {
            Event::Nats(NatsEvent::Delivered {
                subject, payload, ..
            }) => (subject, payload),
            _ => return None,
        };

        let cutoff = observed.at.saturating_sub(self.window);
        let times = self.seen.entry(payload.clone()).or_default();

        times.retain(|at| *at >= cutoff);
        times.push(observed.at);

        let seen = times.len();

        (seen > self.same_payload_max).then(|| Violation {
            invariant: self.name().to_string(),
            detail: format!(
                "the payload on {subject} was delivered {seen} times within {:?}, over the \
                 limit of {}",
                self.window, self.same_payload_max
            ),
            at: observed.at,
        })
    }
}

/// A consumer does not subscribe to its own dead-letter subject.
///
/// The dead-letter loop in one line: a consumer filtered on `ledger.>` receives
/// `ledger.dead_letter`, fails it again, and dead-letters it again. Every piece
/// is behaving correctly, which is why nobody catches it by reading the code.
#[derive(Debug, Default)]
pub struct ConsumerFilterExcludesDeadLetter {
    filters: HashMap<String, String>,
}

#[async_trait]
impl Invariant for ConsumerFilterExcludesDeadLetter {
    fn name(&self) -> &str {
        "consumer_filter_excludes_dead_letter"
    }

    fn describe(&self) -> &str {
        "a consumer's filter subject does not match its own dead-letter subject"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        match &observed.event {
            Event::Nats(NatsEvent::ConsumerConfigured {
                consumer,
                filter_subject,
                ..
            }) => {
                self.filters
                    .insert(consumer.clone(), filter_subject.clone());
                None
            }
            Event::Nats(NatsEvent::DeadLettered {
                subject,
                origin_subject,
            }) => {
                let (consumer, filter) = self
                    .filters
                    .iter()
                    .find(|(_, filter)| subject_matches(filter, subject))?;

                Some(Violation {
                    invariant: self.name().to_string(),
                    detail: format!(
                        "{origin_subject} was dead-lettered to {subject}, which consumer \
                         {consumer} matches with its own filter \"{filter}\""
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
    use crate::event::ConnectionId;

    fn at(millis: u64, event: NatsEvent) -> Observed {
        Observed::on(
            Duration::from_millis(millis),
            ConnectionId(1),
            Event::Nats(event),
        )
    }

    fn configured(consumer: &str, filter: &str, max_deliver: u32) -> NatsEvent {
        NatsEvent::ConsumerConfigured {
            consumer: consumer.to_string(),
            filter_subject: filter.to_string(),
            max_deliver,
            ack_wait: Duration::from_secs(30),
        }
    }

    fn delivered(subject: &str, consumer: &str, num_delivered: u32, payload: &str) -> NatsEvent {
        NatsEvent::Delivered {
            subject: subject.to_string(),
            consumer: consumer.to_string(),
            num_delivered,
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        }
    }

    #[test]
    fn wildcards_match_the_way_nats_does() {
        assert!(subject_matches("ledger.>", "ledger.dead_letter"));
        assert!(subject_matches("ledger.>", "ledger.a.b.c"));
        assert!(!subject_matches("ledger.>", "ledger"));
        assert!(subject_matches("a.*.c", "a.b.c"));
        assert!(!subject_matches("a.*.c", "a.b.d"));
        assert!(!subject_matches("a.*", "a.b.c"));
        assert!(subject_matches("a.b", "a.b"));
        assert!(!subject_matches("a.b", "a.b.c"));
    }

    #[test]
    fn delivering_past_max_deliver_is_a_violation() {
        let mut check = MaxDeliverRespected::default();

        assert!(
            check
                .observe(&at(0, configured("W", "ledger.>", 5)))
                .is_none()
        );
        assert!(
            check
                .observe(&at(1, delivered("ledger.o", "W", 5, "p")))
                .is_none()
        );

        let violation = check
            .observe(&at(2, delivered("ledger.o", "W", 6, "p")))
            .expect("should fire");

        assert!(violation.detail.contains("max_deliver is 5"), "{violation}");
    }

    #[test]
    fn a_second_message_on_a_subject_is_not_a_redelivery_after_ack() {
        let mut check = NoDeliveryAfterAck::default();

        check.observe(&at(0, delivered("ledger.o", "W", 1, "first")));
        check.observe(&at(
            1,
            NatsEvent::Acked {
                consumer: "W".to_string(),
                subject: "ledger.o".to_string(),
            },
        ));

        assert!(
            check
                .observe(&at(2, delivered("ledger.o", "W", 1, "second")))
                .is_none(),
            "a different payload on the same subject is ordinary traffic"
        );
    }

    #[test]
    fn redelivering_an_acked_payload_is_a_violation() {
        let mut check = NoDeliveryAfterAck::default();

        check.observe(&at(0, delivered("ledger.o", "W", 1, "p")));
        check.observe(&at(
            1,
            NatsEvent::Acked {
                consumer: "W".to_string(),
                subject: "ledger.o".to_string(),
            },
        ));

        assert!(
            check
                .observe(&at(2, delivered("ledger.o", "W", 2, "p")))
                .is_some()
        );
    }

    #[test]
    fn a_payload_recurring_past_the_limit_is_a_loop() {
        let mut check = NoInfiniteRedelivery::new(Duration::from_secs(300), 3);

        for millis in 0..3 {
            assert!(
                check
                    .observe(&at(millis, delivered("ledger.dl", "W", 1, "p")))
                    .is_none(),
                "at or below the limit is a retry, not a loop"
            );
        }

        let violation = check
            .observe(&at(4, delivered("ledger.dl", "W", 1, "p")))
            .expect("should fire");

        assert!(violation.detail.contains("4 times"), "{violation}");
    }

    #[test]
    fn recurrences_outside_the_window_do_not_count() {
        let mut check = NoInfiniteRedelivery::new(Duration::from_millis(10), 3);

        for millis in [0, 1, 2, 100, 101, 102] {
            assert!(
                check
                    .observe(&at(millis, delivered("s", "W", 1, "p")))
                    .is_none(),
                "three in one window, then three in another, is a retry each time"
            );
        }
    }

    #[test]
    fn a_consumer_subscribed_to_its_own_dead_letter_subject_is_a_violation() {
        let mut check = ConsumerFilterExcludesDeadLetter::default();

        check.observe(&at(0, configured("LEDGER_WORKER", "ledger.>", 5)));

        let violation = check
            .observe(&at(
                1,
                NatsEvent::DeadLettered {
                    subject: "ledger.dead_letter".to_string(),
                    origin_subject: "ledger.org.org_1.account.acct_1.order".to_string(),
                },
            ))
            .expect("should fire");

        assert!(violation.detail.contains("LEDGER_WORKER"), "{violation}");
        assert!(violation.detail.contains("ledger.>"), "{violation}");
    }

    #[test]
    fn a_dead_letter_subject_outside_the_filter_is_fine() {
        let mut check = ConsumerFilterExcludesDeadLetter::default();

        check.observe(&at(0, configured("LEDGER_WORKER", "ledger.order.>", 5)));

        assert!(
            check
                .observe(&at(
                    1,
                    NatsEvent::DeadLettered {
                        subject: "ledger.dead_letter".to_string(),
                        origin_subject: "ledger.order.new".to_string(),
                    },
                ))
                .is_none()
        );
    }
}

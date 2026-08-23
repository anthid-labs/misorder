//! One subscription's billing lifecycle, and the ordering that bills a
//! customer after they cancelled.
//!
//! ```text
//! cargo run -p misorder --example stripe_webhook_ordering
//! ```
//!
//! # The story
//!
//! A customer subscribes, misses a payment, pays, misses another, and cancels.
//! Stripe generates five events in that order and posts each one to your
//! `/webhooks/stripe` endpoint. Your handler applies them as they arrive. In
//! staging that works every time, because in staging they arrive in order.
//!
//! Then one delivery is late. In production that has two ordinary causes and
//! they are indistinguishable from inside the handler, and Stripe documents
//! both as certainties rather than caveats:
//!
//! - Stripe does not guarantee that events are delivered in the order they were
//!   generated. <https://docs.stripe.com/webhooks#event-ordering>
//! - A delivery that goes unanswered is retried with exponential backoff for up
//!   to three days, while everything generated behind it is delivered normally.
//!   <https://docs.stripe.com/webhooks#automatic-retries>
//!
//! Either way the handler sees a `customer.subscription.deleted`, marks the
//! subscription `canceled`, and then — seconds or days later — receives an
//! `invoice.payment_failed` that was generated *before* the cancellation. It
//! applies it, because nothing told it not to, and the subscription is
//! `past_due` again. The customer is now being dunned for a subscription they
//! cancelled.
//!
//! Nobody wrote a bug. Every line of that handler is correct for the ordering
//! it was written against.
//!
//! # What this example actually does
//!
//! Runs the whole loop the README describes, on one machine, in a few seconds:
//!
//! 1. Stands up the handler below on a loopback port. It is a real HTTP server
//!    and it is deliberately the naive implementation.
//! 2. Puts misorder's HTTP proxy in front of it. Every delivery now passes a
//!    fork where the schedule may pass it through, hold it, drop it, or let a
//!    later one overtake it.
//! 3. Sweeps seeds until an invariant breaks.
//! 4. Shrinks that failure to the decisions that actually caused it.
//! 5. Prints the reproducer, and the arrival order that produced it.
//!
//! # Two honest notes
//!
//! **Days are compressed into milliseconds.** What is explored here is the
//! *ordering* a late delivery produces, because that is the part the handler
//! gets wrong — not the wall-clock gap, which a run bounded by seconds cannot
//! express and which needs the virtual clock that is not built yet.
//!
//! That is also why `Redelivery` is permitted here but never turns out to be
//! required: dropping a delivery in a bounded run loses the event, where in
//! production Stripe would bring it back tomorrow. The fault that reproduces
//! the bug is `Reorder`, and it is the honest one — from the handler's side, an
//! event that arrives after the cancellation is the same event whether it was
//! reordered in flight or retried for three days. The corpus entry
//! `failed_delivery_retried_for_days` in `examples/corpus/stripe.toml` is the
//! behaviour this stands in for, and it is named there and deliberately not
//! claimed here.
//!
//! **The state is in memory.** [`examples/stripe_webhook_ordering.toml`] is the
//! same scenario against a real service and a real Postgres, and expresses the
//! invariants as SQL. This file exists so the loop can be watched without
//! Docker.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use misorder::proxy::http::HttpAdapter;
use misorder::proxy::{Adapter, EventSink, ProxyContext};
use misorder::schedule::{FaultKind, Profile, Scheduler};
use misorder::shrink::{Limits, Oracle, Report, shrink};
use misorder::trace::Trace;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// How many seeds to try before giving up on finding the ordering.
const SEEDS: u64 = 200;

// ---------------------------------------------------------------------------
// What Stripe generated
// ---------------------------------------------------------------------------

/// One webhook delivery, as Stripe would post it.
struct Delivery {
    /// The event id. This is the correct deduplication key, and the handler
    /// below does not use it.
    id: &'static str,
    kind: &'static str,
    /// Stripe's clock, and the only ordering information an event carries.
    /// There is no sequence number.
    created: i64,
    /// The subscription this concerns.
    subscription: &'static str,
    /// The invoice, for the two that have one.
    invoice: Option<&'static str>,
}

impl Delivery {
    fn body(&self) -> String {
        let object = match self.invoice {
            Some(invoice) => format!(
                r#"{{"id":"{invoice}","subscription":"{}","amount_due":4900}}"#,
                self.subscription
            ),
            None => format!(r#"{{"id":"{}","status":"active"}}"#, self.subscription),
        };

        format!(
            r#"{{"id":"{}","type":"{}","created":{},"data":{{"object":{object}}}}}"#,
            self.id, self.kind, self.created
        )
    }
}

/// The five events, in the order Stripe generated them.
///
/// Generation order is the one thing that is certain and the one thing the
/// handler never gets to see. What arrives, and in what order, is the
/// schedule's to decide — which is the whole point: writing them out of order
/// by hand tests one ordering, and the ordering that breaks you is not the one
/// you thought of.
fn subscription_lifecycle() -> Vec<Delivery> {
    vec![
        Delivery {
            id: "evt_1",
            kind: "customer.subscription.created",
            created: 1_760_000_000,
            subscription: "sub_1",
            invoice: None,
        },
        Delivery {
            id: "evt_2",
            kind: "invoice.payment_failed",
            created: 1_760_000_060,
            subscription: "sub_1",
            invoice: Some("in_1"),
        },
        Delivery {
            id: "evt_3",
            kind: "invoice.payment_succeeded",
            created: 1_760_000_120,
            subscription: "sub_1",
            invoice: Some("in_1"),
        },
        Delivery {
            id: "evt_4",
            kind: "invoice.payment_failed",
            created: 1_760_000_180,
            subscription: "sub_1",
            invoice: Some("in_2"),
        },
        Delivery {
            id: "evt_5",
            kind: "customer.subscription.deleted",
            created: 1_760_000_240,
            subscription: "sub_1",
            invoice: None,
        },
    ]
}

// ---------------------------------------------------------------------------
// The service under test
// ---------------------------------------------------------------------------

/// One applied event, in the order the handler actually saw it.
#[derive(Clone)]
struct Applied {
    event: String,
    kind: String,
    /// What the subscription's status became.
    status: String,
}

/// The naive billing handler.
///
/// Nothing here is written badly on purpose. It is written the way a handler is
/// written when the events arrive in order, which is how they arrive every time
/// you test it:
///
/// - it applies each event as it arrives, because that is what a handler does;
/// - it has no notion of a state it should not leave, because in generation
///   order it never needs one;
/// - it settles an invoice whenever it is told the invoice was paid.
#[derive(Default)]
struct Billing {
    status: HashMap<String, String>,
    /// Every event applied, in arrival order. Not part of the handler's logic;
    /// this is what makes the failure readable afterwards.
    applied: Vec<Applied>,
}

impl Billing {
    fn apply(&mut self, body: &str) {
        // The first `id` in the body is the event's own. Everything else is
        // read from inside `data.object`, because searching the whole body for
        // `id` would find the event again and quietly key every subscription on
        // `evt_1`.
        let event = field(body, "id").unwrap_or_default();
        let kind = field(body, "type").unwrap_or_default();
        let object = object_of(body);

        let subscription = field(object, "subscription")
            .or_else(|| field(object, "id"))
            .unwrap_or_default();

        let status = match kind.as_str() {
            "customer.subscription.created" => "incomplete",
            "customer.subscription.deleted" => "canceled",
            "invoice.payment_succeeded" => "active",
            "invoice.payment_failed" => "past_due",
            _ => return,
        };

        self.status.insert(subscription.clone(), status.to_string());

        self.applied.push(Applied {
            event,
            kind,
            status: status.to_string(),
        });
    }
}

/// Pulls a string field out of a flat-enough JSON body.
///
/// Deliberately crude. Parsing is not what this example is about, and a real
/// dependency here would be a dependency in the published crate.
fn field(body: &str, name: &str) -> Option<String> {
    let needle = format!("\"{name}\":\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;

    Some(rest[..end].to_string())
}

/// The body from `data.object` onward, so a field lookup cannot reach back up
/// into the envelope.
fn object_of(body: &str) -> &str {
    match body.find("\"object\":") {
        Some(at) => &body[at..],
        None => body,
    }
}

// ---------------------------------------------------------------------------
// The invariants
// ---------------------------------------------------------------------------

/// A cancelled subscription stays cancelled.
///
/// The one a person recognises without being told: once a customer has
/// cancelled, nothing that arrives later may put them back into a state that
/// gets them dunned.
fn terminal_state_is_final(billing: &Billing) -> Result<(), String> {
    let mut cancelled_by: Option<&str> = None;

    for applied in &billing.applied {
        if applied.status == "canceled" {
            cancelled_by = Some(&applied.event);
            continue;
        }

        if let Some(cancelling) = cancelled_by {
            return Err(format!(
                "{} left the subscription '{}' after {cancelling} had already cancelled it",
                applied.event, applied.status
            ));
        }
    }

    Ok(())
}

// `no_double_charge` is deliberately absent, and the absence is the point.
//
// Stripe delivers at least once, so the same `invoice.payment_succeeded` can
// arrive twice, and this handler settles on the object id rather than the event
// id — so it would settle the same invoice twice. That is a real bug and it is
// *not an ordering bug*: two deliveries of one event break this handler in
// generation order, with no faults enabled, which means a fuzzer is not what
// finds it and an invariant here would fire on the baseline.
//
// The version worth writing needs the interleaving a check-then-insert loses
// against a real database: two copies inside two transactions, each checking
// for a settlement row before the other has committed one. That needs
// `hold_statement` and a real Postgres, which is what
// `examples/stripe_webhook_ordering.toml` expresses and what this in-memory
// file cannot. Naming it here would add a line that reads like coverage and
// never fires, which is the exact failure the scenario format refuses
// everywhere else.

/// One property of a finished run, and the name it is reported under.
///
/// A plain function pointer rather than a closure so the shrinker's oracle can
/// hold one without a lifetime: shrinking re-runs the same property against
/// dozens of candidate traces, and it has to be the *same* property or the
/// search converges on a different bug than the one that was found.
type Invariant = fn(&Billing) -> Result<(), String>;

/// Every property this example checks. One, honestly, rather than two that
/// read like more.
const INVARIANTS: [(&str, Invariant); 1] = [("terminal_state_is_final", terminal_state_is_final)];

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// Serves the handler on a loopback port until the run ends.
async fn serve_billing(listener: TcpListener, billing: Arc<Mutex<Billing>>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };

        let billing = Arc::clone(&billing);

        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);

            loop {
                let mut line = String::new();

                if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }

                let mut length = 0usize;

                loop {
                    let mut header = String::new();

                    if read.read_line(&mut header).await.unwrap_or(0) == 0 {
                        return;
                    }

                    let header = header.trim_end();

                    if header.is_empty() {
                        break;
                    }

                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }

                let mut body = vec![0u8; length];

                if read.read_exact(&mut body).await.is_err() {
                    return;
                }

                billing
                    .lock()
                    .expect("billing state")
                    .apply(&String::from_utf8_lossy(&body));

                if write
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
    }
}

/// Posts every delivery, then stops sending.
///
/// The half-close at the end is the ingress contract misorder's HTTP adapter
/// documents, and it is load-bearing rather than tidy: a request the schedule
/// deferred is released when a later one overtakes it, or when the client stops
/// sending. Waiting for each response before sending the next would give a
/// reorder nothing to swap with, and this whole example would find nothing.
async fn deliver(proxy: SocketAddr, deliveries: &[Delivery]) {
    let Ok(stream) = TcpStream::connect(proxy).await else {
        return;
    };

    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    for delivery in deliveries {
        let body = delivery.body();

        let request = format!(
            "POST /webhooks/stripe HTTP/1.1\r\nHost: billing\r\ncontent-type: \
             application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );

        if write.write_all(request.as_bytes()).await.is_err() {
            return;
        }
    }

    let _ = write.shutdown().await;

    // Drained to EOF rather than counted. A delivery the schedule dropped is
    // never answered, so counting responses would hang on exactly the run this
    // example is looking for.
    let mut sink = Vec::new();
    let _ = read.read_to_end(&mut sink).await;
}

/// One complete run: proxy up, deliveries posted, handler state returned.
async fn run(scheduler: Scheduler) -> Billing {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the billing service");

    let upstream = listener
        .local_addr()
        .expect("the service has an address")
        .to_string();

    let billing = Arc::new(Mutex::new(Billing::default()));

    let serving_billing = tokio::spawn(serve_billing(listener, Arc::clone(&billing)));

    let mut adapter = HttpAdapter::new();

    let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

    let (events, _receiver) = EventSink::new();
    let cancel = CancellationToken::new();

    let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());
    let serving_proxy = tokio::spawn(async move { adapter.serve(context).await });

    deliver(endpoint.listen, &subscription_lifecycle()).await;

    cancel.cancel();
    let _ = serving_proxy.await;
    serving_billing.abort();

    let state = billing.lock().expect("billing state");

    Billing {
        status: state.status.clone(),
        applied: state.applied.clone(),
    }
}

/// The faults Stripe's own documentation describes, in misorder's vocabulary.
///
/// `reorder` and `delay` are "events arrive out of generation order". `drop` is
/// the delivery that went unanswered — the one Stripe will retry for days, and
/// the reason a late arrival exists at all.
fn faults() -> Vec<FaultKind> {
    vec![FaultKind::Reorder, FaultKind::Delay, FaultKind::Redelivery]
}

fn profile() -> Profile {
    Profile {
        // Higher than the default, because six deliveries is a short run and at
        // 0.15 most seeds perturb nothing at all.
        fault_probability: 0.4,
        max_delay: Duration::from_millis(60),
        ack_hold: Duration::from_secs(45),
    }
}

/// Re-runs a candidate trace and asks whether the *same* invariant still
/// breaks.
///
/// The same one, not "the run failed". An oracle that accepted any failure
/// would shrink toward whichever bug was easiest to reproduce, and the
/// reproducer would be for a different finding than the one reported.
struct BillingOracle {
    invariant: Invariant,
}

#[async_trait::async_trait]
impl Oracle for BillingOracle {
    async fn still_fails(&mut self, trace: &Trace) -> misorder::error::Result<bool> {
        let billing = run(Scheduler::replaying(trace)).await;

        Ok((self.invariant)(&billing).is_err())
    }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

fn print_arrival_order(billing: &Billing) {
    println!("  what the handler actually saw, in arrival order:\n");

    for (index, applied) in billing.applied.iter().enumerate() {
        println!(
            "    {}. {:<8} {:<32} -> {}",
            index + 1,
            applied.event,
            applied.kind,
            applied.status
        );
    }

    let generated: Vec<&str> = subscription_lifecycle().iter().map(|d| d.id).collect();
    let arrived: Vec<&str> = billing.applied.iter().map(|a| a.event.as_str()).collect();

    println!("\n    generated: {}", generated.join(" "));
    println!("    arrived:   {}", arrived.join(" "));
}

fn print_reproducer(name: &str, report: &Report) {
    println!(
        "\nMINIMAL REPRODUCER: {name}\nseed {}, {} of {} decisions ({} re-runs)\n",
        report.trace.seed, report.after, report.before, report.attempts
    );

    for (index, record) in report
        .trace
        .records
        .iter()
        .filter(|record| !record.decision.is_neutral())
        .enumerate()
    {
        println!(
            "  {}. [{:>6}ms] conn:{} {} ({})",
            index + 1,
            record.at.as_millis(),
            record.point.key.connection,
            record.decision,
            record.point.detail.as_deref().unwrap_or("")
        );
    }

    // The negative space. Naming the permitted faults that turned out not to be
    // needed stops a reader concluding the bug needs a dropped delivery when it
    // needs one that arrived late.
    let used: std::collections::BTreeSet<_> = report
        .trace
        .records
        .iter()
        .filter_map(|record| record.decision.fault_kind())
        .collect();

    let unused: Vec<String> = faults()
        .into_iter()
        .filter(|fault| !used.contains(fault))
        .map(|fault| fault.to_string())
        .collect();

    if !unused.is_empty() {
        println!("\n  Faults '{}' were not required.", unused.join("', '"));
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    println!(
        "\nStripe webhook ordering\n\
         =======================\n\n\
         Five events, one subscription, generated in this order:\n"
    );

    for delivery in subscription_lifecycle() {
        println!("  {:<8} {}", delivery.id, delivery.kind);
    }

    println!(
        "\nThe handler applies each event as it arrives. Sweeping {SEEDS} orderings \
         of the same five deliveries.\n"
    );

    // The honest baseline first. If the scenario fails with nothing perturbed,
    // the bug was never about ordering and everything below it means nothing.
    let clean = run(Scheduler::seeded(0, vec![], profile(), "stripe")).await;

    for (name, invariant) in INVARIANTS {
        if let Err(violation) = invariant(&clean) {
            println!("The unperturbed run already breaks {name}: {violation}");
            std::process::exit(1);
        }
    }

    println!("  baseline, delivered in order: the invariant holds.\n");

    for seed in 1..=SEEDS {
        // Held rather than passed inline: `Scheduler` shares its recorder with
        // its clones, so this is how the decisions the run made are read back
        // afterwards. That trace is what gets shrunk.
        let scheduler = Scheduler::seeded(seed, faults(), profile(), "stripe");
        let billing = run(scheduler.clone()).await;

        for (name, invariant) in INVARIANTS {
            let Err(violation) = invariant(&billing) else {
                continue;
            };

            println!("INVARIANT VIOLATED: {name}\nseed {seed}\n");
            println!("  {violation}\n");

            print_arrival_order(&billing);

            let mut oracle = BillingOracle { invariant };

            let report = shrink(
                &scheduler.trace(),
                &mut oracle,
                Limits { max_attempts: 200 },
            )
            .await
            .expect("shrinking a recorded trace");

            print_reproducer(name, &report);

            let minimal = run(Scheduler::replaying(&report.trace)).await;

            println!();
            print_arrival_order(&minimal);

            println!(
                "\nThat trace is the artifact. Committed, it replays in milliseconds on \
                 every pull request\nand either reproduces or does not.\n"
            );

            return;
        }
    }

    // Non-zero, so this cannot rot quietly. CI runs this example, and a build
    // where the naive handler stops being caught is either a regression in the
    // scheduler or a sweep that has become too small to reach the ordering.
    // Both are worth failing over.
    println!(
        "No ordering in {SEEDS} seeds broke an invariant. The handler below is the naive \
         one and should be caught, so this is a regression rather than good news."
    );

    std::process::exit(1);
}

//! A billing service that handles Stripe webhooks, and gets one thing wrong.
//!
//! The system under test for
//! [`examples/stripe_webhook_ordering.toml`](../../../examples/stripe_webhook_ordering.toml).
//! It exists to be run by misorder, not to be depended on.
//!
//! # What is wrong with it, and why it is not obvious
//!
//! Nothing here is written badly on purpose. It is written the way a webhook
//! handler is written when the events arrive in the order they were generated,
//! which is how they arrive every single time you test it:
//!
//! - it applies each event as it arrives, because that is what a handler does;
//! - it has no notion of a state it should not leave, because in generation
//!   order it never needs one.
//!
//! It does deduplicate, on the event id, which is exactly what Stripe's own
//! documentation tells you to do, and that is the point worth making. This
//! handler was written by someone who read the docs and did what they said. The
//! duplicate advice is a heading with a code sample; the ordering advice is one
//! sentence with nothing to copy, and it is the one that costs money.
//!
//! # The check endpoints
//!
//! `GET /checks/<name>` answers a question about the service's own final state
//! and returns the **bad** rows as a JSON array, empty when nothing is wrong.
//! That is the same shape a SQL invariant uses, and for the same reason: a
//! query that searches for the bad state needs no knowledge of how many rows a
//! correct run produces, so it stays a test of the service rather than of the
//! scenario.
//!
//! It is test-only surface, and it is on the service rather than in misorder
//! deliberately. Only the service knows what its own state means.
//!
//! # Configuration
//!
//! `PORT`, which misorder sets to a free port per run. Nothing else.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// One event, as the handler applied it.
#[derive(Clone)]
struct Applied {
    event: String,
    subscription: String,
    status: String,
}

#[derive(Default)]
struct Billing {
    /// Event ids already applied.
    ///
    /// The documented fix for at-least-once delivery, keyed on the event rather
    /// than on the object, which is the part people get wrong. It works: no
    /// ordering in this scenario settles an invoice twice.
    seen: std::collections::HashSet<String>,
    /// Current status per subscription.
    status: HashMap<String, String>,
    /// Every event applied, in arrival order.
    applied: Vec<Applied>,
    /// One entry per settlement written. An invoice here twice is a customer
    /// charged twice.
    settlements: Vec<String>,
}

impl Billing {
    /// The handler.
    fn apply(&mut self, body: &str) {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(body) else {
            return;
        };

        // From the envelope, and only the envelope. An earlier version of this
        // scanned the whole body for the first `id` and got the *object's*
        // instead, because a TOML payload serialises its keys alphabetically
        // and `data` sorts before `id`. Every subscription event then keyed on
        // `sub_1`, the cancellation looked like a duplicate of the creation,
        // and it was silently skipped. Worth leaving the story in: a
        // deduplication key read from the wrong level fails closed and quietly.
        let id = event["id"].as_str().unwrap_or_default().to_string();
        let kind = event["type"].as_str().unwrap_or_default().to_string();
        let object = &event["data"]["object"];

        // Stripe delivers at least once, so the same event id can arrive twice.
        // Skipping the second copy is the documented answer and it is correct.
        if !self.seen.insert(id.clone()) {
            return;
        }

        let subscription = object["subscription"]
            .as_str()
            .or_else(|| object["id"].as_str())
            .unwrap_or_default()
            .to_string();

        let status = match kind.as_str() {
            "customer.subscription.created" => "incomplete",
            "customer.subscription.deleted" => "canceled",
            "invoice.payment_succeeded" => {
                if let Some(invoice) = object["id"].as_str() {
                    self.settlements.push(invoice.to_string());
                }

                "active"
            }
            "invoice.payment_failed" => "past_due",
            _ => return,
        };

        // On stderr, which misorder lets through to the terminal. Seeing the
        // arrival order beside the generation order is most of what makes a
        // reordering failure click.
        eprintln!("  applied {id:<8} {kind:<32} -> {status}");

        self.status.insert(subscription.clone(), status.to_string());

        self.applied.push(Applied {
            event: id,
            subscription,
            status: status.to_string(),
        });
    }

    /// Subscriptions that left a terminal state after reaching it.
    ///
    /// A cancelled customer being put back into `past_due` is a customer who
    /// gets dunned for a subscription they cancelled.
    fn reopened_after_cancel(&self) -> Vec<serde_json::Value> {
        let mut cancelled: Option<&Applied> = None;
        let mut rows = Vec::new();

        for applied in &self.applied {
            if applied.status == "canceled" {
                cancelled = Some(applied);
                continue;
            }

            if let Some(cancelling) = cancelled {
                rows.push(serde_json::json!({
                    "subscription": applied.subscription,
                    "cancelled_by": cancelling.event,
                    "reopened_by": applied.event,
                    "left_in": applied.status,
                }));
            }
        }

        rows
    }

    /// Invoices settled more than once.
    fn double_settled(&self) -> Vec<serde_json::Value> {
        let mut counts: HashMap<&str, usize> = HashMap::new();

        for invoice in &self.settlements {
            *counts.entry(invoice.as_str()).or_default() += 1;
        }

        let mut rows: Vec<_> = counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(invoice, count)| serde_json::json!({ "invoice": invoice, "settled": count }))
            .collect();

        // Sorted so two runs that found the same thing print the same thing.
        rows.sort_by_key(|row| row["invoice"].as_str().unwrap_or_default().to_string());

        rows
    }
}

#[tokio::main]
async fn main() {
    // Set by misorder, which picks a free port per run so that
    // `mis fuzz --parallel 16` can start sixteen of these at once.
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap_or_else(|error| panic!("billing_demo could not bind port {port}: {error}"));

    eprintln!("billing_demo listening on 127.0.0.1:{port}");

    let billing = Arc::new(Mutex::new(Billing::default()));

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };

        tokio::spawn(serve(stream, Arc::clone(&billing)));
    }
}

async fn serve(stream: TcpStream, billing: Arc<Mutex<Billing>>) {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    loop {
        let mut line = String::new();

        if read.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }

        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();

        let mut length = 0usize;
        let mut close = false;

        loop {
            let mut header = String::new();

            if read.read_line(&mut header).await.unwrap_or(0) == 0 {
                return;
            }

            let header = header.trim_end();

            if header.is_empty() {
                break;
            }

            let Some((name, value)) = header.split_once(':') else {
                continue;
            };

            match name.to_ascii_lowercase().as_str() {
                "content-length" => length = value.trim().parse().unwrap_or(0),
                "connection" => close = value.trim().eq_ignore_ascii_case("close"),
                _ => {}
            }
        }

        if length > 0 {
            let mut body = vec![0u8; length];

            if read.read_exact(&mut body).await.is_err() {
                return;
            }

            if method == "POST" {
                billing
                    .lock()
                    .expect("billing state")
                    .apply(&String::from_utf8_lossy(&body));
            }
        }

        let (status, body) = route(&billing, &method, &path);

        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: \
             {}\r\nconnection: {}\r\n\r\n{body}",
            body.len(),
            if close { "close" } else { "keep-alive" }
        );

        if write.write_all(response.as_bytes()).await.is_err() || close {
            return;
        }
    }
}

fn route(billing: &Mutex<Billing>, method: &str, path: &str) -> (&'static str, String) {
    let state = billing.lock().expect("billing state");

    let rows = match (method, path) {
        ("POST", "/webhooks/stripe") => return ("200 OK", r#"{"received":true}"#.to_string()),
        ("GET", "/checks/reopened_after_cancel") => state.reopened_after_cancel(),
        ("GET", "/checks/double_settled") => state.double_settled(),
        ("GET", "/health") => return ("200 OK", r#"{"ok":true}"#.to_string()),
        _ => {
            return (
                "404 Not Found",
                format!(r#"{{"error":"no route for {method} {path}"}}"#),
            );
        }
    };

    (
        "200 OK",
        serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()),
    )
}

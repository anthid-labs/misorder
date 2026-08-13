//! Shrinking, end to end through the trace format.
//!
//! The unit tests in `shrink` cover the search. This covers the round trip that
//! makes a reproducer an artifact rather than a value in memory: shrink, write
//! the file, read it back somewhere else, and get the same failure.

use std::time::Duration;

use async_trait::async_trait;
use misorder::error::Result;
use misorder::event::ConnectionId;
use misorder::report::Reproducer;
use misorder::schedule::FaultKind;
use misorder::shrink::{self, Oracle};
use misorder::trace::{Decision, DecisionPoint, PointKey, PointKind, Record, Trace};

/// Fails while the decisions at `required` are still active.
struct NeedsAllOf {
    required: Vec<PointKey>,
}

#[async_trait]
impl Oracle for NeedsAllOf {
    async fn still_fails(&mut self, trace: &Trace) -> Result<bool> {
        let active: Vec<PointKey> = trace.active().map(|record| record.point.key).collect();

        Ok(self.required.iter().all(|key| active.contains(key)))
    }
}

fn key(ordinal: u64) -> PointKey {
    PointKey {
        kind: PointKind::Ack,
        connection: 1,
        ordinal,
    }
}

fn failing_trace(count: u64) -> Trace {
    let mut trace = Trace::new(8_837_291, "dead_letter_no_redelivery");

    for ordinal in 0..count {
        trace.records.push(Record {
            seq: ordinal,
            at: Duration::from_millis(ordinal * 7),
            point: DecisionPoint::new(PointKind::Ack, ConnectionId(1), ordinal)
                .with_detail("ledger.org.org_1.account.acct_1.order"),
            decision: Decision::Drop,
        });
    }

    trace
}

#[tokio::test]
async fn a_shrunk_trace_survives_a_round_trip_through_a_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("repro.jsonl");

    let mut oracle = NeedsAllOf {
        required: vec![key(3), key(17), key(42)],
    };

    let report = shrink::shrink(&failing_trace(847), &mut oracle, shrink::Limits::default())
        .await
        .expect("shrink");

    assert_eq!(report.after, 3);

    report.trace.save(&path).expect("save");
    let reloaded = Trace::load(&path).expect("load");

    assert_eq!(reloaded, report.trace);
    assert!(
        oracle.still_fails(&reloaded).await.expect("oracle"),
        "a reproducer that stops reproducing after a save is not an artifact"
    );
}

#[tokio::test]
async fn shrinking_a_shrunk_trace_changes_nothing() {
    let mut oracle = NeedsAllOf {
        required: vec![key(3), key(17)],
    };

    let once = shrink::shrink(&failing_trace(200), &mut oracle, shrink::Limits::default())
        .await
        .expect("first pass");

    let twice = shrink::shrink(&once.trace, &mut oracle, shrink::Limits::default())
        .await
        .expect("second pass");

    assert_eq!(
        once.after, twice.after,
        "the result of shrinking has to be a fixed point, or committing one is pointless"
    );
}

#[tokio::test]
async fn a_shrunk_trace_renders_a_reproducer_naming_what_was_not_needed() {
    let mut oracle = NeedsAllOf {
        required: vec![key(9)],
    };

    let report = shrink::shrink(&failing_trace(64), &mut oracle, shrink::Limits::default())
        .await
        .expect("shrink");

    let reproducer = Reproducer::build(
        &report.trace,
        misorder::invariant::Violation {
            invariant: "no_infinite_redelivery".to_string(),
            detail: "the payload on ledger.dead_letter was delivered 11 times".to_string(),
            at: Duration::from_millis(900),
        },
        &[],
        &["nats", "postgres"],
        &[
            FaultKind::SwallowAck,
            FaultKind::Reorder,
            FaultKind::ConnectionDrop,
        ],
        report.before,
    );

    let rendered = reproducer.render();

    assert!(rendered.contains("1 of 64 decisions"), "{rendered}");
    assert!(rendered.contains("no_infinite_redelivery"), "{rendered}");
    assert!(
        rendered.contains("'reorder' and 'connection_drop' were not required"),
        "{rendered}"
    );
}

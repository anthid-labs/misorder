# Ledger import

**Status: specified, not implemented.** Nothing in this repository reads the
files described here. This document is what gets built, and it is here rather
than in a branch because the one decision that matters is a boundary decision,
and boundary decisions are cheap now and expensive later.

The worked example is Stripe, because a Stripe integration is the case where the
whole thing is already sitting there waiting to be read.

## Why read a table at all

The corpus is the asset. You cannot theorise that a vendor delivers an event
twice, you record it. Recording is Phase 2 and it needs something in the traffic
path, which is a change to the thing being measured and a conversation with
whoever owns it.

Meanwhile the integration already wrote it all down. Most Stripe integrations
have a table like `stripe_webhook_events`: the raw payload, plus when it
arrived. Months of the vendor's real behaviour, including every duplicate, every
reorder and every delay, already stored, already scrubbed of nothing because it
never left. Reading it is extraction. It needs no recorder, no proxy, and no
permission.

## The mapping

Five fields, and everything after them is extraction.

```toml
[[source.ledger]]
vendor = "stripe"
table  = "stripe_webhook_events"

[source.ledger.map]
event_id    = "stripe_event_id"        # evt_xxx, for duplicate detection
event_type  = "type"                   # invoice.payment_failed
entity_id   = "payload->'data'->'object'->>'id'"
vendor_time = "payload->>'created'"    # Stripe's clock
local_time  = "received_at"            # your clock
sequence    = null                     # Stripe has none, and that is a fact
```

**The two clocks are the whole trick.** `created` is when the vendor says it
happened. `received_at` is when you got it. Every delay, every reorder and every
duplicate is visible in the gap between them, and nothing can be extracted
without both. A ledger with one clock measures nothing: with only the local
clock you see the order you stored things in, which is the order you stored
things in.

**`sequence = null` is load-bearing.** Stripe gives no ordering token at all.
That is not a missing configuration value, it is a property of the vendor worth
recording, because it means order can only ever be inferred from a clock and is
therefore never certain. A vendor that does have a sequence gets a different and
much stronger extraction, and the difference between the two should be visible
in the mapping rather than buried in a per-vendor code path.

## The lifecycle

Extraction can find that two events arrived in an odd order. It cannot know that
one of those orders is illegal without being told which states are terminal.

```toml
[[lifecycle]]
entity     = "subscription"
id_pattern = "sub_*"
states     = ["incomplete", "active", "past_due", "canceled"]
terminal   = ["canceled"]

[lifecycle.transitions]
"customer.subscription.created" = "incomplete"
"invoice.payment_succeeded"     = "active"
"invoice.payment_failed"        = "past_due"
"customer.subscription.deleted" = "canceled"
```

This is the user's own domain, the same way an invariant is, and for the same
reason: the protocol knows an event arrived, and only you know that a canceled
subscription is finished.

## What extraction produces

Corpus entries, with a frequency attached to each.

```
$ mis import ledger.toml --out ./corpus

stripe: 89,431 entities over 2,914,022 events

  same_event_delivered_more_than_once      1,204 occurrences   f = 0.013
  reorder_within_one_entity                                    f = 0.004
    largest inversion 9.2s (charge.succeeded after
    payment_intent.succeeded)
  delivery_delay                           p50 0.8s  p99 14s  max 3h11m
  terminal_before_intermediate                                 f = 0.0002
    customer.subscription.deleted before the final
    invoice.payment_failed

wrote ./corpus/stripe.toml
```

Those numbers are the shape of the output, not a measurement. A real one comes
from a real table.

The last line is the point of the exercise. Two in ten thousand is invisible in
staging, unreachable by anyone writing test cases by hand, and certain in
production at volume. Once it is a corpus entry it is a scenario, and the
scenario runs on every pull request.

### The frequency is not in the corpus schema yet

`corpus::BehaviorFlag` has `name`, `protocol`, `describe` and `provenance`, and
this change does not add to it. Two reasons, and the second is the real one.

The corpus format is a compatibility surface with a long life: an entry
contributed today is read by a build shipped in two years, `deny_unknown_fields`
throughout, and a reader refuses a version from the future rather than guessing.
Fields get added when something writes them.

And the shape is genuinely not settled. A frequency is a property of a
measurement, not of a vendor: two teams measure different numbers for the same
behaviour and both are right, because they have different traffic. So the open
question is whether the frequency belongs on the behaviour at all, or whether
the behaviour stays a claim about the vendor and a separate measurement document
carries the number, the window it covers and the population it was taken over. A
number with no window attached is not a claim. Inventing the field before the
extractor exists is how a wrong shape becomes permanent, which is exactly why
the transcript body format is not specified yet either.

`examples/corpus/stripe.toml` is what can be written honestly today: four
behaviours Stripe documents, with no frequencies, and a note saying which two
entries are missing because nobody can write them without measuring.

## Where it plugs in

**The importer emits the same normalized event stream the wire recorder does,
and extraction never learns which one it came from.**

```
  stripe_webhook_events export --+
                                 |
                                 +--> normalized events --> extraction --> corpus
                                 |
  wire recording ----------------+
```

This is the boundary decision, and it is the reason this document exists before
the code does. If extraction reads rows, adding the recorder later means
rewriting extraction. If extraction reads frames, the importer has to fake
frames it never saw. Either way the second path costs what the first one cost.
With a normalized event in the middle, the second path is a reader.

The normalized event is small, because it is the intersection of what a table
row and a wire frame can both honestly supply: vendor, event id, event type,
entity id, vendor time, local time, and an optional sequence. Anything richer
would be something one path can produce and the other cannot, which is the same
mistake in a smaller font.

## The two things that are genuinely hard

### Entity resolution

`data.object.id` gives you the immediate object. A `payment_intent`, a `charge`,
an `invoice` and a `subscription` are four ids on one customer's lifecycle, and
without stitching them every event is a singleton, every entity has one event,
and no ordering bug is detectable at all. Extraction would run cleanly over
three million rows and report nothing.

So it needs a per-vendor resolver. For Stripe that is walking `invoice` to
`subscription`, and `charge` to `payment_intent` to `invoice`. That is per-vendor
work, and per-vendor work is the business.

The resolver is open source and lives beside the adapter it belongs to, for the
reason adapters are never paywalled: the long tail of vendors is only ever
covered by the people who needed one, and a licence boundary there ends the
contributions that are the only way it gets covered.

### You cannot see what you dropped

The table contains the events you successfully stored. If the endpoint returned
500 and the vendor gave up after its retry window, there is no row. Ledger
import structurally cannot detect a missed event, and missed events are the most
damaging class there is.

There is a clean fix for Stripe and it generalizes: the event list endpoint
returns what Stripe actually generated, with 30 day retention. Diff it against
the table and the gap **is** the drop set. Any vendor with an event list
endpoint supports the same trick, and it turns "I think we got everything" into
a number.

**The engine does not make that call.** No network client, no credentials, no
account: the only sockets this process opens are the Docker daemon, the
dependencies it started, and the service under test. That is a sales
requirement rather than a preference, and it survives only by staying literally
true, so the fetch is the user's, run with the user's own credentials by the
user's own tooling, and what `mis import` reads is the export. The diff is in
scope. The fetch is not. The split costs one command and buys the sentence that
gets the tool through a security review.

The same rule decides how the ledger itself is read: `mis import` reads an
export of `stripe_webhook_events`, not the table. A production replica is not
the Docker daemon, not a dependency the harness started, and not the service
under test, and reading one needs production credentials in a process whose
whole claim is that it has none. `table = "stripe_webhook_events"` in the
mapping names where the export came from, which is provenance rather than a
connection string. If a direct read is ever wanted it is an explicit decision
for whoever owns that claim, not something to arrive by accident inside a
feature.

## Why the mapping is not a scenario key

`[[source.ledger]]` and `[[lifecycle]]` are not scenario keys, and a scenario
file containing them is refused today by `deny_unknown_fields`, correctly.

They live in the importer's own file because the scenario is what the generator
emits and this is the generator's input. A scenario is committed to a user's
repository and run in CI months later; a reproducer that carried a mapping to a
database table would be carrying something it never uses. Keeping them apart
also means the importer's output is two formats that already exist, corpus TOML
and scenario TOML, so the coupling stays what `INTERFACES.md` says all coupling
here is: file formats and process boundaries.

## What gets built, in order

1. The normalized event, and a reader for one export format. No extraction.
2. Duplicate detection and the delay distribution. Both need only the event id
   and the two clocks, and neither needs entity resolution.
3. Entity resolution for Stripe, and with it reorder within one entity.
4. The lifecycle file, and with it `terminal_before_intermediate`.
5. Corpus emission, and with it whatever the frequency question resolves to.
6. The reconciliation diff against an exported event list.

Steps 1 and 2 are worth shipping alone. A measured number for how often your
endpoint sees the same event twice, and a real distribution for how late a
delivery gets, out of data you already have and with nothing added to the
traffic path.

## One thing a frequency must never become

A behaviour measured at 0.0002 is not a behaviour to apply 0.02% of the time.

The point of a seeded sweep is to explore an ordering deliberately, not to
reproduce production's odds. A scheduler that sampled faults from measured
frequencies would be hoping again, and hoping at production's rate means the two
in ten thousand case needs the same volume to surface locally that it needed to
surface in production, which is the entire problem this tool exists to remove.

The frequency is for deciding which behaviours are worth writing a scenario for,
and for saying how much traffic stands behind an entry in a report. It does not
reach the scheduler.

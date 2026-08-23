//! Assertions that ship with each adapter.
//!
//! Zero user input. They encode the semantics of the dependency itself, so a
//! scenario naming one gets a real check without the user knowing anything
//! about how the dependency is supposed to behave. That is the point: the
//! first thing a new user should experience is a caught bug, not a tutorial.
//!
//! ```toml
//! [[invariants]]
//! builtin = "no_infinite_redelivery"
//! window = "5m"
//! same_payload_max = 10
//! ```
//!
//! # Planned entries are listed, not hidden
//!
//! [`REGISTRY`] carries invariants that are specified but not yet implemented,
//! and [`build`] refuses them with a plain message. Listing them is deliberate:
//! `mis check` then shows a user exactly what coverage exists today, instead of
//! reporting an unimplemented invariant as an unknown name and leaving them to
//! guess whether they typed it wrong.

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
pub mod universal;

use crate::error::{Error, Result};
use crate::invariant::Invariant;
use crate::scenario::file::InvariantSpec;

/// Whether an invariant is available or only specified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Implemented,
    /// Specified, and refused by [`build`] until it is written.
    Planned,
}

/// One entry in the catalogue.
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub name: &'static str,
    /// The dependency whose semantics it encodes, or `"any"`.
    pub dependency: &'static str,
    pub describe: &'static str,
    pub status: Status,
}

/// Every built-in, implemented or not.
pub const REGISTRY: &[Entry] = &[
    Entry {
        name: "max_deliver_respected",
        dependency: "nats",
        describe: "a message is never delivered more than the stream's max_deliver",
        status: Status::Implemented,
    },
    Entry {
        name: "no_delivery_after_ack",
        dependency: "nats",
        describe: "an acknowledged message is not delivered again",
        status: Status::Implemented,
    },
    Entry {
        name: "no_infinite_redelivery",
        dependency: "nats",
        describe: "the same payload does not recur past same_payload_max within window",
        status: Status::Implemented,
    },
    Entry {
        name: "consumer_filter_excludes_dead_letter",
        dependency: "nats",
        describe: "a consumer's filter subject does not match its own dead-letter subject",
        status: Status::Implemented,
    },
    Entry {
        name: "no_commit_after_error",
        dependency: "postgres",
        describe: "a connection that reported an error does not then commit",
        status: Status::Implemented,
    },
    Entry {
        name: "no_query_outside_transaction",
        dependency: "postgres",
        describe: "no query is issued outside a transaction that claimed one",
        // Needs the pooler-visible session identity, which the Phase 1 adapter
        // does not yet surface: without it, a statement on a second pooled
        // connection is indistinguishable from one inside the transaction.
        status: Status::Planned,
    },
    Entry {
        name: "set_local_role_survives_pooler",
        dependency: "postgres",
        describe: "SET LOCAL ROLE is still in effect for the statements that follow it",
        status: Status::Planned,
    },
    Entry {
        name: "every_command_gets_a_reply",
        dependency: "redis",
        describe: "every command that reached the server got a reply",
        status: Status::Implemented,
    },
    Entry {
        name: "lock_released_by_owner",
        dependency: "redis",
        describe: "a key taken with SET NX is not deleted by a client that no longer holds it",
        status: Status::Implemented,
    },
    Entry {
        name: "every_request_reaches_terminal_state",
        dependency: "http",
        describe: "every accepted request gets a response or an explicit failure",
        status: Status::Implemented,
    },
    Entry {
        name: "idempotent_retry_returns_same_response",
        dependency: "http",
        describe: "a retried idempotency key returns the response the first attempt got",
        status: Status::Implemented,
    },
    Entry {
        name: "eventually_quiescent",
        dependency: "any",
        describe: "the system stops doing work once the workload is done",
        status: Status::Implemented,
    },
];

/// Every name, for error messages and for `mis check`.
pub fn names() -> Vec<&'static str> {
    REGISTRY.iter().map(|entry| entry.name).collect()
}

pub fn is_known(name: &str) -> bool {
    REGISTRY.iter().any(|entry| entry.name == name)
}

pub fn entry(name: &str) -> Option<&'static Entry> {
    REGISTRY.iter().find(|entry| entry.name == name)
}

/// Constructs a built-in from its scenario block.
pub fn build(spec: &InvariantSpec) -> Result<Box<dyn Invariant>> {
    let name = spec
        .builtin
        .as_deref()
        .ok_or_else(|| Error::Internal("build called without a `builtin` key".to_string()))?;

    let entry = entry(name).ok_or_else(|| {
        Error::Scenario(format!(
            "unknown builtin invariant `{name}`; available: {}",
            names().join(", ")
        ))
    })?;

    if entry.status == Status::Planned {
        return Err(Error::Unsupported(format!(
            "builtin invariant `{name}` is specified but not implemented yet"
        )));
    }

    match name {
        #[cfg(feature = "nats")]
        "max_deliver_respected" => Ok(Box::new(nats::MaxDeliverRespected::default())),
        #[cfg(feature = "nats")]
        "no_delivery_after_ack" => Ok(Box::new(nats::NoDeliveryAfterAck::default())),
        #[cfg(feature = "nats")]
        "no_infinite_redelivery" => Ok(Box::new(nats::NoInfiniteRedelivery::new(
            spec.window.unwrap_or(std::time::Duration::from_secs(300)),
            spec.same_payload_max.unwrap_or(10),
        ))),
        #[cfg(feature = "nats")]
        "consumer_filter_excludes_dead_letter" => {
            Ok(Box::new(nats::ConsumerFilterExcludesDeadLetter::default()))
        }
        #[cfg(feature = "postgres")]
        "no_commit_after_error" => Ok(Box::new(postgres::NoCommitAfterError::default())),
        #[cfg(feature = "redis")]
        "every_command_gets_a_reply" => Ok(Box::new(redis::EveryCommandGetsAReply::default())),
        #[cfg(feature = "redis")]
        "lock_released_by_owner" => Ok(Box::new(redis::LockReleasedByOwner::default())),
        #[cfg(feature = "http")]
        "every_request_reaches_terminal_state" => {
            Ok(Box::new(http::EveryRequestReachesTerminalState::default()))
        }
        #[cfg(feature = "http")]
        "idempotent_retry_returns_same_response" => {
            Ok(Box::new(http::IdempotentRetryReturnsSameResponse::default()))
        }
        "eventually_quiescent" => Ok(Box::new(universal::EventuallyQuiescent::default())),
        other => Err(Error::Unsupported(format!(
            "builtin invariant `{other}` needs a feature this build was compiled without"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_name_is_unique() {
        let mut names = names();
        let count = names.len();
        names.sort_unstable();
        names.dedup();

        assert_eq!(names.len(), count, "duplicate builtin name");
    }

    /// Whether this build has the feature an entry's dependency needs.
    ///
    /// `Implemented` is a claim about the source, not about every build of it.
    /// A build with one feature on still refuses the other adapters' invariants,
    /// and refuses them correctly, with a message naming the missing feature.
    fn compiled_in(dependency: &str) -> bool {
        match dependency {
            "nats" => cfg!(feature = "nats"),
            "postgres" => cfg!(feature = "postgres"),
            "http" => cfg!(feature = "http"),
            _ => true,
        }
    }

    #[test]
    fn every_implemented_entry_builds() {
        for entry in REGISTRY {
            if entry.status != Status::Implemented || !compiled_in(entry.dependency) {
                continue;
            }

            let spec = InvariantSpec {
                builtin: Some(entry.name.to_string()),
                ..InvariantSpec::default()
            };

            let built = build(&spec)
                .unwrap_or_else(|error| panic!("{} claims Implemented: {error}", entry.name));

            assert_eq!(
                built.name(),
                entry.name,
                "name disagrees with its registry entry"
            );
        }
    }

    #[test]
    fn a_planned_entry_is_refused_by_name_rather_than_as_unknown() {
        let planned = REGISTRY
            .iter()
            .find(|entry| entry.status == Status::Planned)
            .expect("the registry documents planned work");

        let spec = InvariantSpec {
            builtin: Some(planned.name.to_string()),
            ..InvariantSpec::default()
        };

        let error = build(&spec).expect_err("planned invariants do not build");

        assert!(matches!(error, Error::Unsupported(_)), "got {error:?}");
        assert!(
            is_known(planned.name),
            "a planned name is still a known name"
        );
    }

    #[test]
    fn an_unknown_name_lists_the_alternatives() {
        let spec = InvariantSpec {
            builtin: Some("no_such_thing".to_string()),
            ..InvariantSpec::default()
        };

        let error = build(&spec).expect_err("should refuse");

        assert!(
            error.to_string().contains("eventually_quiescent"),
            "got {error}"
        );
    }
}

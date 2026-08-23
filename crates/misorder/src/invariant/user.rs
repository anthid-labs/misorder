//! Assertions the user writes.
//!
//! Protocol invariants cannot know that fills never exceed order quantity.
//! That part is irreducibly the user's, and it is also where the leverage is:
//!
//! ```toml
//! [[invariants]]
//! name = "fills_never_exceed_order_qty"
//! check = "sql"
//! query = """
//! select 1 from orders o
//! join fills f on f.order_id = o.id
//! group by o.id, o.qty having sum(f.qty) > o.qty
//! """
//! expect = "empty"
//! ```
//!
//! Five of those, checked against ten thousand orderings, is the whole pitch.
//!
//! # Why the query looks for the violation
//!
//! `expect = "empty"` is the default and the shape to reach for. A query that
//! searches for the bad state either finds it or does not, and needs no
//! knowledge of how many rows a correct run produces. A query asserting the
//! good state has to encode the expected volume, which changes with the
//! workload, and then it is a test of the scenario rather than of the service.

// Gated with the implementation they serve: a build without the postgres
// feature has no SQL invariant, so these would be unused imports rather than
// a harmless surplus.
#[cfg(feature = "postgres")]
use async_trait::async_trait;

use crate::error::{Error, Result};
// Unconditional: the HTTP check needs these in every build, because it asks the
// service under test rather than a dependency and so is not tied to a protocol
// feature at all. They were gated when the SQL check was the only user.
use crate::event::Observed;
use crate::invariant::{CheckContext, Invariant, Violation};
use crate::scenario::file::{CheckKind, Expect, InvariantSpec};

/// Constructs a user invariant from its scenario block.
pub fn build(spec: &InvariantSpec) -> Result<Box<dyn Invariant>> {
    let name = spec
        .name
        .clone()
        .ok_or_else(|| Error::Internal("build called without a `name` key".to_string()))?;

    let expect = spec.expect.unwrap_or_default();

    match spec.check {
        Some(kind @ (CheckKind::Sql | CheckKind::Http)) => {
            let query = spec
                .query
                .clone()
                .ok_or_else(|| Error::Scenario(format!("invariant `{name}` has no `query`")))?;

            match kind {
                CheckKind::Sql => build_sql(name, query, expect),
                CheckKind::Http => Ok(Box::new(HttpInvariant {
                    name,
                    path: query,
                    expect,
                })),
            }
        }
        None => Err(Error::Scenario(format!(
            "invariant `{name}` has no `check`; use `check = \"sql\"` or `check = \"http\"`"
        ))),
    }
}

/// A question put to the service under test once the system is quiescent.
///
/// The HTTP counterpart of [`SqlInvariant`], and deliberately the same shape:
/// `query` is a path that answers a question about the *bad* state, and
/// `expect = "empty"` is what a healthy run returns. A service that owns its
/// own storage can be checked without a database beside it, which is the
/// difference between a scenario someone can run and one they have to set up
/// first.
///
/// Asked of the service directly rather than through the proxy. A terminal
/// check routed through the fault injector could be dropped, delayed past the
/// report, or reordered, and the invariant would then be measuring the harness.
#[derive(Debug)]
struct HttpInvariant {
    name: String,
    path: String,
    expect: Expect,
}

#[async_trait::async_trait]
impl Invariant for HttpInvariant {
    fn name(&self) -> &str {
        &self.name
    }

    fn describe(&self) -> &str {
        "an HTTP check against the final state"
    }

    fn observe(&mut self, _observed: &Observed) -> Option<Violation> {
        None
    }

    async fn finish(&mut self, context: &CheckContext) -> Result<Option<Violation>> {
        let Some(base) = &context.service_url else {
            return Err(Error::Scenario(format!(
                "invariant `{}` uses `check = \"http\"`, but no [[system]] declares a \
                 `listen_env`, so there is no service address to ask",
                self.name
            )));
        };

        let rows = http_get_rows(base, &self.path)
            .await
            .map_err(|error| Error::Environment(format!("checking `{}`: {error}", self.name)))?;

        let violated = match self.expect {
            Expect::Empty => rows > 0,
            Expect::NonEmpty => rows == 0,
        };

        if !violated {
            return Ok(None);
        }

        Ok(Some(Violation {
            invariant: self.name.clone(),
            detail: match self.expect {
                Expect::Empty => format!("{} returned {rows} row(s), expected none", self.path),
                Expect::NonEmpty => {
                    format!("{} returned no rows, expected at least one", self.path)
                }
            },
            at: context.elapsed,
        }))
    }
}

/// GETs a path and counts the rows in the answer.
///
/// A hand-rolled request rather than an HTTP client dependency: this is one
/// GET against loopback, and the engine's dependency list is something buyers
/// read.
async fn http_get_rows(base: &str, path: &str) -> std::result::Result<usize, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(base)
        .await
        .map_err(|error| format!("connecting to the service at {base}: {error}"))?;

    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };

    let request = format!("GET {path} HTTP/1.1\r\nHost: misorder\r\nConnection: close\r\n\r\n");

    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("sending the check request: {error}"))?;

    let mut response = String::new();

    stream
        .read_to_string(&mut response)
        .await
        .map_err(|error| format!("reading the check response: {error}"))?;

    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "the service answered without a complete header".to_string())?;

    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "the service answered without a status line".to_string())?;

    if !status.starts_with('2') {
        return Err(format!("the service answered {status} to {path}"));
    }

    let body = body.trim();

    if body.is_empty() || body == "null" {
        return Ok(0);
    }

    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| format!("the answer to {path} is not JSON: {error}"))?;

    match value {
        serde_json::Value::Null => Ok(0),
        serde_json::Value::Array(rows) => Ok(rows.len()),
        // A single object is one row. Anything else is a service answering the
        // wrong shape, and saying so beats silently counting it as healthy.
        serde_json::Value::Object(_) => Ok(1),
        other => Err(format!(
            "the answer to {path} should be a JSON array of the bad rows, got {other}"
        )),
    }
}

#[cfg(feature = "postgres")]
fn build_sql(name: String, query: String, expect: Expect) -> Result<Box<dyn Invariant>> {
    Ok(Box::new(SqlInvariant {
        name,
        query,
        expect,
    }))
}

#[cfg(not(feature = "postgres"))]
fn build_sql(name: String, _query: String, _expect: Expect) -> Result<Box<dyn Invariant>> {
    Err(Error::Unsupported(format!(
        "invariant `{name}` uses `check = \"sql\"`, but this build has no postgres feature"
    )))
}

/// A query run against the scenario's Postgres once the system is quiescent.
///
/// Terminal rather than streaming, and that is a real limitation rather than a
/// simplification: a transient violation that is repaired before the run ends
/// is invisible here. Catching those needs the query run at every quiescent
/// point, which needs the virtual clock to be affordable.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct SqlInvariant {
    name: String,
    query: String,
    expect: Expect,
}

#[cfg(feature = "postgres")]
#[async_trait]
impl Invariant for SqlInvariant {
    fn name(&self) -> &str {
        &self.name
    }

    fn describe(&self) -> &str {
        "a SQL check against the final state"
    }

    fn observe(&mut self, _observed: &Observed) -> Option<Violation> {
        None
    }

    async fn finish(&mut self, context: &CheckContext) -> Result<Option<Violation>> {
        let url = context.postgres_url.as_deref().ok_or_else(|| {
            Error::Scenario(format!(
                "invariant `{}` needs a [deps.postgres] block to query",
                self.name
            ))
        })?;

        // Connected here rather than held open through the run. A pooled
        // connection kept by the checker would be one more participant in the
        // interleaving it is supposed to be observing, and it would hold locks
        // the service under test is waiting on.
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .map_err(|error| {
                Error::Environment(format!("connecting to check `{}`: {error}", self.name))
            })?;

        let pump = tokio::spawn(connection);

        let rows = client.query(&self.query, &[]).await.map_err(|error| {
            Error::Scenario(format!("invariant `{}` query failed: {error}", self.name))
        });

        drop(client);
        pump.abort();

        let rows = rows?;

        let violated = match self.expect {
            Expect::Empty => !rows.is_empty(),
            Expect::NonEmpty => rows.is_empty(),
        };

        if !violated {
            return Ok(None);
        }

        Ok(Some(Violation {
            invariant: self.name.clone(),
            detail: match self.expect {
                Expect::Empty => format!("query returned {} row(s), expected none", rows.len()),
                Expect::NonEmpty => "query returned no rows, expected at least one".to_string(),
            },
            at: context.elapsed,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str) -> InvariantSpec {
        InvariantSpec {
            name: Some(name.to_string()),
            ..InvariantSpec::default()
        }
    }

    #[test]
    fn a_check_without_a_kind_says_which_to_use() {
        let error = build(&spec("fills")).expect_err("should refuse");

        assert!(error.to_string().contains("check = \"sql\""), "got {error}");
    }

    #[test]
    fn a_sql_check_without_a_query_names_the_invariant() {
        let mut spec = spec("fills_never_exceed_order_qty");
        spec.check = Some(CheckKind::Sql);

        let error = build(&spec).expect_err("should refuse");

        assert!(
            error.to_string().contains("fills_never_exceed_order_qty"),
            "got {error}"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn a_complete_sql_check_builds_and_keeps_its_name() {
        let mut spec = spec("fills_never_exceed_order_qty");
        spec.check = Some(CheckKind::Sql);
        spec.query = Some("select 1".to_string());

        let built = build(&spec).expect("builds");

        assert_eq!(built.name(), "fills_never_exceed_order_qty");
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn a_sql_check_with_no_database_blames_the_scenario_not_the_service() {
        let mut spec = spec("fills");
        spec.check = Some(CheckKind::Sql);
        spec.query = Some("select 1".to_string());

        let mut built = build(&spec).expect("builds");
        let error = built
            .finish(&CheckContext::default())
            .await
            .expect_err("no database");

        assert!(matches!(error, Error::Scenario(_)), "got {error:?}");
    }
}

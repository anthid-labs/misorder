//! Where recorded vendor behaviour comes from.
//!
//! A scenario names behaviours; something has to say what those names mean:
//!
//! ```toml
//! [vendors.lightspeed]
//! behaviors = [
//!   "no_ack_on_second_replace",
//!   "subscribe_events_does_not_replay",
//!   "sequence_does_not_advance_on_reject",
//! ]
//! ```
//!
//! [`CorpusSource`] is that something. This crate ships exactly one
//! implementation, [`LocalCorpus`], which reads a directory of TOML files. The
//! trait is a seam rather than an abstraction for its own sake: a corpus
//! assembled and validated somewhere else implements it from outside this
//! repository, so adding one is a different `CorpusSource` rather than a fork
//! of the engine.
//!
//! # Why the corpus is the hard part
//!
//! You cannot theorise that a broker omits the ack on a second replace. You
//! record it. Documentation, OpenAPI specs and schemas are precisely the
//! artifacts that were wrong in the first place, which is why the recording is
//! the asset and why the engine alone cannot produce one.
//!
//! # Provenance is part of the data, not metadata about it
//!
//! Every behaviour carries where it came from. A buyer in a regulated segment
//! is going to ask, an auditor is going to ask, and "we observed this" versus
//! "a vendor's changelog said this" are different claims with different
//! weights. [`Provenance`] makes the difference structural rather than a note
//! in a description field.
//!
//! # What is deliberately not here
//!
//! No registry client, no network, no credentials. The engine never phones
//! home. Users in this segment treat silent collection of anything resembling
//! production traffic as a compliance incident rather than a PR problem, and
//! one security review that finds an unexpected outbound connection ends the
//! conversation permanently. Anything that talks to a remote service is a
//! separate binary the user chooses to run.
//!
//! The transcript *body* format is also not here. Recording sessions is Phase 2
//! work, and inventing a frame encoding before there is a recorder to validate
//! it would fix the wrong shape into a compatibility surface. Behaviours
//! reference a transcript by id and digest through [`TranscriptRef`], which is
//! the part scenarios need today, and the body joins this trait when the
//! recorder does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Format version for a corpus file on disk.
///
/// A compatibility surface with a long life: a corpus entry contributed today
/// is read by a build shipped in two years. Bumped whenever an older misorder
/// would misread a newer file.
pub const FORMAT_VERSION: u32 = 1;

/// A named thing a vendor was observed doing.
///
/// The name is the interface. It appears in scenarios, in reports, and in drift
/// alerts, so it is written as a sentence about the vendor's behaviour and not
/// as an identifier: `no_ack_on_second_replace`, not `ls_bug_14`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BehaviorFlag {
    pub name: String,

    /// Which wire protocol this shows up on, so an adapter can refuse a
    /// behaviour it cannot express.
    pub protocol: String,

    /// One line, in the vendor's own vocabulary.
    pub describe: String,

    pub provenance: Provenance,
}

/// Where a behaviour came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Provenance {
    /// Observed on the wire. The strongest claim, and the only one that cannot
    /// be got from reading.
    Recorded(TranscriptRef),

    /// The vendor said so: a changelog, a release note, a support ticket.
    /// Weaker than a recording, and still worth having, because it dates the
    /// change.
    Documented { url: String },

    /// Someone else hit it: a GitHub issue on the vendor's SDK, a forum thread.
    /// The weakest claim, and often the first sign of a real one.
    Reported { url: String },
}

/// A recorded session, by id.
///
/// The digest is what makes a corpus entry verifiable rather than asserted: a
/// consumer can check that the transcript it fetched is the one the behaviour
/// was derived from. That check is the difference between a curated corpus and
/// a wiki.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptRef {
    pub transcript: String,

    /// BLAKE3 of the transcript body, when the source published one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// Everything one source knows about one vendor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VendorBehaviors {
    #[serde(default = "current_format")]
    pub format: u32,

    pub vendor: String,

    #[serde(default)]
    pub behaviors: Vec<BehaviorFlag>,
}

fn current_format() -> u32 {
    FORMAT_VERSION
}

impl VendorBehaviors {
    pub fn get(&self, name: &str) -> Option<&BehaviorFlag> {
        self.behaviors.iter().find(|flag| flag.name == name)
    }

    pub fn names(&self) -> Vec<&str> {
        self.behaviors
            .iter()
            .map(|flag| flag.name.as_str())
            .collect()
    }
}

/// Somewhere behaviours are looked up.
///
/// Deliberately read-only and deliberately tiny. Other sources implement this
/// from outside the repository, and nothing in the engine should ever grow a
/// method only one of them could answer - a trait shaped around a single
/// implementation stops being a seam.
#[async_trait]
pub trait CorpusSource: Send + Sync {
    /// Names this source in errors and reports, so a user can tell which corpus
    /// a behaviour came from.
    fn name(&self) -> &str;

    /// Everything this source knows about a vendor, or `None` if it knows
    /// nothing.
    async fn vendor(&self, vendor: &str) -> Result<Option<VendorBehaviors>>;
}

/// Resolves the behaviour names a scenario asked for.
///
/// An unknown name is an error rather than a warning, and the message lists
/// what the vendor does have. A scenario silently running without the behaviour
/// it named is the same failure as a scenario silently permitting no faults: it
/// passes, and it tested nothing.
pub async fn resolve(
    source: &dyn CorpusSource,
    requested: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<BehaviorFlag>> {
    let mut resolved = Vec::new();

    for (vendor, names) in requested {
        let known = source.vendor(vendor).await?.ok_or_else(|| {
            Error::Scenario(format!(
                "corpus `{}` has nothing for vendor `{vendor}`",
                source.name()
            ))
        })?;

        for name in names {
            let flag = known.get(name).ok_or_else(|| {
                Error::Scenario(format!(
                    "corpus `{}` has no behaviour `{name}` for `{vendor}`; it has: {}",
                    source.name(),
                    if known.behaviors.is_empty() {
                        "nothing".to_string()
                    } else {
                        known.names().join(", ")
                    }
                ))
            })?;

            resolved.push(flag.clone());
        }
    }

    Ok(resolved)
}

/// A corpus that knows nothing.
///
/// The default, and the reason a scenario with no `[vendors]` section needs no
/// corpus configured at all. It fails loudly rather than silently for a
/// scenario that does name a vendor, so "I forgot `--corpus`" reads differently
/// from "that behaviour does not exist".
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyCorpus;

#[async_trait]
impl CorpusSource for EmptyCorpus {
    fn name(&self) -> &str {
        "none"
    }

    async fn vendor(&self, vendor: &str) -> Result<Option<VendorBehaviors>> {
        Err(Error::Scenario(format!(
            "this scenario names vendor `{vendor}`, but no corpus was given; \
             pass --corpus <directory>"
        )))
    }
}

/// A directory of `<vendor>.toml` files.
///
/// The only implementation shipped here, and enough to be genuinely useful:
/// a team's own three broker integrations, checked into their own repository,
/// is a real corpus and needs nobody's permission.
#[derive(Debug, Clone)]
pub struct LocalCorpus {
    root: PathBuf,
}

impl LocalCorpus {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Every vendor this directory has a file for, in a stable order.
    pub fn vendors(&self) -> Result<Vec<String>> {
        let entries = std::fs::read_dir(&self.root).map_err(|error| {
            Error::Scenario(format!(
                "cannot read corpus {}: {error}",
                self.root.display()
            ))
        })?;

        let mut vendors: Vec<String> = entries
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "toml")
            })
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(str::to_string)
            })
            .collect();

        vendors.sort();

        Ok(vendors)
    }
}

#[async_trait]
impl CorpusSource for LocalCorpus {
    fn name(&self) -> &str {
        "local"
    }

    async fn vendor(&self, vendor: &str) -> Result<Option<VendorBehaviors>> {
        // Rejected rather than joined: a vendor name is written in a scenario
        // file, and `../../etc/passwd` reaching a path join is how a config
        // format becomes a file-read primitive.
        if vendor.is_empty() || vendor.contains(['/', '\\', '.']) {
            return Err(Error::Scenario(format!(
                "`{vendor}` is not a usable vendor name"
            )));
        }

        let path = self.root.join(format!("{vendor}.toml"));

        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::Io(error)),
        };

        let behaviors: VendorBehaviors = toml::from_str(&text)
            .map_err(|error| Error::Scenario(format!("{}: {error}", path.display())))?;

        if behaviors.format > FORMAT_VERSION {
            return Err(Error::Scenario(format!(
                "{} is corpus format {}; this build reads up to {FORMAT_VERSION}",
                path.display(),
                behaviors.format
            )));
        }

        if behaviors.vendor != vendor {
            return Err(Error::Scenario(format!(
                "{} declares vendor `{}` but is filed under `{vendor}`",
                path.display(),
                behaviors.vendor
            )));
        }

        Ok(Some(behaviors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIGHTSPEED: &str = r#"
vendor = "lightspeed"

[[behaviors]]
name = "no_ack_on_second_replace"
protocol = "fix"
describe = "A second replace on one order gets no execution report."
provenance = { kind = "recorded", transcript = "ls-2026-03-11-a", digest = "abc123" }

[[behaviors]]
name = "sequence_does_not_advance_on_reject"
protocol = "fix"
describe = "A rejected order leaves the sequence number where it was."
provenance = { kind = "documented", url = "https://example.invalid/changelog" }
"#;

    fn corpus() -> (tempfile::TempDir, LocalCorpus) {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(directory.path().join("lightspeed.toml"), LIGHTSPEED).expect("write");

        let corpus = LocalCorpus::new(directory.path());

        (directory, corpus)
    }

    #[tokio::test]
    async fn a_vendor_file_parses_with_its_provenance() {
        let (_directory, corpus) = corpus();

        let vendor = corpus
            .vendor("lightspeed")
            .await
            .expect("read")
            .expect("present");

        assert_eq!(vendor.behaviors.len(), 2);
        assert!(matches!(
            vendor.behaviors[0].provenance,
            Provenance::Recorded(_)
        ));
        assert!(matches!(
            vendor.behaviors[1].provenance,
            Provenance::Documented { .. }
        ));
    }

    #[tokio::test]
    async fn an_unknown_vendor_is_absent_rather_than_an_error() {
        let (_directory, corpus) = corpus();

        assert!(corpus.vendor("tradier").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn a_vendor_name_cannot_escape_the_corpus_directory() {
        let (_directory, corpus) = corpus();

        assert!(corpus.vendor("../../etc/passwd").await.is_err());
    }

    #[tokio::test]
    async fn a_file_filed_under_the_wrong_vendor_is_refused() {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(directory.path().join("tradier.toml"), LIGHTSPEED).expect("write");

        let error = LocalCorpus::new(directory.path())
            .vendor("tradier")
            .await
            .expect_err("mismatched");

        assert!(error.to_string().contains("filed under"), "got {error}");
    }

    #[tokio::test]
    async fn resolving_a_named_behaviour_returns_it() {
        let (_directory, corpus) = corpus();

        let requested = BTreeMap::from([(
            "lightspeed".to_string(),
            vec!["no_ack_on_second_replace".to_string()],
        )]);

        let resolved = resolve(&corpus, &requested).await.expect("resolve");

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].name, "no_ack_on_second_replace");
    }

    #[tokio::test]
    async fn an_unknown_behaviour_lists_what_the_vendor_has() {
        let (_directory, corpus) = corpus();

        let requested = BTreeMap::from([(
            "lightspeed".to_string(),
            vec!["no_such_behaviour".to_string()],
        )]);

        let error = resolve(&corpus, &requested).await.expect_err("unknown");

        assert!(
            error.to_string().contains("no_ack_on_second_replace"),
            "got {error}"
        );
    }

    #[tokio::test]
    async fn naming_a_vendor_with_no_corpus_says_to_pass_one() {
        let requested = BTreeMap::from([("lightspeed".to_string(), vec!["anything".to_string()])]);

        let error = resolve(&EmptyCorpus, &requested)
            .await
            .expect_err("no corpus");

        assert!(error.to_string().contains("--corpus"), "got {error}");
    }

    #[tokio::test]
    async fn a_corpus_from_the_future_is_refused_rather_than_guessed_at() {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            directory.path().join("acme.toml"),
            "format = 99\nvendor = \"acme\"\n",
        )
        .expect("write");

        let error = LocalCorpus::new(directory.path())
            .vendor("acme")
            .await
            .expect_err("future format");

        assert!(error.to_string().contains("format"), "got {error}");
    }

    #[test]
    fn a_corpus_directory_lists_its_vendors_in_a_stable_order() {
        let directory = tempfile::tempdir().expect("tempdir");

        for vendor in ["tradier", "alpaca", "lightspeed"] {
            std::fs::write(
                directory.path().join(format!("{vendor}.toml")),
                format!("vendor = \"{vendor}\"\n"),
            )
            .expect("write");
        }

        assert_eq!(
            LocalCorpus::new(directory.path()).vendors().expect("list"),
            vec!["alpaca", "lightspeed", "tradier"]
        );
    }
}

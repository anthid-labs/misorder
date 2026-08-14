//! The scenario and corpus files shipped in this repository must parse.
//!
//! A broken example is a bad first five minutes, and the example is the
//! onboarding. This also pins the documented format against the parser: a key
//! renamed in `scenario::file` without updating `misorder.example.toml` fails
//! here rather than in someone's terminal.

use std::path::{Path, PathBuf};

use misorder::corpus::{CorpusSource, LocalCorpus};
use misorder::scenario::file::Scenario;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the workspace root is two levels up from this crate")
}

fn resolves(path: &Path) -> misorder::scenario::file::Resolved {
    let scenario = Scenario::load(path)
        .unwrap_or_else(|error| panic!("{} does not parse: {error}", path.display()));

    scenario
        .resolve()
        .unwrap_or_else(|error| panic!("{} does not resolve: {error}", path.display()))
}

/// Every `.toml` directly under `examples/`, which is every scenario and
/// nothing else: the corpus lives a directory down.
fn example_scenarios() -> Vec<PathBuf> {
    let directory = repository_root().join("examples");

    let mut paths: Vec<PathBuf> = std::fs::read_dir(&directory)
        .expect("examples/ exists")
        .map(|entry| entry.expect("readable entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "toml")
        })
        .collect();

    paths.sort();

    assert!(
        !paths.is_empty(),
        "no scenarios found in {}",
        directory.display()
    );

    paths
}

fn example_corpus() -> LocalCorpus {
    LocalCorpus::new(repository_root().join("examples").join("corpus"))
}

#[test]
fn the_documented_example_parses_and_resolves() {
    resolves(&repository_root().join("misorder.example.toml"));
}

#[test]
fn every_scenario_under_examples_parses_and_resolves() {
    for path in example_scenarios() {
        resolves(&path);
    }
}

#[tokio::test]
async fn every_corpus_file_under_examples_parses() {
    let corpus = example_corpus();

    let vendors = corpus.vendors().expect("examples/corpus/ exists");

    assert!(!vendors.is_empty(), "no vendors in examples/corpus/");

    for vendor in vendors {
        corpus
            .vendor(&vendor)
            .await
            .unwrap_or_else(|error| panic!("{vendor}: {error}"))
            .unwrap_or_else(|| panic!("{vendor} lists itself and then is not there"));
    }
}

#[tokio::test]
async fn every_behaviour_an_example_names_is_in_the_example_corpus() {
    // The half that bites. A scenario naming a behaviour the corpus lacks is
    // refused at startup, so a typo in either file is not a warning, it is a
    // first five minutes that ends at an error message.
    let corpus = example_corpus();

    let mut checked = 0;

    for path in example_scenarios() {
        let scenario = resolves(&path);

        if scenario.vendors.is_empty() {
            continue;
        }

        misorder::corpus::resolve(&corpus, &scenario.vendors)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        checked += 1;
    }

    assert!(checked > 0, "no example names a vendor behaviour");
}

#[test]
fn the_documented_example_exercises_every_fault() {
    // Not a style rule. A fault absent from the reference file is a fault
    // nobody knows exists, and the list in `[faults] enabled` is the only place
    // a user learns the vocabulary.
    let text = std::fs::read_to_string(repository_root().join("misorder.example.toml"))
        .expect("read example");

    for fault in misorder::schedule::FaultKind::ALL {
        assert!(
            text.contains(fault.as_str()),
            "`{fault}` is not mentioned in misorder.example.toml"
        );
    }
}

#[test]
fn the_documented_example_mentions_every_builtin_invariant() {
    let text = std::fs::read_to_string(repository_root().join("misorder.example.toml"))
        .expect("read example");

    // The implemented ones only. A planned invariant in the reference file
    // would be a scenario a user could copy that then refuses to run.
    for entry in misorder::invariant::builtin::REGISTRY {
        if entry.status != misorder::invariant::builtin::Status::Implemented {
            continue;
        }

        assert!(
            text.contains(entry.name),
            "`{}` is implemented but not mentioned in misorder.example.toml",
            entry.name
        );
    }
}

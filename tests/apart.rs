//! PLEXUS-26 (PLEXUS-16): no manifest of this repository reaches outside it.
//! Every dependency comes by version or by git rev and every `path` stays in
//! the repository, so a clone builds alone and a program that imports
//! substrate holds one of each crate: two paths to one crate are two crates
//! to cargo (FRAME-9).

use std::path::{Component, Path, PathBuf};

/// This repository's manifests.
const MANIFESTS: &[&str] = &["Cargo.toml"];

fn root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf())
}

/// `path` with `.` and `..` taken out by its text alone: the place named may
/// be gone, and a leftover reference is what this file looks for.
fn resolve(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c),
        }
    }
    out
}

/// Every `path` key in `value`, at any depth (a dependency's, a `[patch]`'s,
/// a target's, a `[lib]`'s), as (the keys it sits under, the path).
fn paths(value: &toml::Value, at: &str, out: &mut Vec<(String, String)>) {
    match value {
        toml::Value::Table(table) => {
            for (key, v) in table {
                let here = if at.is_empty() {
                    key.clone()
                } else {
                    format!("{at}.{key}")
                };
                match v {
                    toml::Value::String(path) if key == "path" => out.push((here, path.clone())),
                    v => paths(v, &here, out),
                }
            }
        }
        toml::Value::Array(items) => {
            for v in items {
                paths(v, at, out);
            }
        }
        _ => {}
    }
}

/// What `found`, read from the manifest in `dir`, breaks of the rule.
fn breaks(found: &[(String, String)], file: &str, dir: &Path, root: &Path) -> Vec<String> {
    found
        .iter()
        .filter(|(_, path)| !resolve(&dir.join(path)).starts_with(root))
        .map(|(key, path)| format!("{file}: {key} = \"{path}\" is outside {}", root.display()))
        .collect()
}

#[test]
fn no_manifest_reaches_outside() {
    let root = root();
    let mut read = 0;
    let mut broken = Vec::new();
    for file in MANIFESTS {
        let path = root.join(file);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
        let value = toml::from_str(&text).unwrap_or_else(|e| panic!("{file}: {e}"));
        let mut found = Vec::new();
        paths(&value, "", &mut found);
        read += found.len();
        broken.extend(breaks(&found, file, path.parent().unwrap(), &root));
    }
    assert!(
        read > 0,
        "no manifest names a path: the check reads nothing"
    );
    assert!(
        broken.is_empty(),
        "PLEXUS-26: reaches outside the repository:\n  {}",
        broken.join("\n  ")
    );
}

#[test]
fn the_check_reads_what_it_is_given() {
    let text = r#"
        [lib]
        path = "src/lib.rs"
        [dependencies]
        # plexus-login = { path = "../in-a-comment" }
        plexus-core = "0.7.0"
        plexus-login = { git = "https://example.com/plexus-login", rev = "23b9e2c" }
        inside = { path = "crates/inside" }
        outside = { path = "../outside", version = "0.1" }
        [target.'cfg(unix)'.dev-dependencies]
        deep = { path = "crates/../../deep" }
        [patch.crates-io]
        patched = { path = "../patched" }
    "#;
    let value = toml::from_str(text).unwrap();
    let mut found = Vec::new();
    paths(&value, "", &mut found);
    assert_eq!(found.len(), 5, "{found:?}");

    let mut broken = breaks(
        &found,
        "Cargo.toml",
        Path::new("/w/repo"),
        Path::new("/w/repo"),
    );
    broken.sort();
    assert_eq!(
        broken,
        [
            "Cargo.toml: dependencies.outside.path = \"../outside\" is outside /w/repo",
            "Cargo.toml: patch.crates-io.patched.path = \"../patched\" is outside /w/repo",
            "Cargo.toml: target.cfg(unix).dev-dependencies.deep.path = \"crates/../../deep\" is outside /w/repo",
        ]
    );

    // A member two deep may name another member, and no further.
    let member = [
        ("dependencies.a.path".to_string(), "../a".to_string()),
        ("dependencies.b.path".to_string(), "../../../b".to_string()),
    ];
    let broken = breaks(
        &member,
        "m/one/Cargo.toml",
        Path::new("/w/repo/m/one"),
        Path::new("/w/repo"),
    );
    assert_eq!(
        broken,
        ["m/one/Cargo.toml: dependencies.b.path = \"../../../b\" is outside /w/repo"]
    );
}

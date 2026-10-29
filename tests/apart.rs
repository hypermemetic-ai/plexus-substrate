//! FRAME-9: plexus-login (and plexus-shell, should this repository take it)
//! comes from its own sibling checkout, `../plexus-login` beside this one —
//! never from menger's tree, where FRAME-4 lifted them out of. Two paths to
//! one crate are two crates to cargo: a program that links this beside
//! menger (or menger's tether) holds one `plexus-login` only if every
//! manifest names the same checkout.

use std::path::{Component, Path, PathBuf};

/// The crates taken from a sibling checkout of their own name.
const SIBLING_CRATES: &[&str] = &["plexus-login", "plexus-shell"];

/// This repository's manifests.
const MANIFESTS: &[&str] = &["Cargo.toml"];

fn root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf())
}

/// `path` with `.` and `..` taken out by its text alone, then canonical
/// where it exists (the place named may be gone).
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
    out.canonicalize().unwrap_or(out)
}

/// Each `<crate> = { path = "…" }` in `text` (a manifest in `dir`) whose
/// crate is one of `crates`, as (line, crate, the place it names) —
/// `#` comments ignored.
fn sibling_paths(dir: &Path, text: &str, crates: &[&str]) -> Vec<(usize, String, PathBuf)> {
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("");
        let Some((key, value)) = line.split_once('=') else { continue };
        let key = key.trim();
        if !crates.contains(&key) {
            continue;
        }
        let Some(at) = value.find("path") else { continue };
        let Some(path) = value[at..].split('"').nth(1) else { continue };
        out.push((i + 1, key.to_string(), resolve(&dir.join(path))));
    }
    out
}

/// What `found` breaks of the rule: each crate from `parent/<crate>`.
fn breaks(found: &[(usize, String, PathBuf)], file: &str, parent: &Path) -> Vec<String> {
    found
        .iter()
        .filter(|(_, krate, place)| *place != resolve(&parent.join(krate)))
        .map(|(n, krate, place)| format!("{file}:{n}: {krate} from {}, not the sibling {}", place.display(), parent.join(krate).display()))
        .collect()
}

#[test]
fn login_comes_from_its_sibling() {
    let root = root();
    let parent = root.parent().expect("a parent").to_path_buf();
    let mut taken = 0;
    let mut broken = Vec::new();
    for file in MANIFESTS {
        let path = root.join(file);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
        let found = sibling_paths(path.parent().unwrap(), &text, SIBLING_CRATES);
        taken += found.len();
        broken.extend(breaks(&found, file, &parent));
    }
    assert!(taken > 0, "no manifest takes plexus-login by path — the check reads nothing");
    assert!(broken.is_empty(), "FRAME-9: taken from elsewhere than the sibling checkout:\n  {}", broken.join("\n  "));
}

#[test]
fn the_check_reads_what_it_is_given() {
    let text = "# plexus-login = { path = \"../x\" }\nplexus-login-extra = { path = \"../x\" }\n\
                plexus-login = { path = \"../plexus-login\", version = \"0.2\" }\n\
                plexus-shell = { path = \"../menger/plexus-shell\" }\nother = { path = \"../plexus-login\" }\n\
                plexus-login = { version = \"0.2\" }\n";
    let found = sibling_paths(Path::new("/w/repo"), text, SIBLING_CRATES);
    assert_eq!(found, [(3, "plexus-login".into(), "/w/plexus-login".into()), (4, "plexus-shell".into(), "/w/menger/plexus-shell".into())]);
    assert_eq!(breaks(&found, "Cargo.toml", Path::new("/w")), ["Cargo.toml:4: plexus-shell from /w/menger/plexus-shell, not the sibling /w/plexus-shell"]);
    let nested = sibling_paths(Path::new("/w/repo/member"), "plexus-login = { path = \"../../plexus-login\" }\n", SIBLING_CRATES);
    assert!(breaks(&nested, "member/Cargo.toml", Path::new("/w")).is_empty(), "a member two deep");
}

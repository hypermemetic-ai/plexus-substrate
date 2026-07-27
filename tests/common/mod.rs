//! Shared fixtures for PLX-151's confinement tests.

#![allow(dead_code)]
// The `common` module is compiled into several test binaries; items unused by
// one of them are not dead, and `pub` here is the only visibility that works.
#![allow(unreachable_pub)]

pub mod adversary;

use std::path::{Path, PathBuf};

/// A fixture directory that removes itself.
///
/// # Why this lives under `$HOME` and not in `TMPDIR`
///
/// Inherited measurement from PLX-144, and it is not cosmetic: on macOS +
/// colima (virtiofs) a bind mount of a macOS `mktemp -d` path fails outright —
/// `bind source path does not exist: /var/folders/…` — because the Linux VM
/// only has `$HOME` and `/tmp/colima` shared into it. A fixture in `TMPDIR`
/// makes every Docker test fail for a reason that has nothing to do with
/// confinement.
pub struct Fixture {
    base: PathBuf,
}

impl Fixture {
    pub fn new(label: &str) -> Self {
        let root = std::env::var_os("PLEXUS_SUBSTRATE_TEST_BASE").map_or_else(
            || {
                let home = std::env::var_os("HOME").expect("HOME must be set to run these tests");
                PathBuf::from(home).join(".plexus-substrate-tests")
            },
            PathBuf::from,
        );
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = root.join(format!("{label}-{}-{nonce:x}", std::process::id()));
        std::fs::create_dir_all(&base).expect("create fixture base");
        Self { base }
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn dir(&self, rel: &str) -> PathBuf {
        let p = self.base.join(rel);
        std::fs::create_dir_all(&p).expect("create fixture dir");
        p
    }

    pub fn file(&self, rel: &str, contents: &str) -> PathBuf {
        let p = self.base.join(rel);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(&p, contents).expect("write fixture file");
        p
    }

    pub fn symlink(&self, rel: &str, target: &Path) -> PathBuf {
        let p = self.base.join(rel);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let _ = std::fs::remove_file(&p);
        std::os::unix::fs::symlink(target, &p).expect("symlink");
        p
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// Kill anything the sandbox started that is still up. A hang must not leave a
/// container behind — PLX-144 filled this machine's disk once already.
pub fn reap_stray_containers() {
    if let Ok(listed) = std::process::Command::new("docker")
        .args(["ps", "-q", "--filter", "name=plexus-sbx-"])
        .output()
    {
        for id in String::from_utf8_lossy(&listed.stdout).split_whitespace() {
            let _ = std::process::Command::new("docker").args(["kill", id]).output();
        }
    }
}

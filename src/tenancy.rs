//! PLX-151 c4 — where the sealed proof is honoured.
//!
//! # The residual this module closes
//!
//! `plexus_sandbox::TenantRoot::resolve` takes a `&TenantId`, not a
//! `TenantRecord`. PLX-144 wanted the sealed record and could not have it:
//! `TenantRecord` lives in plexus-idp, which sits *above* plexus-core and
//! plexus-sandbox, so a confinement crate that depended on the identity
//! provider would invert the dependency order. PLX-144 stated the residual in
//! `TenantRoot`'s own rustdoc rather than hiding it:
//!
//! > *the obligation lands on the caller: resolve the `TenantRecord` first,
//! > check `is_active()`, and pass `record.id()`.*
//!
//! PLX-127 stated the matching half from the mount's side: `AdmittedTenant`
//! proves the caller is who they say they are, **not** that the tenant is
//! live, and the factory returns `Option` precisely so a composer can check
//! existence and `is_active()` — *"substrate's factory does not yet."*
//!
//! This module is the composer that does. `plexus-substrate` is a *sibling* of
//! `plexus-idp` (both sit above `plexus-core`; idp does not depend on
//! substrate), so naming it here is the caller taking the obligation, not an
//! inversion.
//!
//! # Two things this module refuses to assume
//!
//! 1. **A `TenantId` is not a path segment.** `TenantId::try_new("../tenant-a")`
//!    is **valid** — it checks non-empty, ≤ 256 bytes and printable ASCII, and
//!    nothing about path safety. `"a/b"`, `".."` and `"/etc"` are valid too.
//!    [`TenantAdmission::ensure_root`] joins an id onto a base directory, which
//!    makes it a **join site**, so it checks for itself with
//!    `plexus_core::plexus::mount_segment_is_safe` *before* the join — and
//!    `TenantRoot::resolve` then checks again by containment after
//!    canonicalizing. Two independent checks, because a defence that works only
//!    because of a later defence is one refactor away from not working.
//!
//! 2. **The caller's id is not the proven id.** The id passed to
//!    `TenantRoot::resolve` is `record.id()` — the one that came back from the
//!    store — never the one the caller handed in. They are equal today; only
//!    one of them is evidence.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use plexus_auth_core::tenant::TenantId;
use plexus_core::plexus::mount_segment_is_safe;
use plexus_idp::store::IdentityStore;
use plexus_sandbox::{SandboxError, TenantRoot};

/// Why a tenant was not admitted to a confinement.
///
/// Every variant is a refusal, not a failure: an operator reading one should
/// learn what to do, and a caller should not be able to tell "suspended" from
/// "never existed" by anything other than the words — see the note on
/// [`AdmissionRefused::is_operator_actionable`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdmissionRefused {
    /// No tenant row. The id may be well-formed and still name nobody.
    #[error("tenant {0:?} does not exist")]
    UnknownTenant(String),

    /// The row exists and the tenant is not active. PLX-126's revocation story
    /// at the execution boundary: the identity survives, the ability to launch
    /// does not.
    #[error("tenant {0:?} is suspended and cannot launch a confined session")]
    Suspended(String),

    /// The id is not usable as a single path segment. Caught *before* the join,
    /// not after.
    #[error(
        "tenant id {0:?} is not usable as a path segment; \
         TenantId::try_new validates length and control characters only"
    )]
    UnsafeSegment(String),

    /// The tenant directory could not be created or resolved.
    #[error("tenant root for {tenant:?} is unusable: {source}")]
    Root {
        tenant: String,
        #[source]
        source: SandboxError,
    },

    /// The identity store could not be consulted. Distinct from "not found":
    /// an unreachable store must never read as an absent tenant.
    #[error("could not consult the identity store for tenant {tenant:?}: {detail}")]
    Store { tenant: String, detail: String },

    /// The tenant root base directory is unusable.
    #[error("tenant root base {base} is unusable: {detail}")]
    Base { base: PathBuf, detail: String },
}

impl AdmissionRefused {
    /// Whether an operator can fix this by changing configuration, as opposed
    /// to a tenant-supplied value being wrong. Used to decide log level, and
    /// nothing else — in particular, *not* to vary what a caller is told.
    #[must_use]
    pub const fn is_operator_actionable(&self) -> bool {
        matches!(self, Self::Store { .. } | Self::Base { .. } | Self::Root { .. })
    }
}

/// Resolves a tenant to the one host directory its confinement may see.
///
/// Construct once per deployment, from the identity store and the base
/// directory that holds every tenant root.
#[derive(Clone)]
pub struct TenantAdmission {
    store: Arc<IdentityStore>,
    base: PathBuf,
}

impl std::fmt::Debug for TenantAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantAdmission")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl TenantAdmission {
    /// `base` is the directory that holds `<base>/<tenant id>` for every
    /// tenant. It is an operator's choice, never a tenant's.
    #[must_use]
    pub fn new(store: Arc<IdentityStore>, base: impl Into<PathBuf>) -> Self {
        Self {
            store,
            base: base.into(),
        }
    }

    /// The base directory that holds every tenant root.
    #[must_use]
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// **The whole point of this module.**
    ///
    /// 1. Resolve the [`TenantRecord`](plexus_idp::tenant::TenantRecord) — the
    ///    sealed proof that the tenant exists.
    /// 2. Check `is_active()`. A suspended tenant does not launch.
    /// 3. Only then build a [`TenantRoot`], from `record.id()`.
    ///
    /// # Errors
    ///
    /// [`AdmissionRefused`], one variant per reason. There is no success path
    /// that skips step 2.
    pub async fn tenant_root(&self, tenant: &TenantId) -> Result<TenantRoot, AdmissionRefused> {
        // 1. the sealed proof, from the store. `find_tenant` is the only way to
        //    obtain a `TenantRecord`; `TenantRecord::seal` is `pub(crate)` to
        //    plexus-idp, so this cannot be fabricated here or in a test.
        let record = self
            .store
            .find_tenant(tenant.as_str())
            .await
            .map_err(|e| AdmissionRefused::Store {
                tenant: tenant.as_str().to_owned(),
                detail: e.to_string(),
            })?
            .ok_or_else(|| AdmissionRefused::UnknownTenant(tenant.as_str().to_owned()))?;

        // 2. …and it must be live. PLX-127 c4's obligation, in one line that a
        //    reader can find.
        if !record.is_active() {
            return Err(AdmissionRefused::Suspended(tenant.as_str().to_owned()));
        }

        // 3. From here on the id is `record.id()`, never the argument. Equal
        //    today; only one of the two is evidence.
        let proven = record.id();
        let root_dir = self.ensure_root(proven)?;

        TenantRoot::resolve(&root_dir.0, proven).map_err(|source| AdmissionRefused::Root {
            tenant: proven.as_str().to_owned(),
            source,
        })
    }

    /// Create `<base>/<tenant>` if it does not exist yet.
    ///
    /// **This is a join site**, so it checks the segment itself rather than
    /// trusting `TenantId`. See the module docs. It returns the *base*, not the
    /// joined path, so that the only path that reaches `TenantRoot::resolve` is
    /// one that gets canonicalized and contained there too.
    fn ensure_root(&self, tenant: &TenantId) -> Result<BaseDir, AdmissionRefused> {
        let segment = tenant.as_str();
        if !mount_segment_is_safe(segment) {
            return Err(AdmissionRefused::UnsafeSegment(segment.to_owned()));
        }

        std::fs::create_dir_all(&self.base).map_err(|e| AdmissionRefused::Base {
            base: self.base.clone(),
            detail: e.to_string(),
        })?;

        // Safe to join: `mount_segment_is_safe` has just refused `/`, `\`, `.`,
        // `..`, `:`, NUL, whitespace and control bytes.
        let joined = self.base.join(segment);
        if !joined.exists() {
            std::fs::create_dir_all(&joined).map_err(|e| AdmissionRefused::Base {
                base: joined.clone(),
                detail: e.to_string(),
            })?;
        }
        Ok(BaseDir(self.base.clone()))
    }
}

/// A base directory, kept newtyped so `ensure_root`'s return value cannot be
/// mistaken for the tenant root it just created — the tenant root is only ever
/// produced by `TenantRoot::resolve`, which canonicalizes and contains.
struct BaseDir(PathBuf);

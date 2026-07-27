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
use plexus_sandbox::{CanonPath, SandboxError, TenantRoot};

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

// ============================================================================
// PLX-129 — the sealed proof that scopes storage
// ============================================================================

/// The directory a tenant's own activation storage lives in, and the proof it
/// was earned.
///
/// # Why this type exists rather than a `&TenantId` argument
///
/// PLX-128's amendment: *inject the proof, not the identifier.* `TenantId` is
/// public data — `TenantId::try_new("../tenant-a")` succeeds, because it
/// validates non-empty, length and control characters and nothing else — so a
/// storage path scoped by a `TenantId` is scoped by whatever the caller
/// managed to spell. There is **no public constructor** here. The only mint
/// path is [`TenantAdmission::tenant_storage`], which resolves the sealed
/// `TenantRecord`, checks `is_active()`, and derives everything below from
/// `record.id()`.
///
/// # The containment, which is not decorative
///
/// PLX-130 measured that `canonicalize` *resolves* a path and does not
/// *constrain* one, and that a rule written against a non-canonical path
/// silently no-ops. So the directory in here is not `base.join(id).join(…)`
/// spelled out; it is [`TenantRoot::contain`]'s output — canonicalized and
/// then compared against the canonicalized tenant root — which is the same
/// two-step primitive PLX-144 built and PLX-151 measured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantStorageRoot {
    tenant: TenantId,
    /// `<tenant root>/storage`, canonical and proven inside the tenant root.
    storage: CanonPath,
    /// `<tenant root>/storage/activations`, likewise.
    activations: CanonPath,
}

impl TenantStorageRoot {
    /// The subdirectory of a tenant root that holds plexus's own state. A
    /// literal, not caller input.
    const STORAGE_DIR: &'static str = "storage";
    /// Where the per-activation sqlite files go. Also a literal.
    const ACTIVATIONS_DIR: &'static str = "activations";

    /// The tenant this root belongs to — `record.id()`, never a caller's
    /// spelling of it.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// `<tenant root>/storage`.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.storage.as_path()
    }

    /// `<tenant root>/storage/activations` — what
    /// [`StorageScope::for_tenant`](crate::activations::storage::StorageScope::for_tenant)
    /// hands to every activation.
    #[must_use]
    pub fn activations_dir(&self) -> &Path {
        self.activations.as_path()
    }
}

impl TenantAdmission {
    /// **PLX-129's mint path.** The tenant's own storage directory, or a
    /// refusal.
    ///
    /// Identical in shape to [`TenantAdmission::tenant_root`] and built on top
    /// of it, so there is one place that consults the identity store and one
    /// place that checks `is_active()`:
    ///
    /// 1. `tenant_root` — sealed `TenantRecord`, `is_active()`, canonical
    ///    `<base>/<id>` contained inside `<base>`;
    /// 2. create `<root>/storage/activations` if absent — two literal segments,
    ///    no caller input anywhere in the join;
    /// 3. `TenantRoot::contain` each one, which canonicalizes and *then*
    ///    compares, so a symlink planted at `<root>/storage` is refused rather
    ///    than followed out of the root.
    ///
    /// Step 3 is not redundant with step 2. A tenant that can write inside its
    /// own root (which is the entire point of a tenant root) can plant that
    /// symlink between two runs.
    ///
    /// # Errors
    ///
    /// [`AdmissionRefused`] — the same variants as `tenant_root`, plus
    /// [`AdmissionRefused::Base`] if the storage subdirectory cannot be made
    /// and [`AdmissionRefused::Root`] if it does not stay inside the tenant
    /// root.
    pub async fn tenant_storage(
        &self,
        tenant: &TenantId,
    ) -> Result<TenantStorageRoot, AdmissionRefused> {
        let root = self.tenant_root(tenant).await?;
        Self::storage_within(&root)
    }

    /// The half of [`TenantAdmission::tenant_storage`] that runs once the
    /// record has been proved, factored out so a caller that already holds a
    /// [`TenantRoot`] (the confined `claudecode` launcher does) does not pay
    /// for a second store round-trip.
    ///
    /// # Errors
    ///
    /// As [`TenantAdmission::tenant_storage`].
    pub fn storage_within(root: &TenantRoot) -> Result<TenantStorageRoot, AdmissionRefused> {
        let tenant = root.tenant().clone();
        let refuse_root = |source: SandboxError| AdmissionRefused::Root {
            tenant: tenant.as_str().to_owned(),
            source,
        };
        let refuse_base = |p: &Path, e: &std::io::Error| AdmissionRefused::Base {
            base: p.to_path_buf(),
            detail: e.to_string(),
        };

        // Two literal segments. `mount_segment_is_safe` has nothing to say
        // about a constant, and the containment below is what actually holds.
        let storage_dir = root.path().as_path().join(TenantStorageRoot::STORAGE_DIR);
        let activations_dir = storage_dir.join(TenantStorageRoot::ACTIVATIONS_DIR);
        std::fs::create_dir_all(&activations_dir)
            .map_err(|e| refuse_base(&activations_dir, &e))?;

        // Resolve, THEN constrain — both of them, against the canonical root.
        let storage = root.contain(&storage_dir).map_err(refuse_root)?;
        let activations = root.contain(&activations_dir).map_err(refuse_root)?;

        Ok(TenantStorageRoot {
            tenant,
            storage,
            activations,
        })
    }

    /// Remove a tenant's entire storage tree, returning whether anything was
    /// there.
    ///
    /// This is the filesystem half of PLX-131's residual — see
    /// [`crate::tenancy::SubstrateTenantStorage`].
    ///
    /// # Errors
    ///
    /// [`AdmissionRefused`] if the root cannot be resolved or removal fails.
    /// A tenant with no root yet is **not** an error: it is `Ok(false)`.
    pub fn remove_storage(&self, tenant: &TenantId) -> Result<bool, AdmissionRefused> {
        // Deliberately does NOT go through `tenant_root`: by the time storage
        // is reaped the record is gone, so demanding the record would make the
        // obligation undischargeable. The safety obligation is unchanged and is
        // met the same way — segment check before the join, canonicalize and
        // contain after — just without the existence proof, which deletion has
        // already consumed.
        let segment = tenant.as_str();
        if !mount_segment_is_safe(segment) {
            return Err(AdmissionRefused::UnsafeSegment(segment.to_owned()));
        }
        let base = match CanonPath::resolve_dir(&self.base) {
            Ok(b) => b,
            // No base directory at all: nothing was ever written.
            Err(_) => return Ok(false),
        };
        let joined = base.as_path().join(segment);
        let canonical = match CanonPath::resolve_dir(&joined) {
            Ok(c) => c,
            Err(_) => return Ok(false),
        };
        if !canonical.as_path().starts_with(base.as_path()) {
            return Err(AdmissionRefused::Root {
                tenant: segment.to_owned(),
                source: SandboxError::PathEscape {
                    attempted: joined,
                    canonical: canonical.into_path_buf(),
                    root: base.into_path_buf(),
                },
            });
        }
        std::fs::remove_dir_all(canonical.as_path()).map_err(|e| AdmissionRefused::Base {
            base: canonical.as_path().to_path_buf(),
            detail: e.to_string(),
        })?;
        Ok(true)
    }
}

// ============================================================================
// PLX-131's residual, picked up rather than left as a doc comment
// ============================================================================

/// Substrate's implementation of plexus-idp's
/// [`TenantStorageReaper`](plexus_idp::TenantStorageReaper).
///
/// # The obligation this discharges
///
/// PLX-131 shipped `TenantDeleted.per_tenant_storage_removed` as a **wire
/// field that is `false`**, asserted false by test, and said why in
/// `IdentityCore::delete_tenant`'s own rustdoc: *"as of M4·G there is no
/// per-tenant file to remove … when PLX-128/PLX-129 give a tenant storage of
/// its own, this method acquires an obligation it does not have today."*
///
/// PLX-129 gives a tenant storage of its own. The obligation is now real, and
/// this is it: plexus-idp defines the trait (it owns the wire field and the
/// transaction), substrate implements it (it owns the directory). The
/// dependency runs substrate → idp, the same sibling direction PLX-151
/// established, so nothing inverts.
///
/// A deployment that installs no reaper still answers `false`, honestly, and
/// that is what PLX-131's original assertion pins.
#[derive(Debug, Clone)]
pub struct SubstrateTenantStorage {
    admission: Arc<TenantAdmission>,
}

impl SubstrateTenantStorage {
    /// Reap through this admission's base directory.
    #[must_use]
    pub const fn new(admission: Arc<TenantAdmission>) -> Self {
        Self { admission }
    }
}

#[async_trait::async_trait]
impl plexus_idp::TenantStorageReaper for SubstrateTenantStorage {
    async fn remove_tenant_storage(&self, tenant: &TenantId) -> Result<bool, String> {
        self.admission
            .remove_storage(tenant)
            .map_err(|e| e.to_string())
    }
}

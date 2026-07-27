//! PLX-129 c3, part 2 — and no door through the fields either.
//!
//! Both values here typecheck, so the only thing standing in the way is field
//! visibility. That is the seal.

use plexus_auth_core::tenant::TenantId;
use plexus_substrate::TenantStorageRoot;

fn main() {
    let id = TenantId::try_new("anything").expect("valid");
    let canon = plexus_sandbox::CanonPath::resolve("/tmp").expect("canonical");

    let _ = TenantStorageRoot {
        tenant: id,
        storage: canon.clone(),
        activations: canon,
    };
}

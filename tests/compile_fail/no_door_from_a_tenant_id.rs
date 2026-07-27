//! PLX-129 c3, part 1 — there is no constructor that takes an identifier.
//!
//! `TenantId::try_new("../tenant-a")` succeeds. If a `TenantId` could scope
//! storage, storage would be scoped by whatever a caller managed to spell.

use plexus_auth_core::tenant::TenantId;
use plexus_substrate::{StorageScope, TenantStorageRoot};

fn main() {
    let id = TenantId::try_new("../tenant-a").expect("try_new ACCEPTS this");

    // The convenience that must not exist.
    let _ = StorageScope::for_tenant_id(&id);

    // The identifier passed where the proof goes.
    let _ = StorageScope::for_tenant(&id);

    // Asking the proof for a constructor.
    let _ = TenantStorageRoot::new(&id, "/tmp");
}

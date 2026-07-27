//! PLX-129 c3 — *"any shared-database access is behind a wrapper that cannot
//! be constructed without a `TenantId`, shown by a compile-fail test."*
//!
//! The bar is actually higher than the criterion asks, and the difference is
//! PLX-128's amendment: the wrapper cannot be constructed without a
//! **`TenantRecord`**, not merely without a `TenantId`. A `TenantId` is public
//! data — `TenantId::try_new("../tenant-a")` succeeds — so a wrapper gated on
//! one would be gated on nothing. [`TenantStorageRoot`] is gated on the sealed
//! record, via `TenantAdmission::tenant_storage`, and this test is the
//! demonstration that no other door exists.
//!
//! # Where the shared database is, and is not
//!
//! The criterion's wording anticipates shared Postgres. **There is none in
//! substrate** — `q-tenant-isolation` chose per-tenant sqlite files and that is
//! what shipped, so the "shared database" this seals is the process-global
//! sqlite set that every tenant used to receive. The sealed wrapper is what
//! stands between a composition and those files.

#[test]
fn a_storage_scope_cannot_be_built_without_the_sealed_proof() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}

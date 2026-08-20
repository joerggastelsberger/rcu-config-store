//! Read-copy-update configuration store: wait-free snapshot reads through an
//! atomic pointer, updates by pointer swap, and deferred reclamation through
//! an SPSC queue drained by a grace-period reclaimer thread.
//!
//! Layering:
//!
//! - [`RcuCell`] — the mechanism. Sound on its own: publishing is an atomic
//!   swap, and the only way to free a retired snapshot is the `unsafe`
//!   [`Retired::reclaim`], whose contract names the one obligation.
//! - [`store`] — the policy. Splits the cell into a single [`Writer`] and
//!   clonable [`Reader`]s and discharges the reclaim obligation with a
//!   per-snapshot grace period (a heuristic; see the README).
//! - [`arcswap`] — the same handle API over the `arc-swap` crate, which
//!   solves reclamation exactly. Kept as the benchmark baseline.
//! - [`spsc`] — the reclaim channel, vendored from the standalone `spsc`
//!   crate.

pub mod arcswap;
mod cell;
pub mod spsc;
mod store;
mod sync;

pub use cell::{RcuCell, Retired};
pub use store::{Reader, Writer, store, store_with};

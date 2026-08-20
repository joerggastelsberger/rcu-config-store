//! Read-copy-update configuration store: wait-free snapshot reads through an
//! atomic pointer, updates by pointer swap, and deferred reclamation through
//! an SPSC queue drained by a grace-period reclaimer thread.

mod cell;
mod sync;

pub mod spsc;

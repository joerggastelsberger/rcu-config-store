//! Baseline variant backed by the `arc-swap` crate, same handle API as the
//! atomic-pointer store so the benchmark compares like for like.
//!
//! `arc-swap` solves reclamation exactly, not heuristically: a load hands out
//! a guard, and the old `Arc` is freed only after every guard is gone. The
//! price is paid on both paths — reads go through arc-swap's debt machinery
//! instead of a single `Acquire` load, and `store` walks the debt lists to
//! settle outstanding guards. The benchmark measures that price.

use std::sync::Arc;

use arc_swap::ArcSwap;

/// Creates a store backed by `ArcSwap`. No reclaimer thread: reclamation is
/// handled by `arc-swap` itself.
pub fn store<T: Copy + Send + Sync + 'static>(initial: T) -> (Writer<T>, Reader<T>) {
    let shared = Arc::new(ArcSwap::from_pointee(initial));
    (
        Writer {
            shared: Arc::clone(&shared),
        },
        Reader { shared },
    )
}

/// The write end. `update` takes `&mut self` to match the atomic-pointer
/// store's contract, though `ArcSwap` itself would tolerate many writers.
pub struct Writer<T> {
    shared: Arc<ArcSwap<T>>,
}

impl<T: Copy + Send + Sync + 'static> Writer<T> {
    pub fn update(&mut self, value: T) {
        self.shared.store(Arc::new(value));
    }
}

/// The read end. Clonable; `read` copies the snapshot out while the guard
/// lives only for the duration of the call.
pub struct Reader<T> {
    shared: Arc<ArcSwap<T>>,
}

impl<T: Copy + Send + Sync + 'static> Reader<T> {
    pub fn read(&self) -> T {
        **self.shared.load()
    }
}

impl<T> Clone for Reader<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

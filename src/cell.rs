//! The RCU mechanism: an atomic pointer to an immutable heap snapshot.
//!
//! [`RcuCell`] is sound on its own — the only way to free a retired snapshot
//! is [`Retired::reclaim`], which is `unsafe` and states the one obligation
//! (no reader may still hold the node). Reclamation *policy* — deciding when
//! that obligation holds — lives in [`crate::store`].

use crate::sync::{AtomicPtr, Ordering, UnsafeCell};

/// Heap node holding one immutable snapshot. Never written after publication;
/// the `UnsafeCell` exists so loom can instrument the cross-thread read.
struct Node<T> {
    value: UnsafeCell<T>,
}

/// Atomic pointer to the active snapshot.
///
/// Readers pay one `Acquire` load and copy the snapshot out — no lock, no
/// CAS, no reference count — so a multi-field snapshot is always internally
/// consistent: a reader sees the whole old value or the whole new one, never
/// a mix. Writers allocate a new snapshot and swing the pointer with a single
/// atomic swap.
pub struct RcuCell<T> {
    active: AtomicPtr<Node<T>>,
}

// Readers on other threads obtain copies of `T` (Send) through a shared
// reference (Sync).
unsafe impl<T: Send> Send for RcuCell<T> {}
unsafe impl<T: Send + Sync> Sync for RcuCell<T> {}

impl<T: Copy> RcuCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            active: AtomicPtr::new(Box::into_raw(Box::new(Node {
                value: UnsafeCell::new(value),
            }))),
        }
    }

    /// Copies the current snapshot out. Wait-free: one `Acquire` load plus a
    /// memcpy of `T`.
    pub fn read(&self) -> T {
        // Acquire pairs with the Release half of `swap`: observing the new
        // pointer implies observing the fully initialized node behind it.
        let node = self.active.load(Ordering::Acquire);
        unsafe { (*node).value.with(|p| *p) }
    }

    /// Publishes `value` as the new snapshot and returns the retired one.
    ///
    /// The retired node stays allocated — readers that loaded the old pointer
    /// just before the swap may still be copying from it. The caller decides
    /// when that can no longer be true and frees it with [`Retired::reclaim`].
    pub fn swap(&self, value: T) -> Retired<T> {
        let new = Box::into_raw(Box::new(Node {
            value: UnsafeCell::new(value),
        }));
        // Release publishes the node initialization to readers' Acquire
        // loads; Acquire orders the handover of the retired node to us.
        let old = self.active.swap(new, Ordering::AcqRel);
        Retired { node: old }
    }
}

impl<T> Drop for RcuCell<T> {
    fn drop(&mut self) {
        // `&mut self`: no readers can exist; the active node is ours to free.
        let node = self.active.load(Ordering::Relaxed);
        unsafe { drop(Box::from_raw(node)) };
    }
}

/// A snapshot removed from the cell by [`RcuCell::swap`] but not yet freed.
///
/// Dropping a `Retired` without calling [`reclaim`](Retired::reclaim) leaks
/// the node — safe, and deliberately so: freeing is the dangerous direction.
pub struct Retired<T> {
    node: *mut Node<T>,
}

// The retired snapshot is freed on the reclaimer thread.
unsafe impl<T: Send> Send for Retired<T> {}

impl<T> Retired<T> {
    /// Frees the retired snapshot.
    ///
    /// # Safety
    ///
    /// No thread may still hold the retired pointer: every reader that loaded
    /// it must have finished copying the snapshot out. The store layer
    /// establishes this with a grace period; callers using `RcuCell` directly
    /// must establish it themselves (e.g. by joining the reader threads).
    pub unsafe fn reclaim(self) {
        unsafe { drop(Box::from_raw(self.node)) };
    }
}

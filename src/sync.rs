//! Shim over the concurrency primitives so the same source compiles against
//! `std` normally and against `loom` when model checking (`--cfg loom`).

#[cfg(loom)]
pub(crate) use loom::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc,
    atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// Mirror of `loom::cell::UnsafeCell`'s closure-based API, so accesses are
/// written once and loom can instrument them under `--cfg loom`.
#[cfg(not(loom))]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    #[inline]
    pub(crate) fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    #[inline]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    #[inline]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

//! Bounded lock-free SPSC ring buffer, used as the reclaim channel between
//! the writer and the background reclaimer.
//!
//! Vendored from the standalone `spsc` crate (padded variant): producer and
//! consumer indices live on separate cache lines so the two hot loops are
//! independent at the coherence level.

use std::mem::MaybeUninit;

use crate::sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

/// An index alone on its own cache line.
///
/// 128 rather than 64: Apple Silicon uses 128-byte lines, and on x86_64 the
/// adjacent-line prefetcher makes 128 bytes the effective destructive
/// interference range (same reasoning as crossbeam's `CachePadded`).
#[repr(align(128))]
struct CacheAligned(AtomicUsize);

struct Ring<T> {
    /// Next slot to write. Written only by the producer.
    head: CacheAligned,
    /// Next slot to read. Written only by the consumer.
    tail: CacheAligned,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
}

// A `T` is handed from producer to consumer exactly once, so `T: Send` is
// sufficient. The handles enforce single-producer/single-consumer.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

/// Creates a bounded SPSC channel with `capacity` slots, all usable.
///
/// Indices grow without bound and are masked on access, so full/empty are
/// distinguished by `head - tail` and no slot is sacrificed.
///
/// # Panics
///
/// Panics if `capacity` is zero or not a power of two.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two(),
        "capacity must be a nonzero power of two"
    );

    let ring = Arc::new(Ring {
        head: CacheAligned(AtomicUsize::new(0)),
        tail: CacheAligned(AtomicUsize::new(0)),
        buffer: (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect(),
        cap: capacity,
    });

    (
        Producer {
            ring: Arc::clone(&ring),
        },
        Consumer { ring },
    )
}

/// The write end. Not clonable; `push` takes `&mut self`, so there is exactly
/// one producer.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
}

impl<T> Producer<T> {
    /// Attempts to push, returning `Err(value)` if the buffer is full.
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let ring = &*self.ring;
        // Relaxed: only this thread writes `head`.
        let head = ring.head.0.load(Ordering::Relaxed);
        // Acquire pairs with the consumer's Release store of `tail`: once we
        // observe a slot as consumed, the consumer's read of it has completed
        // and the slot may be overwritten.
        let tail = ring.tail.0.load(Ordering::Acquire);

        if head.wrapping_sub(tail) == ring.cap {
            return Err(value);
        }

        let slot = &ring.buffer[head & (ring.cap - 1)];
        slot.with_mut(|p| unsafe { (*p).write(value) });

        // Release publishes the slot write above to the consumer's Acquire
        // load of `head`.
        ring.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

/// The read end. Not clonable; `pop` takes `&mut self`, so there is exactly
/// one consumer.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

impl<T> Consumer<T> {
    /// Attempts to pop, returning `None` if the buffer is empty.
    pub fn pop(&mut self) -> Option<T> {
        let ring = &*self.ring;
        // Relaxed: only this thread writes `tail`.
        let tail = ring.tail.0.load(Ordering::Relaxed);
        // Acquire pairs with the producer's Release store of `head`, making
        // the slot write visible before we read it.
        let head = ring.head.0.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let slot = &ring.buffer[tail & (ring.cap - 1)];
        let value = slot.with(|p| unsafe { (*p).assume_init_read() });

        // Release hands the slot back to the producer's Acquire load of
        // `tail`.
        ring.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Runs when the last handle drops; access is exclusive by then.
        let head = self.head.0.load(Ordering::Relaxed);
        let mut tail = self.tail.0.load(Ordering::Relaxed);
        while tail != head {
            self.buffer[tail & (self.cap - 1)].with_mut(|p| unsafe { (*p).assume_init_drop() });
            tail = tail.wrapping_add(1);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::mem::offset_of;

    /// Generates (rather than asserts by hand) the layout claim: head and
    /// tail sit on distinct cache lines.
    #[test]
    fn head_and_tail_on_separate_cache_lines() {
        let head = offset_of!(Ring<u64>, head);
        let tail = offset_of!(Ring<u64>, tail);
        assert_eq!(head % 128, 0, "head offset {head} not line-aligned");
        assert_eq!(tail % 128, 0, "tail offset {tail} not line-aligned");
        assert!(
            head.abs_diff(tail) >= 128,
            "head at {head} and tail at {tail} share a cache line"
        );
    }
}

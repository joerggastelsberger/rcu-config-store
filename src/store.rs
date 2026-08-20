//! The reclamation policy: a grace-period reclaimer behind an SPSC queue.
//!
//! [`store`] splits the cell into a single [`Writer`] and clonable
//! [`Reader`]s. Each update retires the previous snapshot with a timestamp
//! into an SPSC queue; a background reclaimer thread frees a retired snapshot
//! only once it has aged at least the grace period, and shuts down cleanly
//! when the `Writer` drops.
//!
//! The grace period is a heuristic, not a proof: a reader preempted for
//! longer than the grace period between loading the pointer and copying the
//! snapshot would read freed memory. Epoch-based reclamation replaces the
//! heuristic with tracking; see the README.

use std::collections::VecDeque;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::cell::{RcuCell, Retired};
use crate::spsc;

const DEFAULT_GRACE_PERIOD: Duration = Duration::from_millis(100);
const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// A retired snapshot plus the moment it left the cell; the reclaimer frees
/// it only once `retired_at` is at least a grace period old.
struct Stamped<T> {
    retired: Retired<T>,
    retired_at: Instant,
}

/// Creates a store with a 100 ms grace period and a 1024-slot reclaim queue.
pub fn store<T: Copy + Send + Sync + 'static>(initial: T) -> (Writer<T>, Reader<T>) {
    store_with(initial, DEFAULT_GRACE_PERIOD, DEFAULT_QUEUE_CAPACITY)
}

/// Creates a store with an explicit grace period and reclaim-queue capacity.
///
/// The queue must absorb every update issued within roughly two grace
/// periods; when it fills, `update` spins until the reclaimer drains it.
///
/// # Panics
///
/// Panics if `queue_capacity` is zero or not a power of two.
pub fn store_with<T: Copy + Send + Sync + 'static>(
    initial: T,
    grace_period: Duration,
    queue_capacity: usize,
) -> (Writer<T>, Reader<T>) {
    let cell = Arc::new(RcuCell::new(initial));
    let (tx, rx) = spsc::channel::<Stamped<T>>(queue_capacity);
    let stop = Arc::new(AtomicBool::new(false));
    let reclaimer = spawn_reclaimer(rx, grace_period, Arc::clone(&stop));

    (
        Writer {
            cell: Arc::clone(&cell),
            tx,
            stop,
            reclaimer: Some(reclaimer),
        },
        Reader { cell },
    )
}

/// The write end. Not clonable; `update` takes `&mut self`, so there is
/// exactly one writer — which is also what makes the SPSC reclaim queue
/// sufficient.
pub struct Writer<T: Copy + Send + Sync + 'static> {
    cell: Arc<RcuCell<T>>,
    tx: spsc::Producer<Stamped<T>>,
    stop: Arc<AtomicBool>,
    reclaimer: Option<JoinHandle<()>>,
}

impl<T: Copy + Send + Sync + 'static> Writer<T> {
    /// Publishes `value` as the new snapshot and queues the previous one for
    /// reclamation. Spins if the reclaim queue is full.
    pub fn update(&mut self, value: T) {
        let mut stamped = Stamped {
            retired: self.cell.swap(value),
            retired_at: Instant::now(),
        };

        while let Err(back) = self.tx.push(stamped) {
            // Queue full: wake the reclaimer early instead of waiting out its
            // park timeout.
            if let Some(reclaimer) = &self.reclaimer {
                reclaimer.thread().unpark();
            }
            spin_loop();
            stamped = back;
        }
    }
}

impl<T: Copy + Send + Sync + 'static> Drop for Writer<T> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(reclaimer) = self.reclaimer.take() {
            reclaimer.thread().unpark();
            let _ = reclaimer.join();
        }
    }
}

/// The read end. Clonable; `read` takes `&self` and is wait-free.
pub struct Reader<T> {
    cell: Arc<RcuCell<T>>,
}

impl<T: Copy> Reader<T> {
    /// Copies the current snapshot out: one `Acquire` load plus a memcpy.
    pub fn read(&self) -> T {
        self.cell.read()
    }
}

impl<T> Clone for Reader<T> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
        }
    }
}

/// Drains the queue into an age-ordered pending list and frees entries only
/// once they are at least `grace_period` old — the grace period is a minimum
/// per snapshot, not a drain interval.
fn spawn_reclaimer<T: Send + 'static>(
    mut rx: spsc::Consumer<Stamped<T>>,
    grace_period: Duration,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        // Push order equals retirement order, so `pending` is oldest-first.
        let mut pending: VecDeque<Stamped<T>> = VecDeque::new();

        loop {
            // Read the flag before draining: everything retired before the
            // writer set it is caught by this drain or the final one below.
            let stopping = stop.load(Ordering::Acquire);

            while let Some(stamped) = rx.pop() {
                pending.push_back(stamped);
            }
            while pending
                .front()
                .is_some_and(|s| s.retired_at.elapsed() >= grace_period)
            {
                let stamped = pending.pop_front().unwrap();
                // SAFETY: the snapshot has been out of the cell for a full
                // grace period; per the store's contract no reader still
                // holds it.
                unsafe { stamped.retired.reclaim() };
            }

            if stopping {
                // The writer is gone, so no further retirements: wait out the
                // newest entry's remaining age, then free everything.
                while let Some(stamped) = rx.pop() {
                    pending.push_back(stamped);
                }
                if let Some(newest) = pending.back() {
                    thread::sleep(grace_period.saturating_sub(newest.retired_at.elapsed()));
                }
                for stamped in pending.drain(..) {
                    // SAFETY: aged past the grace period, as above.
                    unsafe { stamped.retired.reclaim() };
                }
                return;
            }

            // Woken early by the writer on queue-full or on shutdown.
            thread::park_timeout(grace_period);
        }
    })
}

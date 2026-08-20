#![cfg(not(loom))]

use std::thread;
use std::time::Duration;

use rcu_config_store::{RcuCell, arcswap, store, store_with};

// Miri executes these tests; keep its iteration counts small.
const N: usize = if cfg!(miri) { 200 } else { 100_000 };

#[test]
fn initial_value_is_readable() {
    let (_w, r) = store([1usize, 2, 3, 4]);
    assert_eq!(r.read(), [1, 2, 3, 4]);
}

#[test]
fn update_is_visible_to_subsequent_reads() {
    let (mut w, r) = store(0u64);
    w.update(7);
    assert_eq!(r.read(), 7);
    w.update(8);
    assert_eq!(r.read(), 8);
}

/// The RCU consistency property: a snapshot is copied whole, so readers must
/// never observe a mix of two updates. Each update maintains `b == a * 2`.
///
/// Ignored under miri, and the reason is a finding, not a workaround: the
/// timer-based grace period creates no happens-before edge between a reader's
/// snapshot copy and the reclaimer's free, so miri's vector-clock race
/// detector correctly reports the free as a data race no matter how much
/// wall-clock time separates them. That formal gap is exactly what
/// epoch-based reclamation closes. `cell_snapshots_are_never_torn_with_join`
/// below checks the same property with join-based reclamation, which has the
/// edge and is miri-clean.
#[test]
#[cfg_attr(
    miri,
    ignore = "timer-based reclamation has no happens-before edge to readers; see comment"
)]
fn snapshots_are_never_torn() {
    let (mut w, r) = store_with((0usize, 0usize), Duration::from_millis(1), 1 << 14);

    thread::scope(|s| {
        for _ in 0..4 {
            let r = r.clone();
            s.spawn(move || {
                for _ in 0..N {
                    let (a, b) = r.read();
                    assert_eq!(b, a * 2, "torn snapshot: ({a}, {b})");
                }
            });
        }

        s.spawn(move || {
            for i in 1..=N {
                w.update((i, i * 2));
            }
        });
    });
}

/// More updates than the queue holds: completes only if the reclaimer keeps
/// draining (and the writer's queue-full unpark path works).
#[test]
fn reclaimer_keeps_up_with_a_small_queue() {
    let updates = if cfg!(miri) { 40 } else { 10_000 };
    let (mut w, r) = store_with(0usize, Duration::from_millis(1), 16);
    for i in 1..=updates {
        w.update(i);
    }
    assert_eq!(r.read(), updates);
}

/// Dropping the writer joins the reclaimer and frees every retired snapshot;
/// miri's leak checker verifies on top of this test.
#[test]
fn drop_reclaims_everything() {
    let (mut w, r) = store_with(0usize, Duration::from_millis(1), 64);
    for i in 0..10 {
        w.update(i);
    }
    drop(w);
    drop(r);
}

#[test]
fn drop_without_updates() {
    let (w, r) = store(42u32);
    drop(r);
    drop(w);
}

#[test]
fn readers_outlive_the_writer() {
    let (mut w, r) = store(1usize);
    w.update(2);
    drop(w);
    assert_eq!(r.read(), 2);
    assert_eq!(r.clone().read(), 2);
}

/// Same torn-snapshot property as above, at the `RcuCell` level with
/// join-based reclamation: retired snapshots are freed only after every
/// thread has joined, so the frees are ordered after all reads and miri can
/// race-check the concurrent read/swap paths themselves.
#[test]
fn cell_snapshots_are_never_torn_with_join() {
    let n = if cfg!(miri) { 50 } else { 10_000 };
    let cell = std::sync::Arc::new(RcuCell::new((0usize, 0usize)));

    let retired = thread::scope(|s| {
        for _ in 0..4 {
            let cell = std::sync::Arc::clone(&cell);
            s.spawn(move || {
                for _ in 0..n {
                    let (a, b) = cell.read();
                    assert_eq!(b, a * 2, "torn snapshot: ({a}, {b})");
                }
            });
        }

        let cell = std::sync::Arc::clone(&cell);
        s.spawn(move || (1..=n).map(|i| cell.swap((i, i * 2))).collect::<Vec<_>>())
            .join()
            .unwrap()
    });

    for r in retired {
        // SAFETY: `thread::scope` joined every reader before returning.
        unsafe { r.reclaim() };
    }
}

#[test]
fn cell_swap_and_read() {
    let cell = RcuCell::new(1u64);
    assert_eq!(cell.read(), 1);
    let retired = cell.swap(2);
    assert_eq!(cell.read(), 2);
    // SAFETY: single-threaded — no reader can still hold the old snapshot.
    unsafe { retired.reclaim() };
}

#[test]
fn arcswap_store_matches_semantics() {
    let (mut w, r) = arcswap::store((0usize, 0usize));
    w.update((3, 6));
    assert_eq!(r.read(), (3, 6));
    let r2 = r.clone();
    drop(w);
    assert_eq!(r2.read(), (3, 6));
}

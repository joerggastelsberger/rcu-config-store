//! Tests for the vendored SPSC ring buffer (the reclaim channel).

#![cfg(not(loom))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use rcu_config_store::spsc;

// Miri executes these tests; keep its iteration count small.
const N: usize = if cfg!(miri) { 500 } else { 100_000 };

#[test]
fn fifo_across_threads() {
    let (mut tx, mut rx) = spsc::channel::<usize>(64);
    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..N {
                while tx.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        s.spawn(move || {
            for expected in 0..N {
                let value = loop {
                    if let Some(v) = rx.pop() {
                        break v;
                    }
                    std::hint::spin_loop();
                };
                assert_eq!(value, expected);
            }
        });
    });
}

#[test]
fn full_capacity_is_usable() {
    let (mut tx, mut rx) = spsc::channel::<u32>(4);
    for i in 0..4 {
        assert_eq!(tx.push(i), Ok(()));
    }
    assert_eq!(tx.push(99), Err(99));
    for i in 0..4 {
        assert_eq!(rx.pop(), Some(i));
    }
    assert_eq!(rx.pop(), None);
}

#[test]
fn wraparound_preserves_order() {
    let (mut tx, mut rx) = spsc::channel::<usize>(4);
    for i in 0..32 {
        assert_eq!(tx.push(i), Ok(()));
        assert_eq!(tx.push(i + 100), Ok(()));
        assert_eq!(rx.pop(), Some(i));
        assert_eq!(rx.pop(), Some(i + 100));
    }
}

#[test]
fn unconsumed_items_are_dropped() {
    struct CountsDrop(Arc<AtomicUsize>);
    impl Drop for CountsDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = spsc::channel::<CountsDrop>(8);
    for _ in 0..5 {
        assert!(tx.push(CountsDrop(Arc::clone(&drops))).is_ok());
    }
    drop(rx.pop());
    assert_eq!(drops.load(Ordering::Relaxed), 1);

    drop(tx);
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 5);
}

#[test]
#[should_panic(expected = "power of two")]
fn non_power_of_two_capacity_panics() {
    let _ = spsc::channel::<u8>(24);
}

#[test]
#[should_panic(expected = "power of two")]
fn zero_capacity_panics() {
    let _ = spsc::channel::<u8>(0);
}

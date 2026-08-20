//! Model checks: `RUSTFLAGS="--cfg loom" cargo test --test loom --release`
//!
//! Loom explores thread interleavings under the C11 memory model. It cannot
//! model wall-clock time, so the grace-period reclaimer is out of scope here
//! (miri and the std tests cover it); what loom checks is the part that has
//! to be argued rather than measured:
//!
//! - the `RcuCell` publication protocol: the Release half of `swap` makes the
//!   snapshot initialization visible to a reader's Acquire load, on every
//!   interleaving (retired nodes are freed after joining, standing in for an
//!   ideal grace period);
//! - the SPSC reclaim channel: the acquire/release pairing on `head`/`tail`
//!   and every `UnsafeCell` access, at capacity 2 with 3 items to force the
//!   full, empty, and wraparound paths;
//! - leak-freedom of both drop paths (loom's leak checker).

#![cfg(loom)]

use loom::sync::Arc;
use loom::thread;

use rcu_config_store::{RcuCell, spsc};

/// A reader racing two updates must always see a whole snapshot — `(a, 2a)`
/// from exactly the initial value or one of the updates, never a mix.
#[test]
fn cell_snapshots_are_never_torn() {
    loom::model(|| {
        let cell = Arc::new(RcuCell::new((0usize, 0usize)));
        let writer_cell = Arc::clone(&cell);

        let writer = thread::spawn(move || {
            (1..=2)
                .map(|i| writer_cell.swap((i, i * 2)))
                .collect::<Vec<_>>()
        });

        let (a, b) = cell.read();
        assert_eq!(b, a * 2, "torn snapshot: ({a}, {b})");
        assert!(a <= 2);

        let retired = writer.join().unwrap();
        for r in retired {
            // SAFETY: the writer has joined and this thread reads no more;
            // no one still holds the retired snapshots.
            unsafe { r.reclaim() };
        }
    });
}

#[test]
fn spsc_push_pop_interleavings() {
    const ITEMS: usize = 3;
    const CAPACITY: usize = 2;

    loom::model(|| {
        let (mut tx, mut rx) = spsc::channel::<usize>(CAPACITY);

        let producer = thread::spawn(move || {
            for i in 0..ITEMS {
                while tx.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        for expected in 0..ITEMS {
            let value = loop {
                if let Some(v) = rx.pop() {
                    break v;
                }
                thread::yield_now();
            };
            assert_eq!(value, expected);
        }

        producer.join().unwrap();
    });
}

#[test]
fn spsc_drop_reclaims_unconsumed_items() {
    loom::model(|| {
        let (mut tx, rx) = spsc::channel::<Box<usize>>(2);

        let producer = thread::spawn(move || {
            let _ = tx.push(Box::new(1));
            let _ = tx.push(Box::new(2));
        });

        // Dropping the consumer while pushes race must still free every
        // published item (loom's leak checker verifies).
        drop(rx);
        producer.join().unwrap();
    });
}

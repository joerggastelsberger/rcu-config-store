//! Atomic-pointer store vs arc-swap baseline, write path and read path.
//!
//! Barrier-synchronized `iter_custom` timing: all threads park on a start
//! barrier, the coordinator stamps the clock, and a stop barrier closes the
//! measurement — thread spawn and join never land inside the timed region.
//! Both variants run through the same harness, so their load profiles are
//! structurally identical.
//!
//! Write path: the writer executes exactly `iters` updates while `READERS`
//! readers spin; reported time is per update under maximum read pressure.
//! Read path: one measured reader executes exactly `iters` reads while the
//! writer updates continuously; reported time is per read under write churn.

use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use rcu_config_store::{arcswap, store_with};

const READERS: usize = 6;

/// Grace period for the atomic-pointer variant. The queue must absorb the
/// updates issued while retirees age, hence the large capacity: at ~10 M
/// updates/s and a 100 ms grace period, up to ~2 M snapshots are in flight.
const GRACE_PERIOD: Duration = Duration::from_millis(100);
const QUEUE_CAPACITY: usize = 1 << 22;

type Config = [usize; 4];

/// Times exactly `iters` writer updates under continuous load from `READERS`
/// reader threads.
fn run_write_under_read_load(
    iters: u64,
    mut write: impl FnMut(u64) + Send,
    read: impl Fn() + Sync,
) -> Duration {
    // 1 writer + READERS readers + this coordinator thread.
    let start_barrier = Barrier::new(READERS + 2);
    let stop_barrier = Barrier::new(READERS + 2);

    // Keeps readers alive exactly as long as the writer is running.
    let is_running = AtomicBool::new(true);

    let start_b = &start_barrier;
    let stop_b = &stop_barrier;
    let running = &is_running;
    let read = &read;

    let mut elapsed = Duration::ZERO;

    thread::scope(|s| {
        s.spawn(move || {
            start_b.wait();

            for i in 0..iters {
                write(i);
            }

            running.store(false, Ordering::Release);
            stop_b.wait();
        });

        for _ in 0..READERS {
            s.spawn(move || {
                start_b.wait();

                while running.load(Ordering::Relaxed) {
                    read();
                }

                stop_b.wait();
            });
        }

        start_barrier.wait();
        let start = Instant::now();

        stop_barrier.wait();
        elapsed = start.elapsed();
    });

    elapsed
}

/// Times exactly `iters` reads on one reader thread while the writer updates
/// continuously.
fn run_read_under_write_load(
    iters: u64,
    mut read: impl FnMut() + Send,
    mut write: impl FnMut(u64) + Send,
) -> Duration {
    // 1 reader + 1 writer + this coordinator thread.
    let start_barrier = Barrier::new(3);
    let stop_barrier = Barrier::new(3);
    let is_running = AtomicBool::new(true);

    let start_b = &start_barrier;
    let stop_b = &stop_barrier;
    let running = &is_running;

    let mut elapsed = Duration::ZERO;

    thread::scope(|s| {
        s.spawn(move || {
            start_b.wait();

            for _ in 0..iters {
                read();
            }

            running.store(false, Ordering::Release);
            stop_b.wait();
        });

        s.spawn(move || {
            start_b.wait();

            let mut i = 0;
            while running.load(Ordering::Relaxed) {
                write(i);
                i += 1;
            }

            stop_b.wait();
        });

        start_barrier.wait();
        let start = Instant::now();

        stop_barrier.wait();
        elapsed = start.elapsed();
    });

    elapsed
}

fn bench_write_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("rcu_write_path");
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(100);

    group.bench_function("atomic_ptr", |b| {
        b.iter_custom(|iters| {
            let (mut w, r) = store_with(Config::default(), GRACE_PERIOD, QUEUE_CAPACITY);
            run_write_under_read_load(
                iters,
                move |i| w.update([0, 0, 0, i as usize]),
                || {
                    black_box(r.read());
                },
            )
        });
    });

    group.bench_function("arcswap", |b| {
        b.iter_custom(|iters| {
            let (mut w, r) = arcswap::store(Config::default());
            run_write_under_read_load(
                iters,
                move |i| w.update([0, 0, 0, i as usize]),
                || {
                    black_box(r.read());
                },
            )
        });
    });

    group.finish();
}

fn bench_read_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("rcu_read_path");
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(100);

    group.bench_function("atomic_ptr", |b| {
        b.iter_custom(|iters| {
            let (mut w, r) = store_with(Config::default(), GRACE_PERIOD, QUEUE_CAPACITY);
            run_read_under_write_load(
                iters,
                move || {
                    black_box(r.read());
                },
                move |i| w.update([0, 0, 0, i as usize]),
            )
        });
    });

    group.bench_function("arcswap", |b| {
        b.iter_custom(|iters| {
            let (mut w, r) = arcswap::store(Config::default());
            run_read_under_write_load(
                iters,
                move || {
                    black_box(r.read());
                },
                move |i| w.update([0, 0, 0, i as usize]),
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_write_path, bench_read_path);
criterion_main!(benches);

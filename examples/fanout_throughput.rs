use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use st3::fanout::{Pool, StdHost};

fn argument(args: &mut impl Iterator<Item = String>, default: usize) -> usize {
    args.next()
        .map(|value| value.parse().expect("arguments must be positive integers"))
        .unwrap_or(default)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let producers = argument(&mut args, 4);
    let consumers = argument(&mut args, 4);
    let items_per_producer = argument(&mut args, 250_000);
    assert!(producers > 0 && consumers > 0 && items_per_producer > 0);
    let total = producers
        .checked_mul(items_per_producer)
        .expect("item count overflow");

    let pool = Pool::new(consumers, 4096, Arc::new(StdHost::new(consumers)));
    let completed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(producers + consumers + 1));

    let workers: Vec<_> = (0..consumers)
        .map(|id| {
            let pool = Arc::clone(&pool);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                pool.run(pool.runner(id))
            })
        })
        .collect();
    let senders: Vec<_> = (0..producers)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let completed = Arc::clone(&completed);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                for _ in 0..items_per_producer {
                    let completed = Arc::clone(&completed);
                    pool.submit_fn(move || {
                        completed.fetch_add(1, Ordering::Release);
                    });
                }
            })
        })
        .collect();

    let start = Instant::now();
    gate.wait();
    for sender in senders {
        sender.join().expect("producer panicked");
    }
    let deadline = start + Duration::from_secs(300);
    while completed.load(Ordering::Acquire) < total && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let elapsed = start.elapsed();
    let delivered = completed.load(Ordering::Acquire);

    pool.shut_down();
    for worker in workers {
        assert!(worker.join().expect("consumer panicked"));
    }
    assert_eq!(delivered, total, "some tasks did not complete");
    println!(
        "pool producers={producers} consumers={consumers} items={total} seconds={:.6} tasks_per_second={:.0}",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64(),
    );
}

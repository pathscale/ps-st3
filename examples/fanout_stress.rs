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
    let items_per_producer = argument(&mut args, 25_000);
    assert!(producers > 0 && consumers > 0 && items_per_producer > 0);
    let total = producers
        .checked_mul(items_per_producer)
        .expect("item count overflow");

    let pool = Pool::new(consumers, 4096, Arc::new(StdHost::new(consumers)));
    let counts = Arc::new(
        std::iter::repeat_with(|| AtomicUsize::new(0))
            .take(total)
            .collect::<Vec<_>>(),
    );
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
        .map(|producer| {
            let pool = Arc::clone(&pool);
            let counts = Arc::clone(&counts);
            let completed = Arc::clone(&completed);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                for offset in 0..items_per_producer {
                    let item = producer * items_per_producer + offset;
                    let counts = Arc::clone(&counts);
                    let completed = Arc::clone(&completed);
                    pool.submit_fn(move || {
                        counts[item].fetch_add(1, Ordering::Relaxed);
                        completed.fetch_add(1, Ordering::Release);
                    });
                }
            })
        })
        .collect();

    gate.wait();
    for sender in senders {
        sender.join().expect("producer panicked");
    }

    let deadline = Instant::now() + Duration::from_secs(120);
    while completed.load(Ordering::Acquire) < total && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(completed.load(Ordering::Acquire), total, "tasks stalled");

    pool.shut_down();
    for worker in workers {
        assert!(worker.join().expect("consumer panicked"));
    }
    for (item, count) in counts.iter().enumerate() {
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "item {item} delivery count"
        );
    }
    println!(
        "verified {total} unique items from {producers} producers across {consumers} consumers"
    );
}

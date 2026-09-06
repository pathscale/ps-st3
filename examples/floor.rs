//! What parallelism is worth here: the sequential floor, the std option, and
//! this pool.
//!
//! # Reading it
//!
//! **The null arm is the floor and it is why the rest is believable.** It runs
//! the sequential arm a second time under another name, so whatever separation
//! it shows is what this harness reports for identical code. Nothing smaller
//! than that means anything. Identical code has measured 7.8% apart at p=0.00
//! in this house before.
//!
//! Bounded on purpose: fixed work, fixed reps, no growth on a slow machine.
//!
//! This example needs a newer toolchain than the library: `thread::scope` is
//! 1.63 and `black_box` is 1.66, against the crate's declared 1.60. An example
//! does not constrain a consumer, so the floor stays where it is.
#![allow(clippy::incompatible_msrv)]

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use orx_parallel::*;
use st3::fanout::{Pool, StdHost};
use st3::lifo::{Stealer as StStealer, Worker as StDeque};

const ITEMS: usize = 400_000;
const CHUNK: usize = 4_096;
const REPS: usize = 5;

/// Deliberately not vectorisable and not foldable, so the compiler cannot
/// delete the thing being measured.
fn work(n: u64) -> u64 {
    let mut a = n;
    for b in 0..256u64 {
        a = a.wrapping_mul(0x9e37_79b9).wrapping_add(b) ^ (a >> 7);
    }
    a
}

fn sequential(data: &[u64]) -> u64 {
    data.iter().map(|n| work(*n)).sum()
}

fn main() {
    let data: Vec<u64> = (0..ITEMS as u64).collect();
    let shared = Arc::new(data.clone());
    let cores = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);

    let host = Arc::new(StdHost::new(cores));
    let pool = Pool::new(cores, 4096, host.clone());
    let mut threads = Vec::new();
    for id in 0..cores {
        let worker = pool.runner(id);
        let pool = pool.clone();
        threads.push(std::thread::spawn(move || pool.run(worker)));
    }

    let mut seq = Vec::new();
    let mut par = Vec::new();
    let mut ray = Vec::new();
    let mut orx = Vec::new();
    let mut nul = Vec::new();
    let expected = sequential(&data);

    for _ in 0..REPS {
        let now = Instant::now();
        black_box(sequential(black_box(&data)));
        seq.push(now.elapsed());

        // One task per chunk, spread across slots. No join in this pool by
        // design, so completion is a count the submitter waits on.
        let done = Arc::new(AtomicUsize::new(0));
        let total = Arc::new(AtomicUsize::new(0));
        let chunks: Vec<&[u64]> = data.chunks(CHUNK).collect();
        let now = Instant::now();
        // A `Task` is `'static`, and this pool has no scoped submit, so a task
        // cannot borrow `data`. Share it and send an index range instead. The
        // earlier version of this example transmuted the borrow to `&'static`
        // and was sound only because of the wait loop below it, which is not a
        // thing to teach in the first example anybody reads.
        for (n, chunk) in chunks.iter().enumerate() {
            let shared = Arc::clone(&shared);
            let range = n * CHUNK..(n * CHUNK + chunk.len());
            let done = done.clone();
            let total = total.clone();
            pool.submit(
                n % cores,
                Box::new(move || {
                    let sum: u64 = shared[range].iter().map(|n| work(*n)).sum();
                    total.fetch_add(sum as usize, Ordering::Relaxed);
                    done.fetch_add(1, Ordering::Release);
                }),
            );
        }
        while done.load(Ordering::Acquire) < chunks.len() {
            std::hint::spin_loop();
        }
        par.push(now.elapsed());
        assert_eq!(
            total.load(Ordering::Relaxed),
            expected as usize,
            "same answer"
        );

        // The floor: fjb's shape, a scoped spawn per worker with one st3 deque
        // moved into each and a steal pass over the rest. No pool, no parking,
        // yield_now when a pass comes up empty.
        let now = Instant::now();
        let r: u64 = {
            let deques: Vec<StDeque<&[u64]>> = (0..cores)
                .map(|_| StDeque::new(chunks.len().max(1)))
                .collect();
            for (n, chunk) in chunks.iter().enumerate() {
                let _ = deques[n % cores].push(*chunk);
            }
            let stealers: Vec<StStealer<&[u64]>> = deques.iter().map(StDeque::stealer).collect();
            std::thread::scope(|scope| {
                let handles: Vec<_> = deques
                    .into_iter()
                    .enumerate()
                    .map(|(id, mine)| {
                        let stealers = &stealers;
                        scope.spawn(move || {
                            let mut sum = 0u64;
                            loop {
                                while let Some(chunk) = mine.pop() {
                                    sum += chunk.iter().map(|n| work(*n)).sum::<u64>();
                                }
                                let mut got = false;
                                for (which, stealer) in stealers.iter().enumerate() {
                                    if which == id {
                                        continue;
                                    }
                                    if stealer.steal(&mine, |n| n - n / 2).is_ok() {
                                        got = true;
                                        break;
                                    }
                                }
                                if !got {
                                    break;
                                }
                            }
                            sum
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).sum()
            })
        };
        ray.push(now.elapsed());
        assert_eq!(r, expected, "same answer");

        let now = Instant::now();
        let o: u64 = orx_parallel::ParIter::map(black_box(&data).par(), |n| work(*n)).sum();
        orx.push(now.elapsed());
        assert_eq!(o, expected, "same answer");

        let now = Instant::now();
        black_box(sequential(black_box(&data)));
        nul.push(now.elapsed());
    }

    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }

    let median = |mut v: Vec<Duration>| {
        v.sort();
        v[REPS / 2]
    };
    let (s, p, r, o, n) = (
        median(seq),
        median(par),
        median(ray),
        median(orx),
        median(nul),
    );
    println!("\n{ITEMS} items, {CHUNK} per task, {cores} cores, median of {REPS}\n");
    println!(
        "  sequential   {:>8.1} ms            <- the floor we have today",
        s.as_secs_f64() * 1e3
    );
    println!(
        "  fjb shape    {:>8.1} ms   {:>5.2}x  <- the floor: fjb's own scheduler",
        r.as_secs_f64() * 1e3,
        s.as_secs_f64() / r.as_secs_f64()
    );
    println!(
        "  fanout       {:>8.1} ms   {:>5.2}x",
        p.as_secs_f64() * 1e3,
        s.as_secs_f64() / p.as_secs_f64()
    );
    println!(
        "  orx-parallel {:>8.1} ms   {:>5.2}x  <- the std option",
        o.as_secs_f64() * 1e3,
        s.as_secs_f64() / o.as_secs_f64()
    );
    println!(
        "  null (seq)   {:>8.1} ms   {:>5.2}x  <- identical code, the harness floor",
        n.as_secs_f64() * 1e3,
        s.as_secs_f64() / n.as_secs_f64()
    );
    println!("\n  park timeouts during the run: {}", host.timeouts());
}

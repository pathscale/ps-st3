//! A `std`-backed `Host`, and the pool's one invariant: every task runs exactly
//! once.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use st3::fanout::{Pool, Runner, StdHost, Task};

fn drain(n: u64, workers: usize, seconds: u64) -> (u64, Vec<u64>) {
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::new(workers, 4096, host);
    let count = Arc::new(AtomicU64::new(0));

    let threads: Vec<_> = (0..workers)
        .map(|id| pool.runner(id))
        .map(|w: Runner| {
            let pool = pool.clone();
            std::thread::spawn(move || pool.run(w))
        })
        .collect();

    for i in 0..n {
        let c = count.clone();
        let task: Task = Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
        });
        pool.submit((i % workers as u64) as usize, task);
    }

    let deadline = Instant::now() + Duration::from_secs(seconds);
    while count.load(Ordering::Relaxed) < n && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let done = count.load(Ordering::Relaxed);
    let per_worker = (0..workers).map(|w| pool.completed(w)).collect();
    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }
    (done, per_worker)
}

#[test]
fn every_task_runs_exactly_once() {
    let (done, _) = drain(50_000, 4, 30);
    assert_eq!(done, 50_000, "every submitted task must run exactly once");
}

/// **The one the previous design failed.** Forty trivial tasks on four workers
/// left two of them with nothing and never finished, while fifty thousand of
/// the same task passed. A scheduler that only works at volume is not working,
/// so the small cases are tested first and by name.
#[test]
fn a_handful_of_tasks_finishes_too() {
    for n in [1u64, 4, 40, 400] {
        let (done, per_worker) = drain(n, 4, 10);
        assert_eq!(done, n, "{n} tasks stalled, workers ran {per_worker:?}");
    }
}

/// Submitting every task to one worker still finishes: the others steal.
#[test]
fn work_reaches_workers_it_was_not_given_to() {
    let workers = 4;
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::new(workers, 4096, host);
    let count = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..workers)
        .map(|id| pool.runner(id))
        .map(|w| {
            let pool = pool.clone();
            std::thread::spawn(move || pool.run(w))
        })
        .collect();

    let n = 20_000u64;
    for _ in 0..n {
        let c = count.clone();
        pool.submit(
            0,
            Box::new(move || {
                for _ in 0..64 {
                    core::hint::spin_loop();
                }
                c.fetch_add(1, Ordering::Relaxed);
            }),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while count.load(Ordering::Relaxed) < n && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(count.load(Ordering::Relaxed), n);
    let stolen: u64 = (1..workers).map(|w| pool.completed(w)).sum();
    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }
    assert!(
        stolen > 0,
        "no worker stole anything: stealing is not working"
    );
}

/// **An idle pool must cost nothing.** This is a regression test with a number
/// in it, because the thing it guards was invisible: nothing woke a parked
/// worker when work appeared somewhere it could steal from, so the only way one
/// ever found out was for its park to expire. That made the park timeout the
/// notification mechanism, and a pool with zero tasks woke every worker roughly
/// 800 times a second and burnt 10% of a core doing it.
///
/// Measured on sixteen workers over two seconds, before and after: 198.8 ms of
/// CPU against 3.7 ms, and 25,705 park timeouts against 304. The bound below is
/// deliberately loose, a tenth of the old cost, so it fails on a regression to
/// polling rather than on a busy machine.
#[test]
fn an_idle_pool_stays_idle() {
    let workers = 8;
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::new(workers, 4096, host.clone());
    let threads: Vec<_> = (0..workers)
        .map(|id| pool.runner(id))
        .map(|w| {
            let pool = pool.clone();
            std::thread::spawn(move || pool.run(w))
        })
        .collect();

    // Let every worker find nothing and park.
    std::thread::sleep(Duration::from_millis(50));
    let before = host.timeouts();
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(1));
    let woke = host.timeouts() - before;
    let seconds = started.elapsed().as_secs_f64();

    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }

    let per_worker_per_second = woke as f64 / seconds / workers as f64;
    assert!(
        per_worker_per_second < 80.0,
        "an idle worker woke {per_worker_per_second:.0} times a second. \
         It should wake about ten, one per 100 ms backstop park. \
         Something is polling instead of sleeping."
    );
}

/// Two threads on one worker is a caller error, and the second is refused
/// rather than raced. It has to be visible that it was refused.
#[test]
fn one_worker_cannot_be_run_twice() {
    let host = Arc::new(StdHost::new(2));
    let pool = Pool::new(2, 64, host);
    let runner = pool.runner(0);
    let running = {
        let pool = pool.clone();
        std::thread::spawn(move || pool.run(runner))
    };
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        !pool.run(pool.runner(0)),
        "the second thread on one worker must be refused"
    );
    pool.shut_down();
    assert!(running.join().expect("the first thread ran"));
}

/// **Fails before the identity check.** A `Runner` carries which worker it is
/// and nothing else, so handing one to a different pool used to be accepted
/// silently: `run` indexed *this* pool's workers by that id and took the wrong
/// deque, with no panic and no way for the caller to notice. Two pools sharing
/// a thread pool's worth of runners would quietly run each other's work.
#[test]
#[should_panic(expected = "belongs to another pool")]
fn a_runner_from_another_pool_is_refused() {
    let first = Pool::new(2, 64, Arc::new(StdHost::new(2)));
    let second = Pool::new(2, 64, Arc::new(StdHost::new(2)));
    let _ = second.run(first.runner(0));
}

/// **Fails before the bounds check.** `runner` took any integer, so an id past
/// the end of the pool was accepted and became an index-out-of-bounds panic
/// inside `run`, on another thread, pointing at the pool's internals rather
/// than at the caller's mistake.
#[test]
#[should_panic(expected = "worker 99 of a pool with 4")]
fn a_runner_for_a_worker_that_does_not_exist_is_refused() {
    let pool = Pool::new(4, 64, Arc::new(StdHost::new(4)));
    let _ = pool.runner(99);
}

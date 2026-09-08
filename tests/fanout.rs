//! A `std`-backed `Host`, and the pool's one invariant: every task runs exactly
//! once.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
/// CPU against 3.7 ms.
///
/// **It measures CPU, because CPU is the claim.** It used to count park
/// timeouts, and then spurious wakes, and neither is the same thing: a worker
/// that spins forever without ever reaching the platform wait burns a core
/// while both counters stay at zero. `getrusage` cannot be fooled that way. The
/// spurious count is asserted too, but as a second, weaker signal.
///
/// The bound is deliberately loose, a quarter of the old cost, so it fails on a
/// regression to polling rather than on a busy machine.
fn cpu_seconds() -> f64 {
    #[repr(C)]
    #[derive(Default)]
    struct Timeval {
        sec: i64,
        usec: i32,
        _pad: i32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Rusage {
        utime: Timeval,
        stime: Timeval,
        rest: [i64; 14],
    }
    extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }
    let mut usage = Rusage::default();
    // SAFETY: `who` is RUSAGE_SELF and the struct is the layout the platform
    // writes; the trailing fields are only ever read as opaque words.
    unsafe {
        getrusage(0, &mut usage);
    }
    usage.utime.sec as f64
        + f64::from(usage.utime.usec) / 1e6
        + usage.stime.sec as f64
        + f64::from(usage.stime.usec) / 1e6
}

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

    // Let every worker find nothing, spin out its backoff, and park.
    std::thread::sleep(Duration::from_millis(200));
    let before = host.spurious();
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(1));
    let cpu = cpu_seconds() - cpu_before;
    let woke = host.spurious() - before;
    let seconds = started.elapsed().as_secs_f64();

    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }

    // The real assertion: an idle pool consumes no processor time. A tenth of
    // the 198.8 ms the polling design cost, per second, across every worker.
    assert!(
        cpu / seconds < 0.05,
        "an idle pool of {workers} workers burnt {:.1} ms of CPU per second; \
         it is spinning rather than sleeping",
        cpu / seconds * 1e3
    );

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

/// A job that is never run still releases what it captured.
///
/// `Job` owns its work through a raw pointer, so nothing releases that work
/// unless `Job` does. The cases that matter are a pool dropped with jobs still
/// queued and a worker unwinding while holding a private queue: in a
/// long-running service those captures would pin buffers and reference counts
/// for the life of the process.
#[test]
fn an_unrun_job_still_drops_its_captures() {
    struct Tell(Arc<AtomicUsize>);
    impl Drop for Tell {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let ran = Arc::new(AtomicUsize::new(0));

    {
        let (tell, did_run) = (Tell(dropped.clone()), ran.clone());
        let job = st3::fanout::Job::from_boxed(move || {
            let _ = &tell;
            did_run.fetch_add(1, Ordering::Relaxed);
        });
        drop(job);
    }

    assert_eq!(dropped.load(Ordering::Relaxed), 1, "the capture leaked");
    assert_eq!(ran.load(Ordering::Relaxed), 0, "a dropped job ran");
}

/// The same, through a pool: work submitted and never run is released when the
/// pool is.
#[test]
fn a_dropped_pool_releases_queued_work() {
    struct Tell(Arc<AtomicUsize>);
    impl Drop for Tell {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    {
        // No worker ever runs, so every job is still queued at the end.
        let pool = Pool::new(2, 64, Arc::new(StdHost::new(2)));
        for _ in 0..16 {
            let tell = Tell(dropped.clone());
            pool.submit_fn(move || {
                let _ = &tell;
            });
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }
    assert_eq!(
        dropped.load(Ordering::Relaxed),
        16,
        "queued work leaked when the pool was dropped"
    );
}

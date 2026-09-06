//! A `std`-backed [`Host`](crate::Host), behind the `host` feature.
//!
//! The core of this crate knows nothing about an operating system: three
//! methods, `park`, `unpark` and `now_ns`. This is what those look like when
//! there is a `std` to implement them with, and it is the reference a target
//! without one replaces.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};
use std::vec::Vec;

use super::Host;

/// Condvar parking, one pair per worker.
#[derive(Debug)]
pub struct StdHost {
    slots: Vec<(Mutex<bool>, Condvar)>,
    origin: Instant,
    /// How many times a park timed out rather than being woken. With the pool
    /// waking its own sleepers this should stay near zero under load; what it
    /// still counts on an idle pool is one tick per worker per 100 ms.
    timeouts: AtomicU64,
}

impl StdHost {
    #[must_use]
    /// One park slot per worker, and a clock whose origin is now.
    pub fn new(workers: usize) -> Self {
        let mut slots = Vec::new();
        slots.resize_with(workers, || (Mutex::new(false), Condvar::new()));
        Self {
            slots,
            origin: Instant::now(),
            timeouts: AtomicU64::new(0),
        }
    }

    /// Parks that timed out instead of being woken.
    #[must_use]
    pub fn timeouts(&self) -> u64 {
        self.timeouts.load(Ordering::Relaxed)
    }
}

impl Host for StdHost {
    /// **Bounded, but only as a backstop.** It used to be bounded at one
    /// millisecond, and that was not a backstop: nothing woke a parked worker
    /// when stealable work appeared, so the timeout *was* the notification.
    /// A pool with nothing to do woke every worker 800 times a second and burnt
    /// 10% of a core doing it.
    ///
    /// The pool now names its sleepers and wakes one when work is published, so
    /// this bound covers only a wakeup genuinely lost below us. A hundred
    /// milliseconds is short enough that such a bug costs latency rather than a
    /// hang, and long enough that idling is free.
    fn park(&self, worker: usize) {
        let (mutex, condvar) = &self.slots[worker];
        let mut guard = mutex.lock().expect("the park mutex is not poisoned");
        // A permit was left by an unpark that arrived before this park. Take it
        // and do not sleep. Locking again here instead of writing through the
        // guard deadlocks: the mutex is not reentrant, and it cost an hour.
        if *guard {
            *guard = false;
            return;
        }
        let (mut guard, timed_out) = condvar
            .wait_timeout(guard, Duration::from_millis(100))
            .expect("the park mutex is not poisoned");
        if timed_out.timed_out() {
            self.timeouts.fetch_add(1, Ordering::Relaxed);
        }
        *guard = false;
    }

    fn unpark(&self, worker: usize) {
        let (mutex, condvar) = &self.slots[worker];
        *mutex.lock().expect("the park mutex is not poisoned") = true;
        condvar.notify_one();
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

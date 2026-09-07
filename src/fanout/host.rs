//! A `std`-backed [`Host`], behind the `host` feature.
//!
//! [`Host`]: super::Host
//!
//! The core of this crate knows nothing about an operating system: three
//! methods, `park`, `unpark` and `now_ns`. This is what those look like when
//! there is a `std` to implement them with, and it is the reference a target
//! without one replaces.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};
use std::vec::Vec;

use super::Host;

/// Condvar parking, one pair per worker.
#[derive(Debug)]
pub struct StdHost {
    slots: Vec<Slot>,
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
        slots.resize_with(workers, Slot::default);
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

/// One worker's park state.
///
/// The permit and the waiter count are atomics so the common cases cost no
/// lock at all: an unpark with nobody waiting is one store and one load, and a
/// park with a permit already left is one swap. The mutex and condvar are only
/// entered when a thread genuinely has to sleep, which is what a mutex per
/// wake was costing before.
#[derive(Debug, Default)]
struct Slot {
    permit: AtomicBool,
    waiting: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
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
        let slot = &self.slots[worker];
        // A permit left by an unpark that arrived first. No lock, no sleep.
        if slot.permit.swap(false, Ordering::AcqRel) {
            return;
        }

        let guard = slot.mutex.lock().expect("the park mutex is not poisoned");
        // Announce before the last look, so an unpark either sees the count
        // and notifies, or leaves a permit this finds below.
        slot.waiting.fetch_add(1, Ordering::SeqCst);
        if slot.permit.swap(false, Ordering::AcqRel) {
            slot.waiting.fetch_sub(1, Ordering::SeqCst);
            return;
        }
        let (_guard, timed_out) = slot
            .condvar
            .wait_timeout(guard, Duration::from_millis(100))
            .expect("the park mutex is not poisoned");
        slot.waiting.fetch_sub(1, Ordering::SeqCst);
        if timed_out.timed_out() {
            self.timeouts.fetch_add(1, Ordering::Relaxed);
        }
        slot.permit.store(false, Ordering::Release);
    }

    fn unpark(&self, worker: usize) {
        let slot = &self.slots[worker];
        slot.permit.store(true, Ordering::Release);
        // Nobody is on the condvar, so there is nothing to notify and no
        // reason to take the lock. This is the case that used to cost a mutex
        // acquisition on every submit that found a sleeper bit set.
        if slot.waiting.load(Ordering::SeqCst) == 0 {
            return;
        }
        let _guard = slot.mutex.lock().expect("the park mutex is not poisoned");
        slot.condvar.notify_one();
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

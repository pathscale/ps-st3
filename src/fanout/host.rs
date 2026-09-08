//! A `std`-backed [`Host`], behind the `host` feature.
//!
//! [`Host`]: super::Host
//!
//! The core of this crate knows nothing about an operating system: three
//! methods, `park`, `unpark` and `now_ns`. This is what those look like when
//! there is a `std` to implement them with, and it is the reference a target
//! without one replaces.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;
use std::vec::Vec;

use super::Host;

/// Futex parking, one word per worker.
///
/// **No mutex and no condition variable.** The pool wakes a worker on the hot
/// path, and a condvar wake is a mutex acquisition, a signal and a release even
/// when nobody is on the other end. This is one atomic word per worker, and a
/// wake with nobody asleep is a store and a load with no system call at all.
#[derive(Debug)]
pub struct StdHost {
    slots: Vec<CachePadded<Slot>>,
    origin: Instant,
    /// How many times a park woke with no permit to show for it.
    spurious: AtomicU64,
}

/// One worker's park word.
///
/// `PARKED` means a thread is, or is about to be, blocked on this address.
/// `PERMIT` means a wake arrived and the next park must consume it rather than
/// sleep, which is the invariant [`Host`] requires.
#[derive(Debug, Default)]
struct Slot {
    word: AtomicU32,
}

const RUNNING: u32 = 0;
const PARKED: u32 = 1;
const PERMIT: u32 = 2;

impl StdHost {
    #[must_use]
    /// One park slot per worker, and a clock whose origin is now.
    pub fn new(workers: usize) -> Self {
        let mut slots = Vec::new();
        slots.resize_with(workers, || CachePadded::new(Slot::default()));
        Self {
            slots,
            origin: Instant::now(),
            spurious: AtomicU64::new(0),
        }
    }

    /// How many parks woke with no permit to show for it.
    ///
    /// The platform is allowed to return from a wait for its own reasons; the
    /// pool loops, so one costs a pass and nothing else. A number climbing
    /// under load means something is waking workers that has no work for them.
    #[must_use]
    pub fn spurious(&self) -> u64 {
        self.spurious.load(Ordering::Relaxed)
    }

    /// How many parks ended in a timeout rather than a wake.
    ///
    /// Always zero: parking has no timeout any more. It had one as a backstop
    /// against a wake going missing, from a time when the pool did not wake its
    /// own sleepers and the timeout *was* the discovery mechanism. The pool
    /// wakes them now and `shut_down` unparks every worker, so the backstop
    /// guarded nothing and cost a wakeup per worker per 100 ms on an idle pool.
    #[must_use]
    #[deprecated(since = "0.6.0", note = "parking no longer times out; see `spurious`")]
    pub fn timeouts(&self) -> u64 {
        0
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
        let word = &self.slots[worker].word;
        // Claim a permit left by a wake that arrived first, and do not sleep.
        if word.swap(RUNNING, Ordering::AcqRel) == PERMIT {
            return;
        }
        // Announce, then look once more. A wake landing between the two sees
        // `PARKED` and calls `wake_one`, which is not lost: this thread is
        // either already waiting, or about to wait on a word that no longer
        // reads `PARKED` and so returns at once.
        if word
            .compare_exchange(RUNNING, PARKED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            word.store(RUNNING, Ordering::Release);
            return;
        }

        // **No timeout.** There used to be one, at 100 ms, as a backstop
        // against a wake going missing. It is not needed: `Pool` sets a
        // worker's sleeper bit before its last look for work, so a submit
        // either publishes before that look or reads a bitmap with the bit
        // already set and unparks; and `shut_down` unparks every worker. What
        // the timeout did cost was a wakeup per worker per 100 ms forever on a
        // pool with nothing to do.
        while word.load(Ordering::Acquire) == PARKED {
            atomic_wait::wait(word, PARKED);
            if word.load(Ordering::Acquire) == PARKED {
                self.spurious.fetch_add(1, Ordering::Relaxed);
            }
        }
        word.store(RUNNING, Ordering::Release);
    }

    fn unpark(&self, worker: usize) {
        let word = &self.slots[worker].word;
        // Leave a permit whatever the state was, because `Host` requires an
        // unpark that arrives before the park to make that park return at once.
        if word.swap(PERMIT, Ordering::AcqRel) == PARKED {
            // Somebody is on the address, so this costs a system call. Nothing
            // was asleep in the other cases and this is a single swap.
            atomic_wait::wake_one(word);
        }
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

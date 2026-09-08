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
/// # Where this came from
///
/// The three-state protocol below is `forte`'s `Semaphore` (`src/latch.rs`,
/// MIT OR Apache-2.0, the same licence as this crate), ported because it is
/// better than what was here and there is no point pretending otherwise. What
/// makes it better is one observation:
///
/// **Both sides swap the same word, and modification order on a single atomic
/// is total even under `Relaxed`.** So there is no store-load race to close and
/// no `SeqCst` to pay for it. The protocol this replaces used a pool-wide
/// bitmap of who was asleep, which is a different word from the queue, so it
/// *did* need `SeqCst`, and every park attempt and every submit hit that one
/// cache line with a read-modify-write. Measured: parking sooner made the pool
/// spend *more* CPU, up to 818 ms against 445, because the bookkeeping cost
/// more than the sleep saved.
#[derive(Debug)]
pub struct StdHost {
    slots: Vec<CachePadded<Slot>>,
    origin: Instant,
    /// How many parks woke with no signal to show for it.
    spurious: AtomicU64,
}

/// One worker's park word.
///
/// * `LOCKED` is running, and no signal outstanding.
/// * `ASLEEP` is blocked, or about to block, on this address.
/// * `SIGNAL` is a permit left by an unpark, which the next park consumes
///   instead of sleeping. That is the invariant [`Host`] requires.
#[derive(Debug, Default)]
struct Slot {
    state: AtomicU32,
}

const LOCKED: u32 = 0;
const SIGNAL: u32 = 1;
const ASLEEP: u32 = 2;

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

    /// How many parks woke with no signal to show for it.
    ///
    /// The platform may return from a wait for its own reasons; the pool loops,
    /// so one costs a pass and nothing else.
    #[must_use]
    pub fn spurious(&self) -> u64 {
        self.spurious.load(Ordering::Relaxed)
    }

    /// How many parks ended in a timeout rather than a wake.
    ///
    /// Always zero: parking has no timeout any more. It had one as a backstop
    /// from a time when the pool did not wake its own sleepers and the timeout
    /// *was* how stealable work got discovered.
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
        let state = &self.slots[worker].state;
        // Announce sleep and read what was there in one operation. `Relaxed` is
        // enough: this and `unpark` swap the same word, and a single atomic's
        // modification order is total whatever the ordering asks for, so one of
        // the two always sees the other.
        if state.swap(ASLEEP, Ordering::Relaxed) != LOCKED {
            // A signal arrived first. Take it and do not sleep.
            state.store(LOCKED, Ordering::Relaxed);
            return;
        }
        // **No timeout.** `Pool` publishes work and then unparks, and
        // `shut_down` unparks every worker, so a wake is never lost; the
        // timeout this used to have cost a wakeup per worker per 100 ms on a
        // pool with nothing to do and guarded nothing.
        while state.load(Ordering::Acquire) == ASLEEP {
            atomic_wait::wait(state, ASLEEP);
            if state.load(Ordering::Acquire) == ASLEEP {
                self.spurious.fetch_add(1, Ordering::Relaxed);
            }
        }
        state.store(LOCKED, Ordering::Relaxed);
    }

    fn unpark(&self, worker: usize) {
        let state = &self.slots[worker].state;
        // Leave a permit whatever the state was, because `Host` requires an
        // unpark arriving before the park to make that park return at once.
        //
        // **The system call happens only when somebody is actually on the
        // address.** Signalling a running worker is this one swap: no syscall,
        // no lock, and no shared line except that worker's own, which is
        // padded. That is what lets `Pool` unpark on every submit without
        // asking first who is asleep.
        if state.swap(SIGNAL, Ordering::Release) == ASLEEP {
            atomic_wait::wake_one(state);
        }
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

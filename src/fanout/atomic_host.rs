//! Blocking atomic waits with core/alloc and the platform ABI, without Rust std.
//!
//! Available with `atomic-host` on the platforms supported by `atomic-wait`.
//! The caller provides workers and a monotonic clock; no threads are created.

use crate::config::AtomicUnsignedLong;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, Ordering};
use crossbeam_utils::CachePadded;

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
pub struct AtomicHost {
    slots: Vec<CachePadded<Slot>>,
    clock: fn() -> u64,
    /// How many parks woke with no signal to show for it.
    spurious: AtomicUnsignedLong,
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

impl AtomicHost {
    #[must_use]
    /// One park slot per worker and a host-supplied monotonic nanosecond clock.
    ///
    /// The clock callback must be thread-safe and monotonic. It is not called
    /// while parking and is independent of any runtime timer installation.
    pub fn new(workers: usize, clock: fn() -> u64) -> Self {
        let mut slots = Vec::new();
        slots.resize_with(workers, || CachePadded::new(Slot::default()));
        Self {
            slots,
            clock,
            spurious: AtomicUnsignedLong::new(0),
        }
    }

    /// How many parks woke with no signal to show for it.
    ///
    /// The platform may return from a wait for its own reasons; the pool loops,
    /// so one costs a pass and nothing else.
    #[must_use]
    #[allow(clippy::useless_conversion)]
    pub fn spurious(&self) -> u64 {
        u64::from(self.spurious.load(Ordering::Relaxed))
    }
}

impl Host for AtomicHost {
    /// Block until signalled, preserving a permit delivered before the wait.
    fn park(&self, worker: usize) {
        let state = &self.slots[worker].state;
        // **`Acquire`, not `Relaxed`.** Total modification order on this word
        // proves which transition happened first and *nothing* about what the
        // unparker wrote before it. Consuming a permit has to acquire the
        // publication that preceded it, or a worker can take the shutdown
        // notification and then read `running` as `true`, park again, and never
        // be told a second time.
        if state.swap(ASLEEP, Ordering::Acquire) != LOCKED {
            // A signal arrived first. Take it and do not sleep.
            //
            // Compare-exchange rather than a store: another unpark may have
            // landed since the swap above, and its permit has to survive rather
            // than be overwritten with `LOCKED`. A failure here means exactly
            // that, and leaving `SIGNAL` in place is the right outcome.
            let _ = state.compare_exchange(ASLEEP, LOCKED, Ordering::Acquire, Ordering::Relaxed);
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
        // Consume the permit that ended the wait, and only that one. A plain
        // store would clear a *newer* permit that arrived between the loop
        // exiting and this line, which is a lost wakeup.
        let _ = state.compare_exchange(SIGNAL, LOCKED, Ordering::Acquire, Ordering::Relaxed);
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
        (self.clock)()
    }
}

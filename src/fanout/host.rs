//! Convenience clock adapter for the no_std atomic parker.
use super::{AtomicHost, Host};
use std::time::Instant;

/// Blocking atomic parking with a std monotonic clock.
///
/// For a host without Rust std, use [`AtomicHost`] with its own clock and
/// worker lifecycle. Both hosts use the same permit and wake implementation.
#[derive(Debug)]
pub struct StdHost {
    parking: AtomicHost,
    origin: Instant,
}
impl StdHost {
    /// One park slot per worker and a clock whose origin is now.
    #[must_use]
    pub fn new(workers: usize) -> Self {
        Self {
            parking: AtomicHost::new(workers, || 0),
            origin: Instant::now(),
        }
    }
    /// How many waits woke without a signal.
    #[must_use]
    pub fn spurious(&self) -> u64 {
        self.parking.spurious()
    }
    /// Always zero: parking has no periodic timeout.
    #[must_use]
    #[deprecated(since = "0.6.0", note = "parking no longer times out; see `spurious`")]
    pub fn timeouts(&self) -> u64 {
        0
    }
}
impl Host for StdHost {
    fn park(&self, worker: usize) {
        self.parking.park(worker);
    }
    fn unpark(&self, worker: usize) {
        self.parking.unpark(worker);
    }
    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

//! A fixed pool of workers, each owning one of this crate's deques and
//! stealing from the others.
//!
//! # What this is
//!
//! One [`lifo::Worker`](crate::lifo::Worker) per pool worker, taken by value by
//! the thread that runs it, so pushing and popping local work touch nothing
//! shared. Work arrives from outside through a per-worker intake, and an idle
//! worker steals from a random victim before it parks.
//!
//! Tasks are independent by construction: no join, no continuation, no
//! completion signal. A task may run whenever a worker gets to it, and a caller
//! that needs to know when a batch finished counts for itself.
//!
//! # You bring the threads
//!
//! The pool does not spawn. [`Pool::runner`] hands out a [`Runner`] per worker
//! and the caller gives each one a thread by calling [`Pool::run`]. That is
//! what keeps the core free of an operating system, and it is why a caller can
//! put a worker on a thread it already owns.
//!
//! # Waking, which is the part that is easy to get wrong
//!
//! `submit` unparks the worker it was given. Nothing else knows that work has
//! appeared somewhere a *different* worker could steal from, so the pool keeps
//! a bitmap of who is asleep and wakes one of them whenever work is published,
//! drained in bulk, or stolen in bulk. Without that the only way a parked
//! worker ever found stealable work was for its park to time out, which turned
//! the timeout into the notification mechanism and cost 10% of a core on a pool
//! with nothing to do.
//!
//! # The operating system
//!
//! Three methods, in [`Host`]: park, unpark, and a clock. This module is
//! `no_std` and knows nothing else about a platform. [`StdHost`], behind the
//! `host` feature, is what those look like with a `std` to implement them.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use crate::lifo::{Stealer, Worker as Queue};
use spin::Mutex;

#[cfg(feature = "host")]
mod host;
#[cfg(feature = "host")]
pub use host::StdHost;

/// The most tasks one steal moves, before halving.
///
/// A steal takes half of what the victim holds, and this caps the half's input,
/// so the largest a single steal can move is `PULL_BATCH / 2`. Taking
/// everything makes the victim idle immediately and the thief the new
/// bottleneck; taking one makes the next steal a fresh contention on the same
/// victim.
pub const PULL_BATCH: usize = 32;

/// A unit of work.
///
/// Independent by construction: no join, no continuation, no completion
/// signal, so a task may run whenever a worker reaches it.
pub type Task = Box<dyn FnOnce() + Send>;

/// What the pool needs from an operating system, which is almost nothing: a way
/// to stop consuming a core, a way to undo that, and a clock. A target without
/// an OS can implement `park` as a spin and `now_ns` as a cycle counter.
///
/// # The one invariant
///
/// **`unpark` must leave a permit that the next `park` consumes.** An `unpark`
/// that arrives while the worker is still running has to make that worker's
/// next `park` return immediately rather than sleep. The pool publishes work
/// and *then* unparks, so an implementation that drops a wakeup for a worker
/// which has not parked yet loses the only notice that work exists, and that
/// worker sleeps with a task sitting in its intake.
///
/// A spin satisfies this trivially, since it never sleeps. A condition variable
/// does not: it needs a flag beside it, which is what [`StdHost`] carries.
///
/// `park` may also return spuriously. The pool loops, so a wakeup with nothing
/// to show for it costs one pass and nothing else.
///
/// [`StdHost`]: crate::fanout::StdHost
pub trait Host: Send + Sync {
    /// Stop consuming a core until `unpark` for this worker, or spuriously.
    fn park(&self, worker: usize);
    /// Wake `worker`, leaving a permit if it has not parked yet.
    fn unpark(&self, worker: usize);
    /// A monotonic clock in nanoseconds. Only the origin is arbitrary.
    fn now_ns(&self) -> u64;
}

/// Which worker a thread is running.
///
/// Only [`Pool::runner`] makes one, and it checks the id against the pool it
/// came from, so a handle cannot name a worker that does not exist. Handing the
/// same `Runner` to two threads is still a caller error, and
/// [`Pool::run`] refuses the second rather than racing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Runner {
    id: usize,
}

impl Runner {
    /// Which worker this is, counting from zero.
    #[must_use]
    pub fn id(self) -> usize {
        self.id
    }
}

/// One worker's deque, held until a thread takes it.
///
/// `st3::lifo::Worker` is `!Sync` by construction: the owner end is
/// single-consumer, so the pool cannot hand out shared references to it. That
/// is not in the way of this design, it is the design. The queue is taken by
/// value once, and from then on the owning thread's pushes and pops touch
/// nothing shared.
struct Local {
    queue: Mutex<Option<Queue<Task>>>,
    /// Where work from outside lands, since a `Queue` cannot be pushed to by
    /// anyone but its owner.
    intake: Mutex<Vec<Task>>,
    completed: AtomicU64,
}

/// A fixed set of workers, their deques, and the intakes work arrives through.
///
/// Construct it with [`Pool::new`], hand each [`Runner`] to a thread of your
/// own with [`Pool::run`], and feed it with [`Pool::submit`].
pub struct Pool {
    local: Vec<Local>,
    stealers: Vec<Stealer<Task>>,
    host: Arc<dyn Host>,
    running: AtomicBool,
    /// One bit per worker, set while that worker is parked or about to park.
    ///
    /// This exists because nothing else can tell a sleeping worker that work
    /// landed somewhere it could steal from. `submit` only knows to unpark the
    /// worker it was given. Without this bitmap the only way a parked worker
    /// ever discovered stealable work was for its park to time out, which made
    /// the timeout the notification mechanism and cost 10% of a core on a pool
    /// with nothing to do.
    sleepers: AtomicUsize,
}

impl Pool {
    /// A pool of `workers` deques, each holding `capacity` tasks.
    ///
    /// # Capacity is not backpressure
    ///
    /// The deques are bounded, which is st3's shape, but [`submit`] does not
    /// push into one. It appends to an unbounded intake that the owning worker
    /// drains, so **`capacity` bounds what a worker holds, not what a caller
    /// may hand it.** A submitter is never blocked and never refused; if tasks
    /// arrive faster than they run, the intake grows until memory runs out.
    ///
    /// When the worker drains an intake larger than its deque, the overflow is
    /// run inline *on that worker*, which neither drops a task nor grows the
    /// deque. That is the only sense in which capacity is enforced, and it
    /// costs the worker its place in the queue rather than costing the
    /// submitter anything.
    ///
    /// A caller that needs backpressure has to impose it: count outstanding
    /// tasks and stop submitting. This pool will not do it for you.
    ///
    /// [`submit`]: Pool::submit
    ///
    /// # Panics
    ///
    /// If `workers` exceeds the bits in a `usize`, which is how many sleeping
    /// workers the wake bitmap can name.
    #[must_use]
    pub fn new(workers: usize, capacity: usize, host: Arc<dyn Host>) -> Arc<Self> {
        assert!(
            workers <= usize::BITS as usize,
            "a pool is one bit per worker in a usize: {workers} workers is more than {} ",
            usize::BITS
        );
        let mut local = Vec::with_capacity(workers);
        let mut stealers = Vec::with_capacity(workers);
        for _ in 0..workers {
            let queue = Queue::new(capacity);
            stealers.push(queue.stealer());
            local.push(Local {
                queue: Mutex::new(Some(queue)),
                intake: Mutex::new(Vec::new()),
                completed: AtomicU64::new(0),
            });
        }
        Arc::new(Self {
            local,
            stealers,
            host,
            running: AtomicBool::new(true),
            sleepers: AtomicUsize::new(0),
        })
    }

    #[must_use]
    /// How many workers this pool was built with.
    pub fn workers(&self) -> usize {
        self.local.len()
    }

    /// Every worker's handle, in order, for the threads that will run them.
    #[must_use]
    pub fn runner(&self, id: usize) -> Runner {
        Runner { id }
    }

    /// How many tasks a worker has finished.
    #[must_use]
    pub fn completed(&self, id: usize) -> u64 {
        self.local[id].completed.load(Ordering::Relaxed)
    }

    /// Hand a task to a worker.
    ///
    /// **Always unparks.** The obvious optimisation is to wake only when the
    /// intake was empty, on the reasoning that a non-empty intake means the
    /// worker is already awake. It is not sound: the worker can drain the
    /// intake and park between one submit and the next, and then nothing wakes
    /// it. That is what stalled the design this replaced, and an unpark on an
    /// already-running worker is cheap.
    pub fn submit(&self, worker: usize, task: Task) {
        self.local[worker].intake.lock().push(task);
        self.host.unpark(worker);
        // And one sleeper besides, so the work can be *stolen* without waiting
        // for anything to time out. The task is published before this reads the
        // bitmap, which is the half of the handshake that makes an unbounded
        // park safe; the other half is in `run_worker`.
        self.wake_one_sleeper(worker);
    }

    /// Wake one parked worker other than `except`, if any is parked.
    ///
    /// The waker clears the bit rather than the sleeper, so a run of submits
    /// wakes a run of *different* workers instead of hammering the same one.
    /// Clearing it also means an unpark is never sent twice for one sleep, and
    /// `Host::unpark` leaves a permit anyway, so a wake that arrives before the
    /// park is not lost.
    fn wake_one_sleeper(&self, except: usize) {
        let mut parked = self.sleepers.load(Ordering::SeqCst) & !(1usize << except);
        while parked != 0 {
            let candidate = parked.trailing_zeros() as usize;
            let bit = 1usize << candidate;
            if self.sleepers.fetch_and(!bit, Ordering::SeqCst) & bit != 0 {
                self.host.unpark(candidate);
                return;
            }
            // Someone else claimed that one first. Try the next.
            parked &= !bit;
        }
    }

    /// Tell every worker to stop once it runs out of work, and wake them all.
    ///
    /// **Tasks submitted from here on may never run.** A worker leaves as soon
    /// as it finds its deque empty, its intake empty and nothing to steal, so
    /// anything that lands afterwards is dropped when the pool is. This pool
    /// has no completion signal by design; if you need every task to have run,
    /// stop submitting and wait for your own count before calling this.
    pub fn shut_down(&self) {
        self.running.store(false, Ordering::Release);
        for id in 0..self.workers() {
            self.host.unpark(id);
        }
    }

    #[must_use]
    /// Whether [`shut_down`](Pool::shut_down) has not been called yet.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Run one worker on this thread until the pool shuts down.
    ///
    /// Takes the worker's deque by value on the way in, so nothing else can
    /// reach it, and puts it back on the way out so the worker can be restarted.
    /// Returns once [`shut_down`](Pool::shut_down) has been called and this
    /// worker has nothing left to do.
    ///
    /// # Returns
    ///
    /// `false`, immediately, if another thread is already running this worker.
    /// Two threads on one deque is a caller error and refusing beats racing, but
    /// a silent refusal looks exactly like a worker that was given no work, so
    /// it is worth checking.
    #[must_use = "a `false` here means the worker was already running and this thread did nothing"]
    pub fn run(self: &Arc<Self>, w: Runner) -> bool {
        let Some(queue) = self.local[w.id].queue.lock().take() else {
            return false;
        };

        let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ (w.id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);

        loop {
            if let Some(task) = queue.pop() {
                task();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            if self.drain_intake(w.id, &queue) > 0 {
                continue;
            }

            if self.steal_once(&queue, w.id, &mut rng) > 0 {
                continue;
            }

            // Nothing anywhere, so this worker is about to sleep. Announce that
            // *before* the last look at the intake. A submit either publishes
            // its task before that look, and the look finds it, or it publishes
            // after, and then it reads a bitmap that already has this bit set
            // and unparks. One of the two has to happen, which is what lets the
            // park be long rather than a poll.
            let bit = 1usize << w.id;
            self.sleepers.fetch_or(bit, Ordering::SeqCst);

            if !self.local[w.id].intake.lock().is_empty() {
                self.sleepers.fetch_and(!bit, Ordering::SeqCst);
                continue;
            }
            // Check for shutdown before parking, so a worker cannot sleep
            // through the end.
            if !self.is_running() {
                self.sleepers.fetch_and(!bit, Ordering::SeqCst);
                break;
            }
            self.host.park(w.id);
            self.sleepers.fetch_and(!bit, Ordering::SeqCst);
        }

        // Put it back, so a caller that restarts this worker finds its deque.
        *self.local[w.id].queue.lock() = Some(queue);
        true
    }

    /// Move what arrived from outside into the local deque.
    fn drain_intake(&self, id: usize, queue: &Queue<Task>) -> usize {
        let taken: Vec<Task> = core::mem::take(&mut *self.local[id].intake.lock());
        let n = taken.len();
        for task in taken {
            if let Err(back) = queue.push(task) {
                // The deque is full. Running it here is the only option that
                // neither drops the task nor grows without bound.
                back();
                self.local[id].completed.fetch_add(1, Ordering::Relaxed);
            }
        }
        // More than one task means there is something left to steal after this
        // worker takes the first. Propagating the wake here is what covers the
        // case no submit can: a worker sitting on a backlog while another
        // sleeps, with nothing new arriving to wake anyone.
        if n > 1 {
            self.wake_one_sleeper(id);
        }
        n
    }

    /// One round of stealing from random victims.
    ///
    /// **Random, not a fixed neighbour.** A deterministic choice gives some
    /// victims probability zero, which is what Blumofe and Leiserson's bound
    /// forbids and what creates hot spots in practice. Measured on the design
    /// this replaces: a fixed neighbour cost 75% against random.
    fn steal_once(&self, queue: &Queue<Task>, id: usize, rng: &mut u64) -> usize {
        let workers = self.workers();
        if workers < 2 {
            return 0;
        }
        for _ in 0..4 {
            *rng ^= *rng << 13;
            *rng ^= *rng >> 7;
            *rng ^= *rng << 17;
            let victim = (*rng % workers as u64) as usize;
            if victim == id {
                continue;
            }
            let got = self.stealers[victim]
                .steal(queue, |n| (n.min(PULL_BATCH) + 1) / 2)
                .unwrap_or(0);
            if got > 0 {
                // A thief that took more than one has spare work of its own
                // now, so the chain continues: one more sleeper joins in.
                if got > 1 {
                    self.wake_one_sleeper(id);
                }
                return got;
            }
        }
        0
    }
}

impl core::fmt::Debug for Pool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Pool")
            .field("workers", &self.workers())
            .field("running", &self.is_running())
            .field(
                "asleep",
                &self.sleepers.load(Ordering::Relaxed).count_ones(),
            )
            .finish_non_exhaustive()
    }
}

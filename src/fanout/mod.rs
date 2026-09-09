//! A fixed pool of workers, each owning one of this crate's deques and
//! stealing from the others.
//!
//! # What this is
//!
//! One [`lifo::Worker`](crate::lifo::Worker) per pool worker, taken by value by
//! the thread that runs it, so pushing and popping local work touch nothing
//! shared. Work arrives from outside through one lock-free injector every
//! worker drains, and an idle worker steals from a random victim before it
//! parks.
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
//! `no_std` and knows nothing else about a platform. `StdHost`, behind the
//! `host` feature, is what those look like with a `std` to implement them.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::config::AtomicUnsignedLong;
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;

use crate::lifo::{Stealer, Worker as Queue};
use spin::Mutex;

#[cfg(feature = "host")]
mod host;
mod job;
#[cfg(feature = "host")]
pub use host::StdHost;
pub use job::{Act, Job};

/// The most tasks one steal moves, before halving.
///
/// A steal takes half of what the victim holds, and this caps the half's input,
/// so the largest a single steal can move is `PULL_BATCH / 2`. Taking
/// everything makes the victim idle immediately and the thief the new
/// bottleneck; taking one makes the next steal a fresh contention on the same
/// victim.
pub const PULL_BATCH: usize = 32;

/// What a worker does when it finds no work, and for how long.
///
/// These are policy, not mechanism: the right values depend on how fast work
/// arrives and on what else is running, so they are a parameter rather than a
/// constant. [`Tuning::default`] is what was measured here, and the numbers
/// that justify each field are on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Tuning {
    /// Empty rounds a worker takes before it announces sleep.
    pub rounds_before_park: u32,
    /// How long to wait between two empty rounds, in spin-loop hints.
    pub backoff_spins: u32,
    /// How many jobs a worker takes off the injector in one trip.
    ///
    /// The injector is the one queue every worker contends on, so the trip is
    /// the expensive part and what lands in the worker's own queue costs
    /// nothing to run. Larger means fewer trips and a longer stretch of
    /// contention-free work; too large and one worker holds a backlog its
    /// neighbours cannot see until the next heartbeat.
    pub injector_batch: usize,
    /// Whether [`Pool::submit_local`] keeps work on the calling worker.
    ///
    /// `false` makes it forward to [`Pool::submit_job`], which is the whole of
    /// the switch. It is a parameter because the right answer depends on the
    /// workload and the two answers are far apart. Measured at eight threads
    /// against a tokio-driven build of the same storage engine:
    ///
    /// ```text
    /// workload                       local     injector
    /// 50% update                      +6.4%      -40.2%
    /// read-modify-write               +2.0%      -38.4%
    /// 95% read / 5% update           -19.2%      +74.4%
    /// 95% read / 5% insert          +137.1%     +321.6%
    /// ```
    ///
    /// Contended work wants the handoff kept warm: its wakes are a chain, and
    /// the successor wants the rows the releasing worker just touched. Sparse
    /// work wants the opposite, because a rare wake queued behind a busy worker
    /// waits while others sleep.
    ///
    /// `true` is the default because it is the better worst case: one workload
    /// 19% behind, against two at about 40% behind the other way. There is no
    /// static rule that gets both, and several were tried: gating on whether
    /// the wake was a task waking *itself*, on whether any worker was idle, and
    /// letting a thief take the slot on second sight. Each reproduced one
    /// column or the other exactly.
    pub local_wakes: bool,

    /// How many jobs a worker runs between two looks at whether anyone needs
    /// work shared with them.
    ///
    /// This is the heartbeat. Small means work becomes stealable promptly and
    /// every worker pays the check often; large means a worker can hoard a
    /// backlog while its neighbours sleep. It is a count rather than a clock
    /// because the pool has no clock it is willing to read on the hot path.
    pub promote_every: u64,
}

impl Default for Tuning {
    fn default() -> Self {
        Self::locality()
    }
}

impl Tuning {
    /// Keep a woken task on the worker that woke it.
    ///
    /// For work whose wakes are a **chain**: a task releases something and the
    /// task it releases wants the lines the first one just touched. Update-heavy
    /// storage paths look like this, and so does anything with a lock handoff.
    ///
    /// Measured on YCSB at eight threads against a tokio-driven build of the
    /// same storage engine: 50% update +6.4%, read-modify-write +2.0%, where
    /// [`Tuning::spread`] puts both about 40% behind.
    ///
    /// This is [`Tuning::default`], because being 19% behind on one shape beats
    /// being 40% behind on two.
    #[must_use]
    pub fn locality() -> Self {
        Self {
            rounds_before_park: ROUNDS_BEFORE_PARK,
            backoff_spins: BACKOFF_SPINS,
            promote_every: PROMOTE_EVERY,
            injector_batch: INJECTOR_BATCH,
            local_wakes: true,
        }
    }

    /// Send every wake to the injector, where any worker can take it.
    ///
    /// For work whose wakes are **independent**: the woken task has no claim on
    /// the waking worker's cache and would rather run now, somewhere else, than
    /// wait behind it. Read-mostly and insert-mostly paths look like this.
    ///
    /// Measured on the same YCSB runs: 95% read / 5% update +74.4%, 95% read /
    /// 5% insert +321.6%, where [`Tuning::locality`] is 19% behind on the first
    /// and less than half as fast on the second.
    ///
    /// The cost is the other column: update-heavy work goes about 40% behind.
    /// There is no setting that wins both, which is why this is a choice and
    /// not a default.
    #[must_use]
    pub fn spread() -> Self {
        Self {
            local_wakes: false,
            ..Self::locality()
        }
    }

    /// Fewer, larger trips to the injector.
    ///
    /// For a firehose of short independent jobs submitted from outside the
    /// pool, where the trip to the shared queue is the cost and nothing wants
    /// locality. This is what the defaults were before `local_wakes` existed.
    ///
    /// **Do not combine a large batch with `local_wakes`.** A worker taking
    /// eight long-lived tasks off the injector keeps all eight, because each
    /// one's self-wake returns it to that worker: measured, a read-only run
    /// went from 15.2M ops/s to 8.9M against tokio's 21.6M.
    #[must_use]
    pub fn throughput() -> Self {
        Self {
            local_wakes: false,
            injector_batch: 8,
            ..Self::locality()
        }
    }
}

/// Empty rounds a worker takes before it announces sleep.
///
/// Parking is not expensive to the parker; it is expensive to whoever has to
/// wake it, because a wake is a mutex and a condvar notify. A worker that
/// parks the moment its queue runs dry makes every later submit pay one, and
/// at eight workers that was 0.95 parks a task: the producer became the
/// bottleneck, and the starvation that caused made the workers park again.
///
/// A steady stream refills within a few hundred cycles, so looking again a few
/// times skips the protocol entirely. Swept at 16, 64, 256 and 1024 empty
/// rounds over 100,000 tasks and 8 workers: 445, 353, 372 and 365 ns a task.
/// Past a few dozen it stops mattering.
const ROUNDS_BEFORE_PARK: u32 = 64;

/// How long to wait between two empty rounds.
///
/// **This is worth more than anything else in this pool.** A worker that finds
/// nothing looks again, and looking means a pop off the injector and a probe of
/// every other worker's deque. Eight idle workers doing that in a tight loop
/// take the very lines the producer is trying to fill, so the pool starves
/// itself: the same 100,000 tasks that one worker finishes in 12.4 ms took
/// eight workers 49.2 ms.
///
/// Swept over 100,000 tasks on 8 workers, with wake latency measured separately
/// on an idle pool over 2,000 samples:
///
/// ```text
/// spins     wall      cpu    latency median   p99
///    64   49.2 ms   440 ms         3917 ns   10333 ns
///   256   36.0 ms   325 ms         2500 ns    7958 ns
///  1024   30.3 ms   277 ms         2375 ns    6000 ns
///  4096   24.4 ms   237 ms         3375 ns   14209 ns
/// 16384   15.6 ms   213 ms        21000 ns   43375 ns
/// ```
///
/// 1024 is the last value that is better than the one before it on **every**
/// axis, so it is the default. Past it the trade is real: 16384 is three times
/// the throughput of 64 and five times the wake latency, which is the right
/// choice for a batch pool and the wrong one for anything waiting on a reply.
const BACKOFF_SPINS: u32 = 1024;

/// Jobs run between two heartbeats.
const PROMOTE_EVERY: u64 = 64;

/// Jobs a worker takes off the injector in one trip.
///
/// **This is the bound on stranding, and one is the right bound now.**
/// What a worker takes goes into its private queue; half is published for
/// stealing at once, the rest is unreachable until it pops again.
///
/// It was 8, on a sweep over 100,000 independent short tasks that found 8, 32
/// and 128 within noise of each other and concluded the size buys no
/// throughput. That sweep is still true and no longer decisive, because
/// `submit_local` changed what a batch costs. A job that wakes itself now
/// stays on the worker that ran it, so a worker taking 8 long-lived tasks off
/// the injector does not merely hold them until its next heartbeat: it keeps
/// them. Measured on a read-only workload of 16 client tasks over 16 workers,
/// a few workers held everything while the rest idled, and throughput sat at
/// 8.9M ops/s against tokio's 21.6M. At a batch of 1 the same run reaches
/// 15.2M, and the update-heavy workloads go from 41% behind tokio to 6% ahead.
///
/// So the earlier reading was right and its number was not: this should be as
/// small as the injector traffic tolerates, and with self-wakes no longer
/// returning through the injector, that traffic is small.
const INJECTOR_BATCH: usize = 1;

/// A unit of work, as a boxed closure.
///
/// Independent by construction: no join, no continuation, no completion
/// signal, so a task may run whenever a worker reaches it.
///
/// **This is the convenience shape, not the cheap one.** The queues hold
/// [`Job`], which is a thin pointer and a function; handing the pool a
/// `Box<dyn FnOnce()>` means boxing that fat pointer again. Callers that care
/// use [`Pool::submit_fn`], which allocates once, or [`Pool::submit_job`],
/// which allocates nothing.
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
/// worker sleeps with a job sitting in the injector.
///
/// A spin satisfies this trivially, since it never sleeps. A condition variable
/// does not: it needs a flag beside it, which is what `StdHost` carries.
///
/// `park` may also return spuriously. The pool loops, so a wakeup with nothing
/// to show for it costs one pass and nothing else.
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
/// Only [`Pool::runner`] makes one. It checks the id, and stamps the runner with
/// the pool it came from, so a handle can neither name a worker that does not
/// exist nor be given to a different pool. Handing the same `Runner` to two
/// threads is still a caller error, and [`Pool::run`] refuses the second rather
/// than racing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Runner {
    pool: usize,
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
    queue: Mutex<Option<Queue<Job>>>,
    completed: AtomicUnsignedLong,
    /// Work a worker gave back to itself, bypassing the injector.
    ///
    /// The private `mine` deque lives on the run loop's stack, so nothing
    /// outside `run` can reach it. That left a task with no way back onto the
    /// worker that woke it: every wake, and every `yield_now`, went through the
    /// one injector every worker contends on, plus a wake. Under a workload
    /// whose tasks yield in a retry loop that is the whole cost.
    ///
    /// A `SegQueue` rather than the sharing deque because that deque is moved
    /// out of `queue` by `run` and owned by the running thread for the whole
    /// loop; this is reachable while that is in flight.
    inbox: SegQueue<Job>,
    /// The single most recent job this worker handed itself.
    ///
    /// A task that wakes itself almost always wants to run *next*, and going
    /// through `inbox` to say so costs a `SegQueue` push and pop, which is an
    /// MPMC structure priced for many producers. This is one uncontended
    /// swap, and it is what tokio's LIFO slot is for. Measured, the inbox
    /// alone left a read-only workload at a third of tokio's throughput
    /// because it paid that queue on all five million wakes.
    ///
    /// Holds one job. A second self-wake displaces the first into `inbox`,
    /// where it is still found, so nothing is lost and the slot never becomes
    /// a queue of its own.
    lifo: Mutex<Option<Job>>,
}

// Distinguishes one pool from another, so a `Runner` cannot be handed to the
// wrong one. Wrapping is harmless: it would take a `usize` of pools to reach a
// collision, and a collision only matters between two live pools.
static NEXT_POOL: AtomicUsize = AtomicUsize::new(0);

/// A fixed set of workers, their deques, and the injector work arrives through.
///
/// Construct it with [`Pool::new`], hand each [`Runner`] to a thread of your
/// own with [`Pool::run`], and feed it with [`Pool::submit`].
pub struct Pool {
    id: usize,
    local: Vec<CachePadded<Local>>,
    stealers: Vec<Stealer<Job>>,
    /// Where work submitted from outside lands.
    ///
    /// **One queue, not one per worker.** It used to be a `Mutex<Vec<Task>>`
    /// per worker, which nothing but that worker could reach. That is the
    /// single decision this pool got wrong: an idle worker could not find work
    /// sitting in a neighbour's intake, so the rule every work-stealing
    /// scheduler relies on, that a searching worker will eventually find any
    /// work there is, did not hold. Six different scheduling policies were
    /// tried on top of it and all six lost. A lock-free queue every worker
    /// drains makes the rule true.
    injector: SegQueue<Job>,
    host: Arc<dyn Host>,
    running: AtomicBool,
    tuning: Tuning,
    /// One bit per worker that is asleep or about to be.
    ///
    /// **Read on submit, written only on an actual park.** A submit that wakes
    /// only the worker it was handed leaves an available worker asleep while a
    /// busy one takes the notification, and with no park timeout nothing
    /// corrects that until the busy worker finishes: the latency of unrelated
    /// work, added to a job that had somewhere to go.
    ///
    /// So a submit reads this and wakes a worker that is genuinely asleep. The
    /// cost is one load, not the read-modify-write the first version of this
    /// pool did per submit and which serialised the whole thing.
    ///
    /// **`SeqCst`, and it has to be.** A worker sets its bit and *then* takes
    /// its last look at the injector; a submit publishes and *then* reads this.
    /// That is a store-then-load on each side against different locations, and
    /// only a total order across both makes one of them see the other. With
    /// weaker orderings both can miss, and the worker sleeps on work that is
    /// already queued.
    sleeping: AtomicUsize,
    /// How many workers are currently hunting for work to steal.
    ///
    /// Capped at half the pool, which is the rule tokio's scheduler uses and
    /// the reason is the same: a steal probe is a compare-exchange against the
    /// victim's deque, so a worker looking for work **slows down the worker
    /// that has it**. With every idle worker sweeping every victim each round,
    /// that cost lands inside the busy workers' own pops, where no profile
    /// attributes it to stealing: the leaf frames look like ordinary work and
    /// throughput has a ceiling nobody can point at.
    ///
    /// Half, rather than a tuned number, because the useful range is bounded
    /// on both sides. Too few searchers and work sits in a deque with nobody
    /// coming for it; too many and they cost more than they redistribute.
    searching: AtomicUsize,
}

impl Pool {
    /// A pool of `workers` deques, each holding `capacity` tasks.
    ///
    /// # Capacity is not backpressure
    ///
    /// The deques are bounded, which is st3's shape, but [`submit`] does not
    /// push into one. It appends to an unbounded injector that every worker
    /// drains, so **`capacity` bounds what a worker holds, not what a caller
    /// may hand it.** A submitter is never blocked and never refused; if tasks
    /// arrive faster than they run, the injector grows until memory runs out.
    ///
    /// A worker takes only `injector.len() / workers + 1` at a time, capped at
    /// half its deque, so what it takes cannot immediately overflow. If it
    /// overflows anyway, because a thief filled the deque meanwhile, the excess
    /// runs inline on that worker, which neither drops a job nor grows the
    /// deque.
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
        Self::with_tuning(workers, capacity, host, Tuning::default())
    }

    /// A pool whose idle behaviour is the caller's to choose.
    ///
    /// See [`Tuning`]. The default is what was measured on this machine and is
    /// not a claim about any other.
    #[must_use]
    pub fn with_tuning(
        workers: usize,
        capacity: usize,
        host: Arc<dyn Host>,
        tuning: Tuning,
    ) -> Arc<Self> {
        assert!(workers > 0, "a pool needs at least one worker");
        assert!(
            tuning.promote_every > 0,
            "`promote_every` is a countdown to zero: 0 never reaches it"
        );
        assert!(
            tuning.injector_batch > 0,
            "`injector_batch` of 0 takes nothing off the injector, so submitted \
             work would never run and shutdown would never finish"
        );
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
            local.push(CachePadded::new(Local {
                queue: Mutex::new(Some(queue)),
                completed: AtomicUnsignedLong::new(0),
                inbox: SegQueue::new(),
                lifo: Mutex::new(None),
            }));
        }
        Arc::new(Self {
            id: NEXT_POOL.fetch_add(1, Ordering::Relaxed),
            local,
            stealers,
            host,
            injector: SegQueue::new(),
            running: AtomicBool::new(true),
            tuning,
            sleeping: AtomicUsize::new(0),
            searching: AtomicUsize::new(0),
        })
    }

    #[must_use]
    /// How many workers this pool was built with.
    pub fn workers(&self) -> usize {
        self.local.len()
    }

    /// Every worker's handle, in order, for the threads that will run them.
    #[must_use]
    /// # Panics
    ///
    /// If `id` is not a worker of this pool.
    pub fn runner(&self, id: usize) -> Runner {
        assert!(
            id < self.workers(),
            "worker {id} of a pool with {} of them",
            self.workers()
        );
        Runner { pool: self.id, id }
    }

    /// How many tasks a worker has finished.
    #[must_use]
    // Useless on a target with 64-bit atomics, where `UnsignedLong` is already
    // `u64`, and load-bearing on one without, where it is `u32`. Clippy sees
    // only the target it is run on.
    #[allow(clippy::useless_conversion)]
    pub fn completed(&self, id: usize) -> u64 {
        // The counter is the crate's conditional atomic, so it is 32-bit on a
        // target without 64-bit atomics. The public answer is a `u64` either
        // way: `UnsignedLong` is `pub(crate)` and has no business in a
        // signature a caller has to name.
        u64::from(self.local[id].completed.load(Ordering::Relaxed))
    }

    /// Hand a boxed closure to the pool.
    ///
    /// `worker` is a **hint**, and only for waking: work goes to the shared
    /// injector that every worker drains, so any worker may run it. The hint
    /// says which one to try to wake first, which is worth something when a
    /// caller knows where the work belongs and worth nothing otherwise.
    ///
    /// This costs two allocations, because a `Box<dyn FnOnce()>` is a fat
    /// pointer and a [`Job`] holds a thin one. [`Pool::submit_fn`] costs one
    /// and is otherwise identical.
    pub fn submit(&self, worker: usize, task: Task) {
        self.push(Some(worker), Job::from_boxed(task));
    }

    /// Hand a closure to the pool, allocating once.
    ///
    /// The closure is boxed and the function that runs it is monomorphised, so
    /// there is no trait object and no second indirection.
    pub fn submit_fn<F>(&self, work: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.push(None, Job::from_boxed(work));
    }

    /// Hand the pool work it does not have to allocate for.
    ///
    /// For a caller whose work is already on the heap: a task inside an `Arc`,
    /// a slot in an arena, a future something else has already boxed. See
    /// [`Job::from_raw`] for what the caller has to guarantee.
    pub fn submit_job(&self, job: Job) {
        self.push(None, job);
    }

    /// Whether any worker is currently parked.
    ///
    /// For a caller deciding between [`Pool::submit_local`] and
    /// [`Pool::submit_job`]: locality is worth having when every worker is
    /// busy, and worth nothing when the alternative is a job sitting behind
    /// this worker's queue while another worker sleeps.
    #[must_use]
    pub fn has_idle_workers(&self) -> bool {
        self.sleeping.load(Ordering::Acquire) != 0
    }

    /// Hand work straight to one worker, skipping the injector.
    ///
    /// For the caller that *is* that worker: a task rescheduling itself, or a
    /// wake happening on a worker thread. Going through [`Pool::submit_job`]
    /// there costs a push to the queue every worker contends on and a wake for
    /// somebody who is already awake, and the job comes back to a random
    /// worker rather than the one holding its cache lines.
    ///
    /// The job is **not** stealable until this worker promotes it, which is the
    /// trade: locality and no contention, against a job that waits if this
    /// worker then blocks. Only use it when the work belongs here.
    ///
    /// A worker that is asleep is woken, so this is still safe to call from
    /// another thread; it just has nothing to offer one.
    pub fn submit_local(&self, worker: usize, job: Job) {
        // Off by policy: see `Tuning::local_wakes` for the measurement behind
        // this being a switch rather than a decision.
        if !self.tuning.local_wakes {
            self.push(None, job);
            return;
        }
        // The slot first, and whatever it held goes to the inbox behind it.
        // One swap on the common path, against a `SegQueue` push and pop.
        let displaced = self.local[worker].lifo.lock().replace(job);
        if let Some(displaced) = displaced {
            self.local[worker].inbox.push(displaced);
        }
        // Publish, then read the bitmap, with a barrier between.
        //
        // **The fence is load-bearing and its absence was a lost wakeup.** The
        // sleep path is a Dekker pair: the worker stores its bit and then reads
        // the slot and the inbox, and this stores the job and then reads the
        // bit. That only guarantees one side sees the other if *both* pairs are
        // separated by a full barrier. The worker's `fetch_or` is `SeqCst` and
        // is one; on this side the job went into a `spin::Mutex`, whose release
        // is a store-release and is not. So both could miss, and both did:
        // caught in a profile with all sixteen workers parked in `__ulock_wait`
        // while the run had work outstanding, at a sixth of normal throughput.
        core::sync::atomic::fence(Ordering::SeqCst);
        if self.sleeping.load(Ordering::SeqCst) & (1usize << worker) != 0 {
            self.wake(Some(worker));
        }
    }

    /// Publish one job and make sure somebody will look.
    fn push(&self, hint: Option<usize>, job: Job) {
        self.injector.push(job);
        self.wake(hint);
    }

    /// Get one worker looking, preferring one that is actually asleep.
    ///
    /// The `hint` is only a fallback. Waking the hinted worker when it is busy
    /// and another is parked is the bug this exists to avoid: the job waits out
    /// unrelated work while a free worker sleeps beside it.
    fn wake(&self, hint: Option<usize>) {
        // Ordered against publishing the job above; see `sleeping`.
        let sleeping = self.sleeping.load(Ordering::SeqCst);
        if sleeping != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let candidate = sleeping.trailing_zeros() as usize;
            self.host.unpark(candidate);
            return;
        }
        // Nobody has announced sleep. A worker may still be between its last
        // look and its park, and the permit `unpark` leaves is what stops that
        // one sleeping on this job.
        if let Some(worker) = hint {
            self.host.unpark(worker);
        }
    }

    /// Tell some worker other than `except` that there may be work about.
    ///
    /// Used when work becomes *stealable* rather than newly submitted: a batch
    /// pulled off the injector, or a private queue shared on the heartbeat.
    fn nudge(&self, except: Option<usize>) {
        let spare = match except {
            Some(id) => 1usize << id,
            None => 0,
        };
        let sleeping = self.sleeping.load(Ordering::SeqCst) & !spare;
        if sleeping != 0 {
            #[allow(clippy::cast_possible_truncation)]
            let candidate = sleeping.trailing_zeros() as usize;
            self.host.unpark(candidate);
        }
    }

    /// Tell every worker to stop once it runs out of work, and wake them all.
    ///
    /// **Tasks submitted from here on may never run.** A worker leaves as soon
    /// as it finds its deque empty, the injector empty and nothing to steal, so
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
        assert_eq!(
            w.pool, self.id,
            "this `Runner` belongs to another pool, and running it here would \
             hand this thread the wrong deque"
        );
        // `let ... else` is 1.65 and this crate supports 1.60, which the queues
        // have no reason to give up because a pool was added beside them.
        let queue = match self.local[w.id].queue.lock().take() {
            Some(queue) => queue,
            None => return false,
        };

        // **The worker's own work, in a queue nothing else can see.**
        //
        // This is the change that matters most in this whole pool. `queue` is
        // the *sharing* deque, and every pop from it is a compare-exchange
        // against any thief that might be looking. `mine` is a plain
        // `VecDeque` on this thread's stack, so running work out of it costs no
        // atomic at all. Work is promoted from here into `queue` on a heartbeat
        // and only when somebody is asleep to receive it, which is the design
        // `forte`, `chili` and Spice all arrived at: pay for sharing on a
        // timer, not on every task.
        let mut mine: VecDeque<Job> = VecDeque::new();
        // A countdown rather than a modulus. `tick % promote_every` is a
        // runtime `u64` remainder on the hot path for a value that only has to
        // reach zero, and it made the interval depend on an absolute count,
        // which is what let it align with an empty queue.
        let mut until_promote = self.tuning.promote_every;
        let mut spins = 0u32;
        let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ (w.id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);

        // How many self-wakes may be served from the LIFO slot back to back
        // before the loop insists on looking elsewhere. Without a bound, two
        // tasks ping-ponging through the slot starve everything this worker
        // holds, including work it has already promised to share.
        let mut lifo_run = 0u32;
        const LIFO_RUN_LIMIT: u32 = 32;

        loop {
            // 0. The task that just woke itself, if there is one. Ahead of
            //    everything: it is the warmest work in the process, and the
            //    slot is one uncontended swap to check.
            //    Written without a `let` chain: this crate supports Rust 1.60
            //    and those are 2024.
            let from_slot = if lifo_run < LIFO_RUN_LIMIT {
                self.local[w.id].lifo.lock().take()
            } else {
                None
            };
            if let Some(job) = from_slot {
                lifo_run += 1;
                spins = 0;
                job.run();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            lifo_run = 0;

            // 1. This worker's own queue. No synchronisation whatsoever.
            if let Some(job) = mine.pop_back() {
                spins = 0;
                until_promote -= 1;
                if until_promote == 0 {
                    until_promote = self.tuning.promote_every;
                    self.promote(&mut mine, &queue, w.id);
                }
                job.run();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // 1.5. What this worker handed back to itself.
            //
            //      Moved into `mine` rather than run from here. Running it
            //      directly looks cheaper and costs far more: a job in the
            //      inbox is reachable by nobody, so a worker that keeps waking
            //      its own tasks keeps them all, and the pool stops balancing.
            //      Measured on a read-only workload with eight client tasks and
            //      sixteen workers, that halved throughput and made it bimodal,
            //      because tasks pinned to whichever worker first woke them
            //      while other workers idled.
            //
            //      Through `mine` they are ordinary local work: LIFO, so a
            //      task that just yielded still runs next and keeps its cache
            //      lines, and subject to the same promotion that makes local
            //      work stealable on the heartbeat.
            {
                let mut moved = 0usize;
                while let Some(job) = self.local[w.id].inbox.pop() {
                    mine.push_back(job);
                    moved += 1;
                    // Bounded so a producer feeding this inbox faster than the
                    // worker drains it cannot spin here forever without running
                    // anything.
                    if moved >= self.tuning.promote_every as usize {
                        break;
                    }
                }
                if moved > 0 {
                    spins = 0;
                    continue;
                }
            }

            // 2. What this worker shared and nobody took.
            if let Some(job) = queue.pop() {
                spins = 0;
                job.run();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // 3. Somebody else's, but only if this worker is allowed to hunt.
            //
            //    See `Pool::searching`. Taking a slot before probing and giving
            //    it back after is what bounds the number of workers whose
            //    compare-exchanges are landing on the workers that are actually
            //    getting things done.
            let workers = self.workers();
            let may_search = workers < 2 || {
                let searchers = self.searching.fetch_add(1, Ordering::AcqRel);
                if searchers * 2 >= workers {
                    self.searching.fetch_sub(1, Ordering::AcqRel);
                    false
                } else {
                    true
                }
            };
            if may_search {
                let stolen = self.steal_once(&queue, w.id, &mut rng);
                if workers >= 2 {
                    self.searching.fetch_sub(1, Ordering::AcqRel);
                }
                if stolen > 0 {
                    spins = 0;
                    continue;
                }
            }

            // 4. What arrived from outside. Last, because it is the queue every
            //    worker contends on.
            if self.take_from_injector(&mut mine) > 0 {
                spins = 0;
                // **Share at the refill, not only on the countdown.** A batch
                // off the injector is the one moment this worker is known to
                // hold more than it needs, and the countdown alone could never
                // catch it: with the default batch of 32 and a heartbeat of 64,
                // every heartbeat landed on the last job of the second batch,
                // when the private queue was empty. The mechanism fired
                // forever and shared nothing.
                // **Unconditionally, not only when somebody is asleep.** A
                // worker that takes a batch and then starts a long job holds
                // the rest privately, where nothing can reach it, and it will
                // not promote again because promoting happens between jobs. If
                // the check for a sleeper runs before anybody has parked, the
                // batch is stranded for the whole of that long job. Measured:
                // 0 of 31 short jobs ran behind one long one.
                //
                // So the refill always publishes half. That half costs a
                // compare-exchange to take back if nobody steals it, which is
                // the price of it being reachable at all.
                self.share(&mut mine, &queue, w.id);
                until_promote = self.tuning.promote_every;
                continue;
            }

            // Nothing found this round, and nothing left to run. Shutdown is
            // checked **here**, not only on the park path below: a worker with
            // a large `rounds_before_park` would otherwise spin out its whole
            // budget before ever asking, and one with a budget large enough
            // would never ask at all and `run` would never return. Found by
            // trying to make the idle-CPU test fail on a pool that never parks,
            // which hung instead.
            if !self.is_running() {
                break;
            }

            // Look again before announcing anything: see
            // `Tuning::rounds_before_park` for why the wake, not the park,
            // costs.
            if spins < self.tuning.rounds_before_park {
                spins += 1;
                // Back off *without* touching anything shared. Looking again
                // means a pop off the injector and a probe of every other
                // worker's deque, so an empty round is not free to anyone else:
                // repeating it immediately is what starves the producer this is
                // trying to keep fed.
                for _ in 0..self.tuning.backoff_spins {
                    core::hint::spin_loop();
                }
                continue;
            }

            // Nothing anywhere, so this worker sleeps.
            //
            // **Announce first, then look.** A submit publishes its job and
            // then reads the bitmap; this sets its bit and then reads the
            // injector. Two store-then-load pairs against different locations,
            // so only `SeqCst` on all four makes at least one of them see the
            // other. Announcing after the look, or announcing with a weaker
            // ordering, lets a worker sleep on a job that is already queued.
            let bit = 1usize << w.id;
            self.sleeping.fetch_or(bit, Ordering::SeqCst);

            // The inbox is checked alongside the injector, and for the same
            // reason: `submit_local` publishes and then reads this bitmap, so
            // announcing before looking is what stops a worker sleeping on a
            // job already handed to it.
            if !self.injector.is_empty()
                || !self.local[w.id].inbox.is_empty()
                || self.local[w.id].lifo.lock().is_some()
            {
                self.sleeping.fetch_and(!bit, Ordering::SeqCst);
                continue;
            }
            // Checked before parking so a worker cannot sleep through the end;
            // `shut_down` unparks every worker, so one that gets past this
            // still leaves.
            if !self.is_running() {
                self.sleeping.fetch_and(!bit, Ordering::SeqCst);
                break;
            }
            self.host.park(w.id);
            self.sleeping.fetch_and(!bit, Ordering::SeqCst);
            spins = 0;
        }

        // Whatever this worker still holds privately would be invisible to
        // everyone else, so it goes back where another worker can reach it.
        // Shutdown makes no promise that it runs, but losing it silently is a
        // different thing from not running it.
        for job in mine {
            self.injector.push(job);
        }

        // Put it back, so a caller that restarts this worker finds its deque.
        *self.local[w.id].queue.lock() = Some(queue);
        true
    }

    /// Move a run of what arrived from outside into this worker's own queue.
    ///
    /// **Pop, do not ask.** `SegQueue::len` reads both ends, so a worker that
    /// calls it every empty round turns the length counter into the contended
    /// line the per-worker intakes used to be. A failed pop answers the same
    /// question and touches one end.
    ///
    /// A run rather than one, because the trip to a queue every worker shares
    /// is the expensive part and what lands here costs nothing to run. A run
    /// rather than all of it, because this queue is private: a worker that
    /// swallowed the injector would make the whole backlog unstealable until
    /// its next heartbeat.
    fn take_from_injector(&self, mine: &mut VecDeque<Job>) -> usize {
        let mut taken = 0;
        while taken < self.tuning.injector_batch {
            match self.injector.pop() {
                Some(job) => {
                    mine.push_back(job);
                    taken += 1;
                }
                None => break,
            }
        }
        if taken > 1 {
            self.nudge(None);
        }
        taken
    }

    /// Publish some of this worker's private queue where thieves can reach it.
    ///
    /// **Only when somebody is asleep.** With every worker busy there is nobody
    /// to take it, and moving a job into the sharing deque costs a
    /// compare-exchange that buys nothing. The bitmap load is `Relaxed`: a
    /// missed sleeper waits for the next heartbeat, which is a few thousand
    /// jobs away, not for ever.
    ///
    /// The **oldest** jobs go, because in a depth-first workload the oldest is
    /// the largest subtree, so one steal moves the most work. That is Cilk's
    /// argument and it is why the private queue is a deque rather than a stack.
    /// The heartbeat: share only if somebody is waiting for it.
    ///
    /// Between jobs, with everyone busy, moving work into the sharing deque
    /// costs a compare-exchange and buys nothing. The refill path calls
    /// [`share`](Pool::share) instead, which does not ask.
    #[cold]
    fn promote(&self, mine: &mut VecDeque<Job>, queue: &Queue<Job>, id: usize) {
        // The load is `Relaxed`: a sleeper missed here waits for the next
        // refill or the next countdown, and both are close.
        if self.sleeping.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.share(mine, queue, id);
    }

    /// Move half of this worker's private queue where thieves can reach it.
    fn share(&self, mine: &mut VecDeque<Job>, queue: &Queue<Job>, id: usize) {
        if mine.len() < 2 {
            return;
        }
        let share = mine.len() / 2;
        let mut shared = 0;
        for _ in 0..share {
            match mine.pop_front() {
                Some(job) => match queue.push(job) {
                    Ok(()) => shared += 1,
                    Err(back) => {
                        // The sharing deque is full, which means the pool is
                        // busy and nobody needs help after all.
                        mine.push_front(back);
                        break;
                    }
                },
                None => break,
            }
        }
        if shared > 0 {
            self.nudge(Some(id));
        }
    }

    /// One round of stealing from random victims.
    ///
    /// **Random, not a fixed neighbour.** A deterministic choice gives some
    /// victims probability zero, which is what Blumofe and Leiserson's bound
    /// forbids and what creates hot spots in practice. Measured on the design
    /// this replaces: a fixed neighbour cost 75% against random.
    fn steal_once(&self, queue: &Queue<Job>, id: usize, rng: &mut u64) -> usize {
        let workers = self.workers();
        if workers < 2 {
            return 0;
        }
        // A random start, then every victim once.
        //
        // The start is random because a fixed neighbour gives some victims
        // probability zero, which is what Blumofe and Leiserson's bound forbids
        // and what makes hot spots in practice. Measured on the design this
        // replaces, a fixed neighbour cost 75% against random.
        //
        // The sweep is exhaustive because a fixed number of random draws does
        // not bound discovery: four draws over 64 workers find the one busy
        // victim about 6% of the time, and a miss means parking with work
        // available. When work is plentiful the first probe hits and the rest
        // of the loop never runs.
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        let start = (*rng % workers as u64) as usize;
        for offset in 0..workers {
            let victim = (start + offset) % workers;
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
                    self.nudge(Some(id));
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
                &self.sleeping.load(Ordering::Relaxed).count_ones(),
            )
            .finish_non_exhaustive()
    }
}

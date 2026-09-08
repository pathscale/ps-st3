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
pub use job::Job;

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
        Self {
            rounds_before_park: ROUNDS_BEFORE_PARK,
            backoff_spins: BACKOFF_SPINS,
            promote_every: PROMOTE_EVERY,
            injector_batch: PULL_BATCH,
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
    /// Round-robin cursor for choosing which worker a hintless submit nudges.
    next_hint: AtomicUsize,
    /// How many workers are asleep right now.
    ///
    /// **Not the wake mechanism**, which is per-worker and needs no shared
    /// state; this is only so a worker can skip sharing its private queue when
    /// there is nobody to share with. It is `Relaxed` and touched only when a
    /// worker actually parks or wakes, which the backoff makes rare, so it is
    /// not the contended line the sleeper bitmap it replaces was.
    asleep: AtomicUsize,
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
            next_hint: AtomicUsize::new(0),
            asleep: AtomicUsize::new(0),
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
        self.push(worker, Job::from_boxed(task));
    }

    /// Hand a closure to the pool, allocating once.
    ///
    /// The closure is boxed and the function that runs it is monomorphised, so
    /// there is no trait object and no second indirection.
    pub fn submit_fn<F>(&self, work: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.push(self.hint(), Job::from_boxed(work));
    }

    /// Hand the pool work it does not have to allocate for.
    ///
    /// For a caller whose work is already on the heap: a task inside an `Arc`,
    /// a slot in an arena, a future something else has already boxed. See
    /// [`Job::from_raw`] for what the caller has to guarantee.
    pub fn submit_job(&self, job: Job) {
        self.push(self.hint(), job);
    }

    /// Round-robin, for submitters that expressed no preference. It only
    /// chooses who to *wake*, so a poor choice costs a wakeup and not a task.
    fn hint(&self) -> usize {
        self.next_hint.fetch_add(1, Ordering::Relaxed) % self.workers()
    }

    /// Publish one job and make sure somebody will look.
    fn push(&self, worker: usize, job: Job) {
        self.injector.push(job);
        // **Unconditionally, and without asking who is asleep.** There used to
        // be a pool-wide bitmap of sleepers, consulted here so the unpark could
        // be skipped when nobody was on the other end. That bitmap was one
        // cache line taking a `SeqCst` read-modify-write from every worker that
        // parked and every submit that looked, and it cost more than the wake
        // it saved: parking sooner made the pool spend 818 ms of CPU against
        // 445. `Host::unpark` on a running worker is now a single swap of that
        // worker's own padded word, with no system call, so asking first buys
        // nothing.
        //
        // The race is closed by the park protocol rather than here: a worker
        // that looks at the injector, finds it empty, and then parks finds the
        // signal this leaves and does not sleep. See `StdHost::park`.
        self.host.unpark(worker);
    }

    /// Tell some worker other than `except` that there may be work about.
    ///
    /// Round-robin rather than "whoever is asleep", because knowing who is
    /// asleep needs a shared bitmap and that bitmap was this pool's most
    /// contended line. A signal to a worker that is already running is one swap
    /// of its own padded word, so the worst a wrong guess costs is one wasted
    /// pass around that worker's loop.
    fn nudge(&self, except: Option<usize>) {
        let workers = self.workers();
        if workers < 2 {
            return;
        }
        let mut candidate = self.next_hint.fetch_add(1, Ordering::Relaxed) % workers;
        if Some(candidate) == except {
            candidate = (candidate + 1) % workers;
        }
        self.host.unpark(candidate);
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
        let mut tick = 0u64;
        let mut spins = 0u32;
        let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ (w.id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);

        loop {
            // 1. This worker's own queue. No synchronisation whatsoever.
            if let Some(job) = mine.pop_back() {
                spins = 0;
                tick = tick.wrapping_add(1);
                if tick % self.tuning.promote_every == 0 {
                    self.promote(&mut mine, &queue, w.id);
                }
                job.run();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // 2. What this worker shared and nobody took.
            if let Some(job) = queue.pop() {
                spins = 0;
                job.run();
                self.local[w.id].completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // 3. Somebody else's.
            if self.steal_once(&queue, w.id, &mut rng) > 0 {
                spins = 0;
                continue;
            }

            // 4. What arrived from outside. Last, because it is the queue every
            //    worker contends on.
            if self.take_from_injector(&mut mine) > 0 {
                spins = 0;
                continue;
            }

            // Nothing found this round. Look again before announcing anything:
            // see `Tuning::rounds_before_park` for why the wake, not the park,
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

            // Nothing anywhere, so this worker sleeps. One last look at the
            // injector first, and then the park protocol closes the race: a
            // submit landing after this look leaves a signal that `park`
            // consumes instead of sleeping on.
            if !self.injector.is_empty() {
                continue;
            }
            // Checked before parking so a worker cannot sleep through the end;
            // `shut_down` unparks every worker, so one that gets past this
            // still leaves.
            if !self.is_running() {
                break;
            }
            self.asleep.fetch_add(1, Ordering::Relaxed);
            self.host.park(w.id);
            self.asleep.fetch_sub(1, Ordering::Relaxed);
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
    #[cold]
    fn promote(&self, mine: &mut VecDeque<Job>, queue: &Queue<Job>, id: usize) {
        if self.asleep.load(Ordering::Relaxed) == 0 {
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
            .field("asleep", &self.asleep.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

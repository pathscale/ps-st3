# Design choices, and the review that tested them

This records an external review of the `fanout` pool, finding by finding, with
the verdict on each. It is written this way because a summary of a review loses
the asks: a section headed "accepted" hides the three things nobody did, and a
finding waved away in a sentence leaves no record of the argument. So each one
is here in the reviewer's own terms, followed by whether it stands.

The review was source inspection only, no compilation and no measurement, which
the reviewer stated. That matters when reading the severities: seven findings
are labelled P1, and the measurements taken separately say the submit path is
not what separates this pool from a hand-rolled scoped-thread fan-out. A real
defect and an expensive-looking line of code are not the same thing, and only
one of those can be found by reading.

## What changed as a result

Seven of the ten were fixed on the branch: the two outright bugs, the atomics,
the conditional wake, the recycled buffer, the cache padding and the exhaustive
steal sweep. Measured on the same example before and after, three runs each,
with the null arm at 0.99 to 1.00 throughout:

| | before | after |
|---|---|---|
| this pool, three runs | 9.6, 10.6, 10.3 ms | 9.6, 9.5, 9.5 ms |
| against the hand-rolled fan-out | 8 to 12% behind | 3 to 6% behind |

The spread matters more than the median here. It fell from a full millisecond to
a tenth of one, which is what removing a mutex acquisition and a condvar notify
from every submission looks like.

Three were not fixed and are recorded below with the reason: the boxed task API,
the strength of the exactly-once test, and the absence of a latency benchmark.

## Accepted, and true

### The `Runner` documentation was false

> `Runner`'s documented safety contract is false. The documentation says
> `runner` checks the ID and associates it with its pool, but `runner` merely
> stores any integer. An invalid ID panics in `run`; a runner from another pool
> is accepted.

Correct, and it is the worst finding in the set because of where it came from.
That sentence was written *while* correcting three other false doc statements
inherited from the crate this pool arrived in. `Pool::runner` is
`Runner { id }`, and it checks nothing.

Two separate defects sit under it: the id is unchecked, so an out-of-range one
panics inside `run` rather than being refused, and a `Runner` from one pool is
accepted by another, which hands a second thread the wrong deque.

The reviewer also notes a third thing worth keeping: work submitted to a worker
whose `Runner` was never given a thread sits in that worker's private intake
where **no other worker can steal it**. That is a real hole in the design, not
just in the docs, and it is the one item here that changes what the pool
guarantees.

### The steal wake fires after the inline overflow, not before

> Overflow handling creates unbounded priority inversion. The first `capacity`
> tasks are queued, but every later task in the intake is executed inline before
> those queued tasks. Worse, the thief wake occurs only after all overflow tasks
> have executed.

Correct on the ordering. `drain_intake` pushes what fits, runs the rest inline,
and calls `wake_one_sleeper` below the loop. An intake of ten thousand tasks
against a four thousand deque therefore runs roughly six thousand tasks on one
worker before any sleeper is told there is work to steal.

The wake belongs before the loop. That is two lines and it is the single
cheapest fix in this review.

The priority inversion is real too and is a consequence of the unbounded intake,
which is discussed under "standing choices" below.

### `AtomicU64` gives up targets this crate deliberately supports

> Default `fanout` breaks targets without 64-bit atomics. The existing queue
> code deliberately falls back to 32-bit atomics, but `fanout` directly imports
> `AtomicU64`. Because `fanout` is now a default feature, supported `no_std`
> targets lacking 64-bit atomics lose their default build.

Correct, and specific to this merge. `src/config.rs` carries explicit
`#[cfg(target_has_atomic = "64")]` fallbacks precisely so the queues work
without 64-bit atomics. The completion counter walked straight past them, and
because `fanout` is on by default, it takes the whole crate's default build with
it on those targets.

### Every submit takes the parking path even when the worker is awake

> Every submit enters the OS parking path even when the worker is running.
> `submit` performs a virtual `unpark`, a global `SeqCst` bitmap load, and
> potentially a second `unpark`. For `StdHost`, each unpark takes a
> `std::sync::Mutex` and notifies a condition variable.

Correct, and it is the sharpest finding here, because it is only *safe* to fix
as of the change this pull request makes.

`submit` used to say, in a comment, that waking conditionally was unsound: the
worker can drain its intake and park between one submit and the next, so
skipping the wake loses it. That was true when nothing tracked who was asleep.
It is no longer true. A worker now sets its sleeper bit **before** its last look
at the intake, so a submitter that finds the bit clear knows the worker will
look again before it sleeps. Reading the bit and skipping the unpark is
therefore sound, and it removes a mutex acquisition and a condvar notify from
every single submission.

The reviewer found an optimisation that the new handshake enables and the old
code correctly refused. That is the most useful thing in the review.

### The intake's capacity is thrown away on every drain

> Every intake drain discards its allocation. `mem::take` leaves a
> zero-capacity `Vec`; consuming `taken` then frees its buffer. The next
> submission reallocates while holding the intake spinlock.

Correct. Swapping a worker-local scratch `Vec` in rather than taking keeps the
capacity circulating.

One qualification on the severity: it is one allocate-free cycle per *drain*,
not per task, and it degenerates to per-task only when batches are size one.
The reviewer's phrasing, "at low volume, this can mean an allocation/free cycle
per task", is right but reads as the common case when it is the small-batch
case.

### The completion counter is a shared write on every task

> Telemetry doubles the task-completion RMW cost. `Local` records are densely
> packed, so these writes can also share cache lines with intake locks and
> adjacent workers.

Correct. `Vec<Local>` packs the records, so one worker's counter and another's
intake lock land on the same line and every completed task writes to it.
`crossbeam-utils` is already a dependency and `CachePadded` is the fix, with a
worker-local count published on demand if the accounting is kept at all.

### Four probes do not bound steal latency

> A worker makes only four random attempts, then parks for up to 100 ms. With
> 64 workers and one busy victim, four draws find that victim only about 6.1%
> of the time.

The arithmetic is right and the concern is real. A randomised start followed by
a bounded full sweep is the standard answer and costs nothing when work is
plentiful, because the first probe usually hits.

One qualification: probing is not the only discovery path any more. A submit
wakes a sleeper, a bulk drain wakes one, and a thief that takes more than one
wakes another, so the chain propagates without depending on a probe landing.
The 100 ms park is a backstop rather than the mechanism, which is the whole
point of the change under review. The finding stands, but it is a latency tail
rather than the systematic starvation the phrasing suggests.

### The exactly-once test does not establish exactly-once

> The "exactly once" test uses a single counter; one duplicated task plus one
> lost task still produces `n`, and the sampled value is captured before joining
> workers.

Correct on both counts. A per-task flag, checked for double-set and for
completeness after the workers are joined, is what that test needs to earn its
name. The Loom job also does not exercise this protocol, since `fanout`'s tests
require the `host` feature and Loom runs without it.

## Answered, and not changed

### Boxed tasks

> The task API forces allocator and indirect-call jitter.
> `Task = Box<dyn FnOnce() + Send>` normally means one allocation on submission,
> a vtable call, and deallocation on the worker. For fine-grained HFT tasks,
> this dominates the deque.

The mechanism is right, and it matches what was measured independently: this
pool runs 8 to 12% behind a hand-rolled scoped-thread fan-out over the same
deques, and the difference is an allocation plus two moves per task, not
anything in the stealing.

It is not a defect to fix in place. An allocation-free form is a different API,
either a generic `Pool<T>` over a concrete task type or an inline
function-and-context pair, and both give up the thing that makes this pool
usable: heterogeneous closures submitted from anywhere. Worth building if the
workload demands it, as an addition rather than a correction, and worth building
against a measurement of what it recovers rather than on the argument that
allocation is expensive.

### Publishing does not wait for Miri

> The version bump triggers irreversible publication, while the publish gate
> reruns only stable tests before publishing. It does not wait for Miri or
> re-run the complete gate in that job.

Half accepted. The gate now runs `cargo test --release --features host` rather
than the plain `cargo test` it ran before, which is the material half: it was
publishing without ever running the pool's own tests.

Waiting for Miri is declined deliberately. Miri here takes minutes, and it now
runs on `workflow_dispatch` rather than on every push, because a change to a
doc comment should not queue behind it. Running it belongs to changing the
unsafe code or the atomics, which for this crate means the queues, the transfer
path, or the wake protocol. A release gate that waits on a job nobody runs is a
gate that gets bypassed.

## Where the review is wrong

### The benchmark bias runs the other way

> It also compares a prestarted pool with scoped threads created inside the
> competing timed section. This cannot support HFT suitability.

The observation is correct and the inference from it is backwards. Timing the
competitor's thread creation and not this pool's startup biases the comparison
**toward** `fanout`. The honest gap is therefore wider than the example reports,
not narrower, which strengthens the conclusion rather than undermining it: this
pool is behind a hand-rolled fan-out over the same deques, and the harness is
being generous to it.

The rest of that finding stands. The example measures throughput and reports no
dispatch latency, no percentiles, no allocation counts and no context switches,
and it is not evidence of suitability for a latency-sensitive workload. What it
is evidence of is that the pool is in the same league as the alternatives on
throughput, which is all it was built to answer.

### Seven P1s

The severities are inflated by the method. A review that does not compile and
does not measure can identify a cost but not its share, and several of these are
costs on paths that measurement says are not the bottleneck. The ordering that
falls out of the evidence is: the two outright bugs first, then the atomics,
then the conditional wake, which is the one likely to move a number, then the
allocation and cache-line items.

## Standing choices, which keep being rediscovered

These are decisions rather than defects, and each has been questioned more than
once. They are recorded so the next reading does not have to re-derive them.

**No join, no completion signal.** Tasks are independent by construction. A
caller that needs to know when a batch finished counts for itself. This is what
lets a task run on any worker at any time with no bookkeeping between them.

**The caller brings the threads.** The pool does not spawn. `Pool::runner` hands
out a handle and the caller gives it a thread. That is what keeps the core free
of an operating system, and it is why the `Host` trait is three methods.

**The intake is unbounded and there is no backpressure.** `capacity` bounds what
a worker holds, not what a caller may hand it. A submitter is never blocked and
never refused, and if tasks arrive faster than they run, memory is the limit.
This is documented on `Pool::new` rather than hidden, and the reviewer is right
that it interacts badly with inline overflow execution. A bounded `try_submit`
is the addition that would fix both, and it has not been written.

**`unpark` must leave a permit.** The pool publishes work and then unparks, so a
`Host` whose wakeup is lost when the target has not parked yet will sleep a
worker with a task in its intake. A spin satisfies this for free; a bare
condition variable does not, which is why `StdHost` carries a flag beside it.

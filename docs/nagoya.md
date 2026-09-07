# Nagoya: what would sit on top of these queues, and what it is not

A design note and a name, written 8 September 2026 so the argument survives the
conversation that produced it. Nothing here is built.

## Where this crate sits

`ps-st3` is a fork of `st3`, and its README is precise about what the two queues
target:

> The FIFO queue is based on the Tokio queue, but provides a somewhat more
> convenient and more flexible API. The LIFO queue is a novel design [...] a
> faster, fixed-size alternative to the Chase-Lev deque.

So the halves point at different rivals: `fifo` at Tokio's internal queue,
`lifo` at the Chase-Lev deque that `crossbeam-deque` implements and `rayon-core`
runs on.

**Only one of those is substitutable.** Tokio's queue is private - nothing can
depend on `tokio::runtime::scheduler::multi_thread::queue` - so nobody migrates
to this crate *from* Tokio. `crossbeam-deque` is a public crate that rayon
depends on, so it is the one thing here that could actually displace something.
The customer is a library author building a scheduler, not an application
choosing a runtime.

`fanout` is in neither camp. No futures and no I/O driver, so it is not Tokio.
No `join`, `scope` or `par_iter`, so it is not rayon. It is the layer both of
them contain.

## What would make it feel like a runtime

`Host` is already the whole operating-system seam: park, unpark, a clock. That
is the useful fact, because it means the first four of these need nothing more
than what is already abstracted, and the `no_std` core survives all of them.

| | adds | cost |
|---|---|---|
| 1 | **`JoinHandle`** - `spawn` returns a handle, the task writes its result to a slot and unparks the waiter | one allocation and one synchronisation point per task |
| 2 | **`block_on`** - drives one future on the calling thread, waker is `Host::unpark` | small, and independent of the pool |
| 3 | **futures and `Waker`** - a task becomes a `Future`, waking re-submits it to a worker | the change that makes this a runtime rather than a pool |
| 4 | **timers** - a wheel over `Host::clock`, giving `sleep` and `timeout` | self-contained |
| 5 | **an I/O driver** - epoll, kqueue, io_uring | the actual moat, needs an operating system, **ends `no_std`** |

1 and 2 buy most of the felt difference for a fraction of the work. 3 is the
fork in the road. **Stopping after 4 is a coherent product rather than a
half-built Tokio: an async runtime that needs no operating system.** Tokio
cannot be that. "Tokio but ours" is not a position; that is.

## The tension, named

`fanout` says of itself:

> Tasks are independent by construction: no join, no continuation, no completion
> signal.

That is not a limitation, it is why it is cheap. Every layer above adds per-task
state and spends some of it. So they belong beside `submit`, not instead of it,
or the resemblance is bought with the advantage.

## The name

**Nagoya**, when it graduates. Not before: claiming a registry name for an
unpublished idea is the thing this note objects to on Tokio's behalf below.

A runtime with `JoinHandle`, `block_on`, futures and timers does not belong
under a name that says "these are st3's queues". It would be its own crate
depending on this one, and that crate is what is called `nagoya`. While it stays
a module it stays `fanout`, and the name is premature.

Rejected, with reasons:

- **`tokyo`** - one letter from `tokio`. It reads as a typosquat or a joke at
  the expense of the project whose queue design this crate builds on, and it
  mislabels the thing: naming it after the product it deliberately is not,
  exactly when the interesting claim is "no operating system required".
- **`osaka`** - taken, and taken by an abandoned **async runtime** (0.3.0,
  25,650 downloads, last touched 2019, "async for rust without the noise").
  A new runtime that users must disambiguate from a dead runtime of the same
  name is the one collision worth avoiding.
- **`kyoto`** - taken by a dormant cache crate (0.0.1, 1,690 downloads, 2021).
  Only a nominal collision, but unavailable.

`nagoya` and `sapporo` were both free on 8 September 2026. `nagoya` because
`sapporo` reads as the beer first.

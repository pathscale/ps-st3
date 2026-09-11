# 0.6.2 (2026-09-12)

### Added

- `Tuning::almost_tokio()` names the three-poll LIFO/FIFO-sharing policy;
  `Tuning::parking()` adds immediate host parking after an unsuccessful search.
  Both are opt-in and available without Rust std.

- `atomic-host` exposes the blocking OS parker as `AtomicHost` without Rust
  `std`; callers supply a monotonic clock and worker lifecycle. `StdHost`
  delegates to the same permit protocol.
- `Tuning::with_stealable_inbox` exposes displaced local jobs to idle peers
  through a FIFO inbox. The warm LIFO slot becomes stealable at its fairness
  quota, including a lone self-waking task. Existing presets keep their
  previous displacement policy.


### Changed

- Locality and its derived presets use four empty search rounds with 128
  spin hints per round before host parking. The former 64 by 1024 budget
  burned several CPU cores between sparse bursts.

### Fixed

- The final check after announcing sleep now includes peer queues, closing
  the interval between a worker's last search and its sleep announcement.
  Sharing uses a publication fence before checking for sleeping peers.

- After reaching its LIFO fairness quota, a worker still probes other queues
  first, then resumes ready local work without an idle backoff. Previously
  self-waking tasks paid an unnecessary delay at every quota boundary.
- A deterministic regression test checks that a continuously ready local chain
  performs zero idle backoffs; displaced-work fairness remains covered.

# 0.6.0 (2026-09-10)

### Added

- `Tuning`, the `fanout` pool's idle and wake policy, with three presets:
  `Tuning::locality` (the default), `Tuning::spread` and `Tuning::throughput`.
  Each carries the measurement that produced it in its doc comment, so a caller
  chooses on evidence rather than on the name.
- `Tuning::with_rounds_before_park`, `with_backoff_spins`, `with_injector_batch`,
  `with_local_wakes` and `with_promote_every`, taking `self` by value so they
  chain into `Pool::with_tuning`.
- `Pool::submit_local`, which keeps a woken task on the worker that woke it.

### Changed

- `Pool` no longer wakes a sleeping worker when none is sleeping. The sleep path
  is a Dekker pair, so publishing to a worker and then reading whether it sleeps
  needs a full barrier on both sides; a `SeqCst` fence in `submit_local` closes
  a lost wakeup that the spin-mutex release did not.
- `INJECTOR_BATCH` is 1 rather than 8. Measured: a pure-read workload at sixteen
  threads went from 8.9M to 15.2M operations per second.

### Breaking

- `Tuning` is `#[non_exhaustive]`, so it can gain a field without a major bump
  next time. **This is why the bump is minor rather than patch**: a downstream
  struct literal, including one using `..Default::default()`, no longer
  compiles. Build one from `Tuning::locality()`, `spread()` or `throughput()`
  and the `with_*` methods instead. Fields stay public to read, because callers
  legitimately derive values from the defaults.

# 0.5.0 (2026-09-06)

### Added

- `fanout`, a work-stealing pool built on these queues: a fixed set of workers,
  each owning a `lifo::Worker` taken by value, stealing from each other, with the
  operating system behind a three-method `Host` trait so the pool stays `no_std`.
  On by default. `default-features = false` gets the queues alone.
- `host`, off by default, a `std`-backed `Host`.

### Note for existing users

A minor bump rather than a patch, because the default feature set now pulls in
`spin`. Nothing in `fifo` or `lifo` changed, so a consumer that wants only the
queues can stay where it is or take this with `default-features = false`.

# 0.4.1 (2022-12-07)

- Make it possible to obtain a reference to a stealer from a worker with
  `stealer_ref` ([#5]).
- Implement `Eq` and `PartialEq` on stealers ([#5]).
- Replace the soon-to-be-deprecated `cache-padded` crate with `crossbeam-utils` ([#4])

[#4]: https://github.com/asynchronics/st3/pull/4
[#5]: https://github.com/asynchronics/st3/pull/5

# 0.4.0 (2022-11-15)

- Revert the fix added in 0.3.1 as the code was actually correct and the fix was
  unnecessary (comments added).
- Add a FIFO queue variant in the `fifo` module with the same API, based on the
  Tokio queue.

## :warning: Breaking changes

- Make the queue capacity a run-time parameter,
- Move the LIFO queue to a `lifo` module.
- Choose buffer indexing integer size based on `target_has_atomic` and remove
  the `long_counter` feature.
- Bump the MSRV to 1.60 to enable the use of `target_has_atomic`.

# 0.3.1 (2022-09-20)

- Fix bug that could result in an underflow when estimating the capacity of a
  stealer.

# 0.3.0 (2022-09-05)

- Implement FusedIterator for the Drain iterator.
- Add a `Worker::spare_capacity` method.
- BREAKING CHANGE: remove `Worker::len` as it was not very useful due to its
  weak guaranties.
- BREAKING CHANGE: move the `drain` method to the worker rather than the
  stealer, as it is mostly useful to move items back into an injection queue on
  overflow.

# 0.2.0 (2022-07-20)

- Add a drain iterator to efficiently steal batches of items into custom
  containers.

# 0.1.1 (2022-02-11)

- Mitigate other potential ABA problems by using 32-bit positions throughout.
- Update documentation.
- Inline MIT license in `tokio_queue.rs` and explicitly state license exceptions
  for tests/benchmark in README.
- Update copyright date in MIT license.

# 0.1.0 (2022-02-10)

Initial release

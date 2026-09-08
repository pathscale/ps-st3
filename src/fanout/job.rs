//! What the pool's queues actually hold.
//!
//! # Why not `Box<dyn FnOnce()>`
//!
//! A boxed trait object is a *fat* pointer, so a queue of them is a queue of
//! two words that must be moved and dropped as a unit, and every submitter has
//! to allocate one even when it already has the work sitting on the heap.
//!
//! A [`Job`] is the same two words with the indirection removed: a thin pointer
//! to whatever the work is, and a monomorphised function that knows how to run
//! *that* thing. It is plain data, so a queue of jobs is a queue of `usize`
//! pairs, and a caller that already owns a heap allocation, such as a task
//! inside an `Arc`, can hand it over without allocating anything at all.
//!
//! This is the representation `rayon` calls `JobRef`, `chili` calls
//! `JobShared`, and `forte` also calls `JobRef`. It is not novel and it is not
//! ours; it is the shape that lets a work-stealing queue hold work by value.
//!
//! # The safety contract in one line
//!
//! **A `Job` is a pointer whose type has been forgotten, plus the one function
//! that still remembers it.** Everything unsafe here is that sentence: pair the
//! wrong function with the wrong pointer and it is a type confusion.

use alloc::boxed::Box;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;

/// What to do with a job's allocation.
///
/// One callback rather than two, so a [`Job`] stays two words. A queue of jobs
/// is a queue of these, and a third word would cost a third of the queue's
/// cache footprint to save a branch that predicts perfectly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Act {
    /// Run the work, then release it.
    Run,
    /// Release the work **without running it**, because the job is being
    /// dropped rather than executed.
    Drop,
}

/// A unit of work, type-erased into a pointer and the function that owns it.
///
/// Running a job consumes it. There is no `Copy` and no `Clone` on purpose:
/// two jobs naming one allocation would run it twice, and the second run is a
/// use-after-free.
///
/// # Dropping one without running it
///
/// **A `Job` owns its work, and dropping one releases it.** That is why the
/// callback takes an [`Act`] rather than being a bare `execute`: a queue that
/// is dropped with work still in it, a pool shut down with jobs pending, or a
/// worker unwinding with a private queue, all have to release those closures
/// and whatever they captured. `Box<dyn FnOnce()>`, which this replaced, did
/// that for free; getting it wrong here would leak buffers and reference counts
/// for the life of a process.
pub struct Job {
    pointer: NonNull<()>,
    handle: unsafe fn(NonNull<()>, Act),
}

// SAFETY: a `Job` is only ever built by `Job::from_boxed` or by
// `Job::from_raw`, whose contracts require the pointed-to work to be `Send`.
// The pointer itself is an owning handle that is moved, never shared, so
// sending one to another thread sends the work with it.
unsafe impl Send for Job {}

impl Job {
    /// Take ownership of `work` on the heap and turn it into a job.
    ///
    /// One allocation, and the function that runs it is monomorphised for `F`,
    /// so there is no vtable and no second indirection at run time.
    pub fn from_boxed<F>(work: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        unsafe fn handle<F: FnOnce() + Send + 'static>(pointer: NonNull<()>, act: Act) {
            // SAFETY: the only way this function reaches a pointer is through
            // the `Job` built beside it below, whose pointer came from
            // `Box::into_raw` on a `Box<F>` for this same `F`, and it is
            // reached at most once because both running and dropping consume
            // the job. The body of an `unsafe fn` is already an unsafe block on
            // this edition.
            let work = Box::from_raw(pointer.as_ptr().cast::<F>());
            match act {
                // The box goes out of scope either way; this is the only
                // difference, and it is what stops a dropped job running.
                Act::Run => work(),
                Act::Drop => drop(work),
            }
        }

        let pointer = Box::into_raw(Box::new(work)).cast::<()>();
        Self {
            // SAFETY: `Box::into_raw` never returns null.
            pointer: unsafe { NonNull::new_unchecked(pointer) },
            handle: handle::<F>,
        }
    }

    /// Build a job from a pointer this crate did not allocate.
    ///
    /// This is the zero-allocation path, for a caller whose work already lives
    /// on the heap: a task inside an `Arc`, a slot in an arena, a future
    /// already boxed by something else.
    ///
    /// # Safety
    ///
    /// The caller must ensure all of:
    ///
    /// * `handle` is correct for whatever `pointer` points at, and both
    ///   [`Act::Run`] and [`Act::Drop`] on that pointer are sound. **A `Job`
    ///   owns its work**: `Act::Drop` has to release the allocation without
    ///   running it, or a queue dropped with work in it leaks.
    /// * `pointer` stays valid until the job runs. The pool gives no bound on
    ///   when that is, and a job may outlive the thread that submitted it.
    /// * The work behind `pointer` is `Send`, because the job will very likely
    ///   run on a different thread.
    /// * `handle` does not unwind. A panic crossing a worker's run loop takes
    ///   the worker's deque with it.
    /// * This job is the only one naming `pointer`, since running consumes the
    ///   pointer's ownership.
    // Not `const`: a function pointer in a `const fn` is 1.61, and this crate
    // holds a 1.60 floor.
    #[must_use]
    pub unsafe fn from_raw(pointer: NonNull<()>, handle: unsafe fn(NonNull<()>, Act)) -> Self {
        Self { pointer, handle }
    }

    /// Run the work, consuming the job.
    pub(crate) fn run(self) {
        // `Drop` would otherwise release the work a second time after running
        // it. `ManuallyDrop` is what makes running and dropping exclusive.
        let job = ManuallyDrop::new(self);
        // SAFETY: `from_boxed` pairs a `Box<F>` pointer with `handle::<F>`, and
        // `from_raw` makes the pairing the caller's obligation. The handle is
        // reached once: `ManuallyDrop` above stops the destructor, and taking
        // `self` by value stops a second call.
        unsafe { (job.handle)(job.pointer, Act::Run) }
    }
}

/// Releases the work without running it.
///
/// The case that matters is a queue dropped with jobs still in it: a pool shut
/// down with work pending, or a worker unwinding while holding a private queue.
/// Those closures own whatever they captured, and nothing else will release it.
impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: as in `run`, and this is the other of the two exclusive
        // paths: `run` consumes the job through `ManuallyDrop`, so a job that
        // reaches here has not been run and will not be.
        unsafe { (self.handle)(self.pointer, Act::Drop) }
    }
}

impl core::fmt::Debug for Job {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Job")
            .field("pointer", &self.pointer)
            .finish_non_exhaustive()
    }
}

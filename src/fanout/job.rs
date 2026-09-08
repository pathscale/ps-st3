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
use core::ptr::NonNull;

/// A unit of work, type-erased into a pointer and the function that runs it.
///
/// Running a job consumes it. There is no `Copy` and no `Clone` on purpose:
/// two jobs naming one allocation would run it twice, and the second run is a
/// use-after-free.
pub struct Job {
    pointer: NonNull<()>,
    execute: unsafe fn(NonNull<()>),
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
        unsafe fn execute<F: FnOnce() + Send + 'static>(pointer: NonNull<()>) {
            // SAFETY: the only way this function reaches a pointer is through
            // the `Job` built beside it below, whose pointer came from
            // `Box::into_raw` on a `Box<F>` for this same `F` and has not been
            // run before, because running consumes the job.
            let work = unsafe { Box::from_raw(pointer.as_ptr().cast::<F>()) };
            work();
        }

        let pointer = Box::into_raw(Box::new(work)).cast::<()>();
        Self {
            // SAFETY: `Box::into_raw` never returns null.
            pointer: unsafe { NonNull::new_unchecked(pointer) },
            execute: execute::<F>,
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
    /// * `execute` is correct for whatever `pointer` points at, and running it
    ///   on that pointer is sound.
    /// * `pointer` stays valid until the job runs. The pool gives no bound on
    ///   when that is, and a job may outlive the thread that submitted it.
    /// * The work behind `pointer` is `Send`, because the job will very likely
    ///   run on a different thread.
    /// * `execute` does not unwind. A panic crossing a worker's run loop takes
    ///   the worker's deque with it.
    /// * This job is the only one naming `pointer`, since running consumes the
    ///   pointer's ownership.
    #[must_use]
    pub const unsafe fn from_raw(pointer: NonNull<()>, execute: unsafe fn(NonNull<()>)) -> Self {
        Self { pointer, execute }
    }

    /// Run the work, consuming the job.
    pub(crate) fn run(self) {
        // SAFETY: `from_boxed` pairs a `Box<F>` pointer with `execute::<F>`,
        // and `from_raw` makes the pairing the caller's obligation. Consuming
        // `self` is what stops a second run.
        unsafe { (self.execute)(self.pointer) }
    }
}

impl core::fmt::Debug for Job {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Job")
            .field("pointer", &self.pointer)
            .finish_non_exhaustive()
    }
}

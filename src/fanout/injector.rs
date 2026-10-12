//! Unbounded MPMC injector for the fanout pool.
//!
//! The algorithm is the block-linked queue of crossbeam-queue's `SegQueue`
//! (MIT OR Apache-2.0, same as this crate): blocks of `BLOCK_CAP` slots, each
//! slot with a `WRITE`/`READ`/`DESTROY` state word, head and tail tickets that
//! carry a block offset, and cooperative block reclamation by the last reader.
//! A thread only touches a block after owning a ticket inside it, so no block
//! is freed under a reader. Contended ticket retries back off exponentially
//! (1 to 64 spin hints) instead of hammering the index line.

use alloc::alloc::{alloc_zeroed, handle_alloc_error, Layout};
use alloc::boxed::Box;
use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{self, AtomicPtr, AtomicUsize, Ordering};

use crate::cache_padded::CachePadded;

const WRITE: usize = 1;
const READ: usize = 2;
const DESTROY: usize = 4;

const LAP: usize = 32;
const BLOCK_CAP: usize = LAP - 1;
const SHIFT: usize = 1;
const HAS_NEXT: usize = 1;

struct Backoff(u32);

impl Backoff {
    #[inline]
    fn new() -> Self {
        Backoff(0)
    }

    #[inline]
    fn spin(&mut self) {
        for _ in 0..1u32 << self.0.min(6) {
            core::hint::spin_loop();
        }
        if self.0 <= 6 {
            self.0 += 1;
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
}

impl<T> Slot<T> {
    #[inline]
    fn wait_write(&self) {
        let mut backoff = Backoff::new();
        while self.state.load(Ordering::Acquire) & WRITE == 0 {
            backoff.spin();
        }
    }
}

struct Block<T> {
    next: AtomicPtr<Block<T>>,
    slots: [Slot<T>; BLOCK_CAP],
}

impl<T> Block<T> {
    fn new() -> Box<Self> {
        let layout = Layout::new::<Self>();
        // SAFETY: the layout has a non-zero size (it holds an `AtomicPtr`). An all-zero
        // `Block` is valid: a null `next`, zero slot states, and uninit slot values.
        unsafe {
            let raw = alloc_zeroed(layout);
            if raw.is_null() {
                handle_alloc_error(layout);
            }
            Box::from_raw(raw.cast())
        }
    }

    fn wait_next(&self) -> *mut Block<T> {
        let mut backoff = Backoff::new();
        loop {
            let next = self.next.load(Ordering::Acquire);
            if !next.is_null() {
                return next;
            }
            backoff.spin();
        }
    }

    /// Mark slots from `start` as `DESTROY`; free the block once no reader holds a slot.
    unsafe fn destroy(this: *mut Block<T>, start: usize) {
        for i in start..BLOCK_CAP - 1 {
            let slot = (*this).slots.get_unchecked(i);
            if slot.state.load(Ordering::Acquire) & READ == 0
                && slot.state.fetch_or(DESTROY, Ordering::AcqRel) & READ == 0
            {
                return;
            }
        }
        drop(Box::from_raw(this));
    }
}

struct Position<T> {
    index: AtomicUsize,
    block: AtomicPtr<Block<T>>,
}

pub(super) struct Injector<T> {
    head: CachePadded<Position<T>>,
    tail: CachePadded<Position<T>>,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for Injector<T> {}
unsafe impl<T: Send> Sync for Injector<T> {}

impl<T> Injector<T> {
    pub(super) fn new() -> Self {
        Injector {
            head: CachePadded::new(Position {
                block: AtomicPtr::new(ptr::null_mut()),
                index: AtomicUsize::new(0),
            }),
            tail: CachePadded::new(Position {
                block: AtomicPtr::new(ptr::null_mut()),
                index: AtomicUsize::new(0),
            }),
            _marker: PhantomData,
        }
    }

    pub(super) fn push(&self, value: T) {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.index.load(Ordering::Acquire);
        let mut block = self.tail.block.load(Ordering::Acquire);
        let mut next_block = None;

        loop {
            let offset = (tail >> SHIFT) % LAP;

            // A producer is installing the next block; wait for it.
            if offset == BLOCK_CAP {
                backoff.spin();
                tail = self.tail.index.load(Ordering::Acquire);
                block = self.tail.block.load(Ordering::Acquire);
                continue;
            }

            // Allocate the next block before taking the last ticket, so the
            // window in which others wait on the install stays short.
            if offset + 1 == BLOCK_CAP && next_block.is_none() {
                next_block = Some(Block::<T>::new());
            }

            if block.is_null() {
                let new = Box::into_raw(Block::<T>::new());
                if self
                    .tail
                    .block
                    .compare_exchange(block, new, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    self.head.block.store(new, Ordering::Release);
                    block = new;
                } else {
                    next_block = unsafe { Some(Box::from_raw(new)) };
                    tail = self.tail.index.load(Ordering::Acquire);
                    block = self.tail.block.load(Ordering::Acquire);
                    continue;
                }
            }

            let new_tail = tail + (1 << SHIFT);
            match self.tail.index.compare_exchange_weak(
                tail,
                new_tail,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    if offset + 1 == BLOCK_CAP {
                        let next_block = Box::into_raw(next_block.take().unwrap());
                        let next_index = new_tail.wrapping_add(1 << SHIFT);
                        self.tail.block.store(next_block, Ordering::Release);
                        self.tail.index.store(next_index, Ordering::Release);
                        (*block).next.store(next_block, Ordering::Release);
                    }
                    let slot = (*block).slots.get_unchecked(offset);
                    slot.value.get().write(MaybeUninit::new(value));
                    slot.state.fetch_or(WRITE, Ordering::Release);
                    return;
                },
                Err(t) => {
                    tail = t;
                    block = self.tail.block.load(Ordering::Acquire);
                    backoff.spin();
                }
            }
        }
    }

    pub(super) fn pop(&self) -> Option<T> {
        let mut backoff = Backoff::new();
        let mut head = self.head.index.load(Ordering::Acquire);
        let mut block = self.head.block.load(Ordering::Acquire);

        loop {
            let offset = (head >> SHIFT) % LAP;

            if offset == BLOCK_CAP {
                backoff.spin();
                head = self.head.index.load(Ordering::Acquire);
                block = self.head.block.load(Ordering::Acquire);
                continue;
            }

            let mut new_head = head + (1 << SHIFT);

            if new_head & HAS_NEXT == 0 {
                atomic::fence(Ordering::SeqCst);
                let tail = self.tail.index.load(Ordering::Relaxed);
                if head >> SHIFT == tail >> SHIFT {
                    return None;
                }
                if (head >> SHIFT) / LAP != (tail >> SHIFT) / LAP {
                    new_head |= HAS_NEXT;
                }
            }

            // Only while the very first push is still installing the first block.
            if block.is_null() {
                backoff.spin();
                head = self.head.index.load(Ordering::Acquire);
                block = self.head.block.load(Ordering::Acquire);
                continue;
            }

            match self.head.index.compare_exchange_weak(
                head,
                new_head,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    if offset + 1 == BLOCK_CAP {
                        let next = (*block).wait_next();
                        let mut next_index = (new_head & !HAS_NEXT).wrapping_add(1 << SHIFT);
                        if !(*next).next.load(Ordering::Relaxed).is_null() {
                            next_index |= HAS_NEXT;
                        }
                        self.head.block.store(next, Ordering::Release);
                        self.head.index.store(next_index, Ordering::Release);
                    }

                    let slot = (*block).slots.get_unchecked(offset);
                    slot.wait_write();
                    let value = slot.value.get().read().assume_init();

                    if offset + 1 == BLOCK_CAP {
                        Block::destroy(block, 0);
                    } else if slot.state.fetch_or(READ, Ordering::AcqRel) & DESTROY != 0 {
                        Block::destroy(block, offset + 1);
                    }
                    return Some(value);
                },
                Err(h) => {
                    head = h;
                    block = self.head.block.load(Ordering::Acquire);
                    backoff.spin();
                }
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        let head = self.head.index.load(Ordering::SeqCst);
        let tail = self.tail.index.load(Ordering::SeqCst);
        head >> SHIFT == tail >> SHIFT
    }

    pub(super) fn len(&self) -> usize {
        loop {
            let mut tail = self.tail.index.load(Ordering::SeqCst);
            let mut head = self.head.index.load(Ordering::SeqCst);
            if self.tail.index.load(Ordering::SeqCst) == tail {
                tail &= !((1 << SHIFT) - 1);
                head &= !((1 << SHIFT) - 1);
                // An index parked on the block boundary counts as the next block's start.
                if (tail >> SHIFT) & (LAP - 1) == LAP - 1 {
                    tail = tail.wrapping_add(1 << SHIFT);
                }
                if (head >> SHIFT) & (LAP - 1) == LAP - 1 {
                    head = head.wrapping_add(1 << SHIFT);
                }
                let lap = (head >> SHIFT) / LAP;
                tail = tail.wrapping_sub((lap * LAP) << SHIFT);
                head = head.wrapping_sub((lap * LAP) << SHIFT);
                tail >>= SHIFT;
                head >>= SHIFT;
                // Subtract the boundary slots between head and tail.
                return tail - head - tail / LAP;
            }
        }
    }
}

impl<T> Drop for Injector<T> {
    fn drop(&mut self) {
        let mut head = *self.head.index.get_mut();
        let mut tail = *self.tail.index.get_mut();
        let mut block = *self.head.block.get_mut();
        head &= !((1 << SHIFT) - 1);
        tail &= !((1 << SHIFT) - 1);
        unsafe {
            while head != tail {
                let offset = (head >> SHIFT) % LAP;
                if offset < BLOCK_CAP {
                    let slot = (*block).slots.get_unchecked(offset);
                    (*slot.value.get()).assume_init_drop();
                } else {
                    let next = *(*block).next.get_mut();
                    drop(Box::from_raw(block));
                    block = next;
                }
                head = head.wrapping_add(1 << SHIFT);
            }
            if !block.is_null() {
                drop(Box::from_raw(block));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Injector;
    use alloc::sync::Arc;

    #[test]
    fn fifo_len_and_drop() {
        let q = Injector::new();
        assert!(q.is_empty());
        for i in 0..1000 {
            q.push(alloc::boxed::Box::new(i));
        }
        assert_eq!(q.len(), 1000);
        for i in 0..500 {
            assert_eq!(*q.pop().unwrap(), i);
        }
        assert_eq!(q.len(), 500);
        drop(q);
        let q: Injector<Arc<()>> = Injector::new();
        let a = Arc::new(());
        for _ in 0..100 {
            q.push(a.clone());
        }
        drop(q);
        assert_eq!(Arc::strong_count(&a), 1);
    }
}

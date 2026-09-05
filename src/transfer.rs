//! Bulk transfer of stolen items between ring buffers.

use core::mem::MaybeUninit;

use crate::config::UnsignedShort;
use crate::loom_exports::cell::UnsafeCell;

#[cfg(not(all(test, st3_loom)))]
use core::ptr;

#[cfg(not(all(test, st3_loom)))]
use crate::loom_exports::debug_or_loom_assert;

/// Maximum number of items moved by the inline scalar path of
/// [`transfer_items`] before falling back to a bulk copy.
#[cfg(not(all(test, st3_loom)))]
const SMALL_CHUNK: usize = 8;

/// Bitwise-move `count` items from position `src_pos` of a ring buffer to
/// position `dst_pos` of another ring buffer.
///
/// Splitting both ring ranges at the union of their physical wrap points
/// yields at most 3 chunks that are contiguous on both sides at once, so the
/// whole transfer lowers to at most 3 `ptr::copy_nonoverlapping` calls (SIMD
/// bulk copies). Chunks of at most [`SMALL_CHUNK`] items keep an inline
/// scalar loop since the fixed cost of a call would otherwise dominate.
///
/// The items are *relocated*, not copied: the source slots are logically
/// uninitialized afterwards, exactly as with the per-element
/// `read_at`/`write_at` pair this helper replaces.
///
/// # Safety
///
/// - `count` must not exceed the number of live items at `src_pos` in the
///   source queue nor the spare capacity at `dst_pos` in the destination
///   queue. When both buffers are the same (self-stealing), this in
///   particular guarantees that the source and destination physical ranges
///   are disjoint.
/// - The moved items must not be read or dropped again from the source
///   queue.
#[cfg(not(all(test, st3_loom)))]
pub(crate) unsafe fn transfer_items<T>(
    src_buffer: &[UnsafeCell<MaybeUninit<T>>],
    src_mask: UnsignedShort,
    src_pos: UnsignedShort,
    dst_buffer: &[UnsafeCell<MaybeUninit<T>>],
    dst_mask: UnsignedShort,
    dst_pos: UnsignedShort,
    count: UnsignedShort,
) {
    let cap = src_mask as usize + 1;
    let dcap = dst_mask as usize + 1;
    let src_idx = (src_pos & src_mask) as usize;
    let dst_idx = (dst_pos & dst_mask) as usize;
    let n = count as usize;

    debug_or_loom_assert!(cap == src_buffer.len() && dcap == dst_buffer.len());
    debug_or_loom_assert!(cap.is_power_of_two() && dcap.is_power_of_two());

    let src_base = src_buffer.as_ptr().cast::<T>();
    let dst_base = dst_buffer.as_ptr().cast::<T>() as *mut T;

    // Fast path for transfers of at most one item (the most common steal
    // size): no chunk computation needed.
    if n <= 1 {
        if n == 1 {
            dst_base.add(dst_idx).write(src_base.add(src_idx).read());
        }
        return;
    }

    // Run length before the wrap point of each range; the sorted pair
    // (lo, hi) is the union of both wrap points and defines up to 3 chunks
    // that are contiguous on both sides at once.
    let s = (cap - src_idx).min(n);
    let d = (dcap - dst_idx).min(n);
    let (lo, hi) = if s <= d { (s, d) } else { (d, s) };

    // When self-stealing, check that the source and destination chunks are
    // physically disjoint, as required by `copy_nonoverlapping`.
    debug_or_loom_assert!({
        if !ptr::eq(src_buffer.as_ptr(), dst_buffer.as_ptr()) {
            true
        } else {
            let chunks = [(0, lo), (lo, hi), (hi, n)];
            let mut disjoint = true;
            for &(a_start, a_end) in &chunks {
                let a0 = (src_idx + a_start) & (cap - 1);
                let al = a_end - a_start;
                for &(b_start, b_end) in &chunks {
                    let b0 = (dst_idx + b_start) & (dcap - 1);
                    let bl = b_end - b_start;
                    disjoint &= a0 + al <= b0 || b0 + bl <= a0;
                }
            }
            disjoint
        }
    });

    // Capacities are powers of two, so a single masked add per chunk yields
    // the wrapped base index (each range wraps at most once).
    for (start, end) in [(0, lo), (lo, hi), (hi, n)] {
        let len = end - start;
        if len == 0 {
            continue;
        }
        let src_chunk = src_base.add((src_idx + start) & (cap - 1));
        let dst_chunk = dst_base.add((dst_idx + start) & (dcap - 1));
        if len <= SMALL_CHUNK {
            for i in 0..len {
                dst_chunk.add(i).write(src_chunk.add(i).read());
            }
        } else {
            ptr::copy_nonoverlapping(src_chunk, dst_chunk, len);
        }
    }
}

/// The per-element reference implementation.
///
/// Under loom this *is* `transfer_items`: loom's `UnsafeCell` exposes no raw pointers, so
/// the bulk path cannot be expressed there. That leaves two implementations of one contract
/// with nothing checking they agree, which is why this is also compiled under `test` and
/// held against the bulk version by `bulk_matches_scalar` below. A divergence between them
/// would otherwise be invisible: loom would keep passing on the version that does not ship.
#[cfg(any(test, st3_loom))]
#[allow(dead_code)]
pub(crate) unsafe fn transfer_items_scalar<T>(
    src_buffer: &[UnsafeCell<MaybeUninit<T>>],
    src_mask: UnsignedShort,
    src_pos: UnsignedShort,
    dst_buffer: &[UnsafeCell<MaybeUninit<T>>],
    dst_mask: UnsignedShort,
    dst_pos: UnsignedShort,
    count: UnsignedShort,
) {
    for offset in 0..count {
        let item = src_buffer[(src_pos.wrapping_add(offset) & src_mask) as usize]
            .with(|slot| slot.read().assume_init());
        dst_buffer[(dst_pos.wrapping_add(offset) & dst_mask) as usize]
            .with_mut(|slot| slot.write(MaybeUninit::new(item)));
    }
}

#[cfg(all(test, st3_loom))]
pub(crate) use self::transfer_items_scalar as transfer_items;

#[cfg(all(test, not(st3_loom)))]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    fn buffer(n: usize) -> Vec<UnsafeCell<MaybeUninit<u32>>> {
        (0..n)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect()
    }

    /// The bulk transfer must move exactly what the per-element loop moves, for every
    /// combination of ring sizes, start positions and counts that the booking step can
    /// produce - including every wrap point, which is where a segmented copy can go wrong
    /// and a scalar loop cannot.
    #[test]
    fn bulk_matches_scalar() {
        for &src_cap in &[2usize, 4, 8, 16, 32] {
            for &dst_cap in &[2usize, 4, 8, 16, 32] {
                let (src_mask, dst_mask) =
                    (src_cap as UnsignedShort - 1, dst_cap as UnsignedShort - 1);
                for src_pos in 0..(src_cap as UnsignedShort * 2) {
                    for dst_pos in 0..(dst_cap as UnsignedShort * 2) {
                        for count in 0..=(src_cap.min(dst_cap) as UnsignedShort) {
                            let src = buffer(src_cap);
                            for i in 0..src_cap {
                                src[i].with_mut(|s| unsafe {
                                    s.write(MaybeUninit::new(0xC0DE_0000u32 + i as u32))
                                });
                            }
                            let bulk = buffer(dst_cap);
                            let scalar = buffer(dst_cap);
                            unsafe {
                                transfer_items(
                                    &src, src_mask, src_pos, &bulk, dst_mask, dst_pos, count,
                                );
                                transfer_items_scalar(
                                    &src, src_mask, src_pos, &scalar, dst_mask, dst_pos, count,
                                );
                            }
                            for offset in 0..count {
                                let idx = (dst_pos.wrapping_add(offset) & dst_mask) as usize;
                                let a = bulk[idx].with(|s| unsafe { s.read().assume_init() });
                                let b = scalar[idx].with(|s| unsafe { s.read().assume_init() });
                                assert_eq!(
                                    a, b,
                                    "bulk and scalar disagree: src_cap={src_cap} dst_cap={dst_cap} \
                                     src_pos={src_pos} dst_pos={dst_pos} count={count} offset={offset}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

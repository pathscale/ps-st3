use core::fmt;
use core::ops::{Deref, DerefMut};

/// Aligns a value so adjacent values do not share a cache line.
///
/// x86-64 uses 128 bytes because its spatial prefetcher can fetch adjacent
/// 64-byte lines together. Apple aarch64 uses 128 bytes for the largest cores;
/// other targets use 64 bytes.
#[cfg_attr(
    any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_vendor = "apple")
    ),
    repr(align(128))
)]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_vendor = "apple")
    )),
    repr(align(64))
)]
#[repr(C)]
pub(crate) struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    /// Wraps a value with cache-line alignment.
    pub(crate) const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}

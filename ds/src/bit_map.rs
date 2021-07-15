use std::sync::atomic::{AtomicU32, Ordering};
pub struct BitMap {
    blocks: Vec<AtomicU32>,
}

pub(crate) const BLK_SIZE: usize = std::mem::size_of::<u32>() * 8;
const BLK_MASK: usize = BLK_SIZE - 1;

impl BitMap {
    pub fn with_capacity(cap: usize) -> Self {
        let blocks = (cap + BLK_MASK) / BLK_SIZE;
        BitMap {
            blocks: (0..blocks).map(|_| AtomicU32::new(0)).collect(),
        }
    }

    pub fn mark(&self, pos: usize) {
        let idx = pos / BLK_SIZE;
        let offset = pos - idx * BLK_SIZE;
        unsafe {
            let old = self
                .blocks
                .get_unchecked(idx)
                .fetch_or(1 << offset, Ordering::Relaxed);
            log::debug!("bitmap-mark: pos:{} before:{:#b}", pos, old);
        }
    }
    pub fn unmark(&self, pos: usize) {
        let idx = pos / BLK_SIZE;
        let offset = pos - idx * BLK_SIZE;
        // mark不需要获取返回值，所以不同的mark之间访问时使用relaxed即可。
        unsafe {
            self.blocks
                .get_unchecked(idx)
                .fetch_and(!(1 << offset), Ordering::Relaxed);
        }
    }

    pub(crate) fn blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn snapshot(&self, ss: &mut [u32]) {
        debug_assert_eq!(self.blocks.len(), ss.len());
        unsafe {
            use std::ptr::copy_nonoverlapping;
            copy_nonoverlapping(
                self.blocks.as_ptr() as *const u32,
                ss.as_mut_ptr(),
                ss.len(),
            );
        }
    }
    // snapshot是通过Self::snapshot获取
    pub fn unmark_all(&self, snapshot: &[u32]) {
        debug_assert_eq!(snapshot.len(), self.blocks.len());
        //std::sync::atomic::fence(Ordering::Release);
        unsafe {
            for i in 0..snapshot.len() {
                self.blocks
                    .get_unchecked(i)
                    .fetch_and(!snapshot.get_unchecked(i), Ordering::Relaxed);
            }
        }
    }
}

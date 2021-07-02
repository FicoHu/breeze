use std::sync::Arc;

pub struct Cid {
    id: usize,
    ids: Arc<Ids>,
}

impl Cid {
    pub fn new(id: usize, ids: Arc<Ids>) -> Self {
        Cid { id, ids }
    }
    #[inline(always)]
    pub fn id(&self) -> usize {
        self.id
    }
}
impl Drop for Cid {
    fn drop(&mut self) {
        self.ids.release(self.id);
    }
}

use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
pub struct Ids {
    _active: AtomicIsize,
    bits: Vec<AtomicBool>,
}

impl Ids {
    pub fn with_capacity(cap: usize) -> Self {
        log::debug!("ids builded, cap:{}", cap);
        Self {
            _active: AtomicIsize::new(0),
            bits: (0..cap).map(|_| AtomicBool::new(false)).collect(),
        }
    }
    pub fn next(&self) -> Option<usize> {
        for (id, status) in self.bits.iter().enumerate() {
            match status.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    log::debug!(
                        "fetch next connection id success. _active:{} cap:{}",
                        self._active.fetch_add(1, Ordering::AcqRel) + 1,
                        self.bits.len()
                    );
                    return Some(id);
                }
                Err(_) => {}
            }
        }
        log::debug!(
            "fetch next connection build failed. _active:{}",
            self._active.load(Ordering::Acquire)
        );
        None
    }

    pub fn release(&self, id: usize) {
        log::debug!(
            "connection id freeed. {}",
            self._active.fetch_add(-1, Ordering::AcqRel)
        );
        unsafe {
            match self.bits.get_unchecked(id).compare_exchange(
                true,
                false,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(_) => panic!("not a valid status."),
            }
        }
    }
}

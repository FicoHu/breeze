use lockfree::map::Map;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 无锁，支持并发更新offset，按顺序读取offset的数据结构
pub(super) struct SeqOffset {
    tries: usize,
    offset: AtomicUsize,
    slow_cache: Map<usize, usize>,
}

impl SeqOffset {
    pub(super) fn from(tries: usize) -> Self {
        assert!(tries >= 1);
        Self {
            tries: tries,
            offset: AtomicUsize::new(0),
            slow_cache: Map::default(),
        }
    }
    // 插入一个span, [start, end)。
    // 如果start == offset，则直接更新offset,
    // 否则将span临时存储下来。
    // TODO 临时存储空间可能会触发OOM
    // end > start
    pub(super) fn insert(&self, start: usize, end: usize) {
        debug_assert!(end > start);
        for _i in 0..self.tries {
            match self
                .offset
                .compare_exchange(start, end, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return;
                }
                Err(_offset) => {}
            }
        }
        self.slow_cache.insert(start, end);
    }

    // load offset, [0.. offset)都已经调用insert被相应的span全部填充
    pub(super) fn load(&self) -> usize {
        let mut offset = self.offset.load(Ordering::Acquire);
        let old = offset;
        while let Some(removed) = self.slow_cache.remove(&offset) {
            offset = *removed.val();
            println!("read offset loaded by map:{} ", offset);
        }
        if offset != old {
            self.offset.store(offset, Ordering::Release);
        }
        offset
    }
}

#[cfg(test)]
mod offset_tests {
    use super::SeqOffset;
    #[test]
    fn test_seq_offset() {
        let offset = SeqOffset::from(8);
        assert_eq!(0, offset.load());
        offset.insert(0, 8);
        assert_eq!(8, offset.load());

        offset.insert(9, 10);
        assert_eq!(8, offset.load());

        offset.insert(20, 40);
        assert_eq!(8, offset.load());
        offset.insert(8, 9);
        assert_eq!(10, offset.load());
        offset.insert(10, 15);
        assert_eq!(15, offset.load());
        offset.insert(15, 20);
        assert_eq!(40, offset.load());
    }
}

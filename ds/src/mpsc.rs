// 实现一个有固定多个producer的mpsc
// 写入的时候阻塞式写入。

use cache_line_size::CacheAligned;
use std::cell::Cell;
use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::waker::AtomicWaker;
use super::BitMap;

use crossbeam_queue::SegQueue;

unsafe impl<T> Send for Sender<T> {}
unsafe impl<T> Sync for Sender<T> {}
unsafe impl<T> Send for Receiver<T> {}
unsafe impl<T> Sync for Receiver<T> {}

struct Item<T> {
    lock: AtomicBool,
    data: Cell<MaybeUninit<T>>,
}

impl<T> Item<T> {
    fn try_lock(&self) -> bool {
        match self
            .lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => true,
            _ => false,
        }
    }
    fn unlock(&self) {
        debug_assert_eq!(true, self.lock.load(Ordering::Acquire));
        self.lock.store(false, Ordering::Release);
    }
    fn put(&self, t: T) {
        unsafe { ptr::write(self.data.as_ptr(), MaybeUninit::new(t)) };
    }
    fn write_to(&self, dst: *mut MaybeUninit<T>) {
        unsafe { ptr::copy_nonoverlapping(self.data.as_ptr(), dst, 1) };
    }
    fn take(&self) -> T {
        unsafe { ptr::read(self.data.as_ptr()).assume_init() }
    }
    fn new() -> Self {
        Self {
            lock: AtomicBool::new(false),
            data: Cell::new(MaybeUninit::uninit()),
        }
    }
}
struct Share<T> {
    l1: Vec<CacheAligned<Item<T>>>,
    l1_bits: BitMap,
    l2: SegQueue<T>,
    waker: AtomicWaker,
    closed: AtomicBool,
}

impl<T> Share<T> {
    fn new(p: usize) -> Self {
        let l1 = (0..p).map(|_| CacheAligned(Item::new())).collect();
        Self {
            l1: l1,
            l2: SegQueue::new(),
            waker: AtomicWaker::new(),
            l1_bits: BitMap::with_capacity(p),
            closed: AtomicBool::new(false),
        }
    }
}

pub struct Sender<T> {
    state: Arc<Share<T>>,
}

impl<T> Sender<T> {
    // 先按位置插入。
    pub fn insert(&self, pos: usize, t: T)
    where
        T: std::fmt::Debug,
    {
        debug_assert!(pos < self.state.l1.len());
        unsafe {
            // 先走l1
            let item = &self.state.l1.get_unchecked(pos).0;
            // 在recv中unlock
            if item.try_lock() {
                log::debug!("mpsc-insert: pos:{} l1 hit. {:?}", pos, t);
                item.put(t);
                self.state.l1_bits.mark(pos);
            } else {
                log::debug!("mpsc-insert: pos:{} l1 missed. {:?}", pos, t);
                self.state.l2.push(t);
            }
        }
        self.state.waker.wake();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.waker.wake();
    }
}

pub struct Receiver<T> {
    r: usize,
    w: usize,
    l0: Vec<MaybeUninit<T>>,
    state: Arc<Share<T>>,

    // 记录一次扫描后，哪些item处理过需要unlock。
    // 因为所有的unlock操作需要在unmark之后进行
    lock_w: usize,
    unlocking_idx: Vec<usize>,

    bitmap_snapshot: Vec<u32>,
}
impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Option<T>
    where
        T: std::fmt::Debug,
    {
        unsafe {
            if self.r == self.w {
                self.r = 0;
                self.w = 0;
                self.state.l1_bits.snapshot(&mut self.bitmap_snapshot);
                for i in 0..self.bitmap_snapshot.len() {
                    let mut bits = *self.bitmap_snapshot.get_unchecked(i);
                    log::debug!("mpsc-recv: bits. {} => {:#0b}", i, bits);
                    while bits > 0 {
                        let zeros = bits.trailing_zeros() as usize;
                        let pos = zeros + i * super::bit_map::BLK_SIZE;
                        debug_assert!(pos < self.state.l1.len());
                        let item = &self.state.l1.get_unchecked(pos).0;
                        item.write_to(self.l0.as_mut_ptr().offset(self.w as isize));
                        let p = self.l0.get_unchecked(self.w).as_ptr();
                        log::debug!("mpsc-recv: push to l1 cache:{:?} pos:{}", ptr::read(p), pos);
                        self.w += 1;
                        bits = bits & !(1 << zeros);
                        *self.unlocking_idx.get_unchecked_mut(self.lock_w) = pos;
                        self.lock_w += 1;
                    }
                }
                self.state.l1_bits.unmark_all(&self.bitmap_snapshot);

                for i in 0..self.lock_w {
                    self.state
                        .l1
                        .get_unchecked(*self.unlocking_idx.get_unchecked(i))
                        .0
                        .unlock();
                }
                self.lock_w = 0;
            }

            if self.r < self.w {
                // l1缓存命中
                let data = ptr::read(self.l0.as_mut_ptr().offset(self.r as isize)).assume_init();
                self.r += 1;
                log::debug!("mpsc-recv: l1 cache hit {:?}", data);
                Some(data)
            } else {
                // 说明l1没有命中。继续l2
                log::debug!("mpsc-recv: l1 cache miss");
                self.state.l2.pop()
            }
        }
    }
    pub fn poll_recv(&mut self, cx: &mut Context) -> Poll<Option<T>>
    where
        T: Debug,
    {
        let data = self.recv();
        if let Some(data) = data {
            Poll::Ready(Some(data))
        } else {
            if self.state.closed.load(Ordering::Acquire) {
                Poll::Ready(None)
            } else {
                self.state.waker.register_by_ref(&cx.waker());
                Poll::Pending
            }
        }
    }
}

// p: 最多有p个producer
pub fn channel<T>(p: usize) -> (Sender<T>, Receiver<T>) {
    let share = Arc::new(Share::new(p));
    (
        Sender {
            state: share.clone(),
        },
        Receiver {
            r: 0,
            w: 0,
            l0: (0..p).map(|_| MaybeUninit::uninit()).collect(),
            state: share.clone(),
            lock_w: 0,
            unlocking_idx: vec![0; p],
            bitmap_snapshot: vec![0; share.l1_bits.blocks()],
        },
    )
}

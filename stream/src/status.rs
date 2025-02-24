use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use crate::ResponseData;
use ds::RingSlice;
use protocol::{Request, RequestId};

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum ItemStatus {
    Init = 0u8,
    RequestReceived,
    RequestSent,
    ResponseReceived, // 数据已写入
}
use ItemStatus::*;
impl PartialEq<u8> for ItemStatus {
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<ItemStatus> for u8 {
    fn eq(&self, other: &ItemStatus) -> bool {
        *self == *other as u8
    }
}

unsafe impl Send for ItemStatus {}
#[derive(Default)]
pub struct Item {
    _id: usize,
    seq: AtomicUsize, // 用来做request与response的同步
    status: AtomicU8, // 0: 待接收请求。

    rid: RefCell<RequestId>,
    request: RefCell<Option<Request>>,

    // 下面的数据要加锁才能访问
    waker_lock: AtomicBool,
    waker: RefCell<Option<Waker>>,
    response: RefCell<RingSlice>,
}

unsafe impl Send for Item {}

impl Item {
    pub fn new(cid: usize) -> Self {
        Self {
            _id: cid,
            status: AtomicU8::new(Init as u8),
            ..Default::default()
        }
    }
    // 把buf的指针保存下来。
    // 上面的假设待验证
    #[inline(always)]
    pub fn place_request(&self, req: &Request) {
        debug_assert_eq!(self.status.load(Ordering::Acquire), ItemStatus::Init as u8);
        self.status_cas(ItemStatus::Init as u8, ItemStatus::RequestReceived as u8);
        *self.request.borrow_mut() = Some(req.clone());
        log::debug!(
            "item status: place:{:?}",
            self.rid.replace(req.id().clone())
        );
    }
    #[inline(always)]
    pub fn take_request(&self, seq: usize) -> Request {
        let req = self.request.borrow_mut().take().expect("take request");
        log::debug!(
            "item status: take request. {:?} seq:{} noreply:{}",
            self.rid,
            seq,
            req.noreply()
        );
        // 如果不需要回复，则request之后立即恢复到init状态
        let new = if req.noreply() {
            Init
        } else {
            self.seq_cas(0, seq);
            RequestSent
        };
        self.status_cas(RequestReceived as u8, new as u8);
        req
    }
    #[inline(always)]
    pub fn seq(&self) -> usize {
        self.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn seq_cas(&self, old: usize, new: usize) {
        match self
            .seq
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(cur) => panic!("item status seq cas. {} => {} but found {}", old, new, cur),
        }
    }

    // 有两种可能的状态。
    // Received, 说明之前从来没有poll过
    // Reponded: 有数据并且成功返回
    // 调用方确保当前status不为shutdown
    pub fn poll_read(&self, cx: &mut Context) -> Poll<Result<ResponseData>> {
        self.lock_waker();
        let status = self.status();
        if status == ResponseReceived {
            let response = self.response.take();
            let rid = self.rid.take();
            self.unlock_waker();
            log::debug!("item status: read {:?}", response.location());
            Poll::Ready(Ok(ResponseData::from(response, rid, self.seq())))
        } else if status == Init {
            // 在stream/io/receiver中，确保了一定先发送请求，再读取response。
            // 所以读请求过来时，状态一定不会是Init。
            // 如果是Init，则有一种情况：因为stream异常，状态被重置了。直接返回错误，让client进行容错处理
            return Poll::Ready(Err(Error::new(
                ErrorKind::ConnectionReset,
                "read in init status",
            )));
        } else {
            *self.waker.borrow_mut() = Some(cx.waker().clone());
            self.unlock_waker();
            Poll::Pending
        }
    }
    pub fn place_response(&self, response: RingSlice, seq: usize) {
        debug_assert_eq!(seq, self.seq());
        log::debug!("item status: write :{:?} ", response.location());
        self.response.replace(response);

        // 1. response到达之前的状态是 RequestSent. 即请求已发出。 这是大多数场景
        // 2. 因为在req_handler中，是先发送请求，再调用
        //    bind_req来更新状态为RequestSent，有可能在这中间，response已经接收到了。此时的状态是RequestReceived。
        self.status_cas(RequestSent as u8, ResponseReceived as u8);
        self.try_wake();
    }
    #[inline(always)]
    fn status_cas(&self, old: u8, new: u8) {
        log::debug!(
            "item status cas. expected: {} current:{} update to:{}",
            old,
            self.status.load(Ordering::Acquire),
            new
        );
        match self
            .status
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(cur) => panic!("item status: cas {} => {}, but {} found", old, new, cur),
        }
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline]
    pub fn response_done(&self, seq: usize) {
        // 把状态调整为Init
        let status = self.status();
        debug_assert_eq!(status, ItemStatus::ResponseReceived as u8);
        self.status_cas(status, ItemStatus::Init as u8);
        self.seq_cas(seq, 0);
    }
    #[inline]
    fn try_wake(&self) -> bool {
        self.lock_waker();
        let waker = self.waker.borrow_mut().take();
        self.unlock_waker();
        if let Some(waker) = waker {
            log::debug!(
                "item status: wakeup. id:{} status:{}",
                self._id,
                self.status()
            );
            waker.wake();
            true
        } else {
            log::debug!("item status: not waker found. {}", self._id);
            false
        }
    }
    // TODO ReadGuard
    #[inline(always)]
    fn try_wake_lock(&self) -> bool {
        match self
            .waker_lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    #[inline(always)]
    fn unlock_waker(&self) {
        debug_assert_eq!(self.waker_lock.load(Ordering::Acquire), true);
        self.waker_lock.store(false, Ordering::Release);
    }
    #[inline(always)]
    fn lock_waker(&self) {
        while !self.try_wake_lock() {
            std::hint::spin_loop();
        }
    }
    #[inline(always)]
    pub(crate) fn status_init(&self) -> bool {
        self.status() == ItemStatus::Init as u8
    }
    // reset只会把状态从shutdown变更为init
    // 必须在shutdown之后调用
    pub(crate) fn reset(&self) {
        //self.status_cas(ItemStatus::Shutdown as u8, ItemStatus::Init as u8);
        self.status.store(Init as u8, Ordering::Release);
        self.try_wake();
    }
}

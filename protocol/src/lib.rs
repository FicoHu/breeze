//mod controller;
//pub use controller::{Controller, GroupStream};
//

use std::io::Result;

pub mod chan;
pub mod memcache;
mod slice;
pub use slice::RingSlice;
mod meta;
pub use meta::MetaStream;

use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait Protocol: Unpin + Clone + 'static + Unpin {
    // 一个请求包的最小的字节数
    fn min_size(&self) -> usize;
    // 从response读取buffer时，可能读取多次。
    // 限制最后一个包最小返回长度。
    fn min_last_response_size(&self) -> usize;
    // parse会被一直调用，直到返回true.
    // 当前请求是否结束。
    // 一个请求在req中的第多少个字节结束。
    // req包含的是一个完整的请求。
    fn parse_request(&self, req: &[u8]) -> Result<(bool, usize)>;
    // 按照op来进行路由，通常用于读写分离
    fn op_route(&self, req: &[u8]) -> usize;
    // 调用方必须确保req包含key，否则可能会panic
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8];
    fn keys<'a>(&self, req: &'a [u8]) -> Vec<&'a [u8]>;
    fn build_gets_cmd(&self, keys: Vec<&[u8]>) -> Vec<u8>;
    // 解析当前请求是否结束。返回请求结束位置，如果请求
    // 包含EOF（类似于memcache协议中的END）则返回的位置不包含END信息。
    // 主要用来在进行multiget时，判断请求是否结束。
    fn probe_response_eof(&self, partial_resp: &[u8]) -> (bool, usize);
    // 从response中解析出一个完成的response
    fn parse_response(&self, response: &RingSlice) -> (bool, usize);
    fn probe_response_found(&self, response: &[u8]) -> bool;
    fn meta(&self, url: &str) -> MetaStream;
}
#[enum_dispatch(Protocol)]
#[derive(Clone)]
pub enum Protocols {
    Mc(memcache::Memcache),
}

impl Protocols {
    pub fn from(name: &str) -> Option<Self> {
        match name {
            "mc" | "memcache" | "memcached" => Some(Self::Mc(memcache::Memcache::new())),
            _ => None,
        }
    }
}

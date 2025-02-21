use super::RingSlice;

use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;

// <= read的字节是已经全部读取完的
// [read, processed]是已经写入，但未全部收到读取完成通知的
// write是当前写入的地址
pub struct ResponseRingBuffer {
    data: NonNull<u8>,
    size: usize,
    read: usize,
    processed: usize,
    write: usize,
}

impl ResponseRingBuffer {
    pub fn with_capacity(size: usize) -> Self {
        let buff_size = size.next_power_of_two();
        let mut data = Vec::with_capacity(buff_size);
        let ptr = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        std::mem::forget(data);
        Self {
            size: buff_size,
            data: ptr,
            read: 0,
            processed: 0,
            write: 0,
        }
    }
    #[inline]
    pub fn processed(&self) -> usize {
        self.processed
    }
    #[inline]
    pub fn reset_read(&mut self, read: usize) {
        self.read = read;
    }
    #[inline]
    pub fn writtened(&self) -> usize {
        self.write
    }
    #[inline]
    pub fn advance_processed(&mut self, n: usize) {
        self.processed += n;
    }
    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.write += n;
    }
    #[inline(always)]
    fn mask(&self, offset: usize) -> usize {
        offset & (self.size - 1)
    }
    // 如果无法写入，则返回一个长度为0的slice
    #[inline(always)]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        let offset = self.mask(self.write);
        let n = if self.read + self.size == self.write {
            // 已满
            0
        } else {
            let read = self.mask(self.read);
            if offset < read {
                read - offset
            } else {
                self.size - offset
            }
        };
        log::debug!(
            "response as mut bytes. read:{} processed:{} write:{} available:{}",
            self.read,
            self.processed,
            self.write,
            n
        );

        unsafe { from_raw_parts_mut(self.data.as_ptr().offset(offset as isize), n) }
    }
    // 返回已写入，未处理的字节。即从[processed,write)的字节
    #[inline(always)]
    pub fn processing_bytes(&self) -> RingSlice {
        RingSlice::from(self.data.as_ptr(), self.size, self.processed, self.write)
    }
}

impl Drop for ResponseRingBuffer {
    fn drop(&mut self) {
        unsafe {
            let _ = Vec::from_raw_parts(self.data.as_ptr(), 0, self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ResponseRingBuffer;
    use rand::Rng;
    use std::ptr::copy_nonoverlapping;

    fn rnd_write(w: &mut [u8], size: usize) -> Vec<u8> {
        debug_assert!(size <= w.len());
        let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        unsafe { copy_nonoverlapping(data.as_ptr(), w.as_mut_ptr(), size) };
        data
    }

    #[test]
    fn test_response_buffer() {
        let cap = 32;
        let mut buffer = ResponseRingBuffer::with_capacity(cap);
        let data = rnd_write(buffer.as_mut_bytes(), cap);
        let mut response = buffer.processing_bytes();
        assert_eq!(response.available(), 0);
    }
}

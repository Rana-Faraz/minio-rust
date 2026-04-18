use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Debug)]
pub struct BytePoolCap {
    buffers: Mutex<VecDeque<Vec<u8>>>,
    max_size: usize,
    width: usize,
    width_cap: usize,
}

impl BytePoolCap {
    pub fn new(max_size: u64, width: usize, capwidth: usize) -> Self {
        assert!(capwidth > 0, "total buffer capacity must be provided");
        assert!(
            capwidth >= 64,
            "buffer capped with smaller than 64 bytes is not supported"
        );
        assert!(
            width <= capwidth,
            "minimum buffer length cannot be > capacity of the buffer"
        );

        Self {
            buffers: Mutex::new(VecDeque::with_capacity(max_size as usize)),
            max_size: max_size as usize,
            width,
            width_cap: capwidth,
        }
    }

    pub fn populate(&self) {
        let mut buffers = self.buffers.lock().expect("byte pool mutex poisoned");
        while buffers.len() < self.max_size {
            buffers.push_back(self.allocate());
        }
    }

    pub fn get(&self) -> Vec<u8> {
        let mut buffers = self.buffers.lock().expect("byte pool mutex poisoned");
        buffers.pop_front().unwrap_or_else(|| self.allocate())
    }

    pub fn put(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() != self.width_cap {
            return;
        }

        buffer.resize(self.width, 0);

        let mut buffers = self.buffers.lock().expect("byte pool mutex poisoned");
        if buffers.len() < self.max_size {
            buffers.push_back(buffer);
        }
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn width_cap(&self) -> usize {
        self.width_cap
    }

    pub fn current_size(&self) -> usize {
        let buffers = self.buffers.lock().expect("byte pool mutex poisoned");
        buffers.len() * self.width
    }

    pub fn buffered_count(&self) -> usize {
        let buffers = self.buffers.lock().expect("byte pool mutex poisoned");
        buffers.len()
    }

    fn allocate(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.width_cap);
        buffer.resize(self.width, 0);
        buffer
    }
}

use std::io::{self, Read};

#[derive(Debug, Clone)]
pub struct DummyDataGenerator {
    seed: u64,
    remaining: usize,
    offset: usize,
}

impl DummyDataGenerator {
    pub fn new(size: usize, seed: u64) -> Self {
        Self {
            seed,
            remaining: size,
            offset: 0,
        }
    }

    fn byte_at(&self, index: usize) -> u8 {
        let mixed = self
            .seed
            .wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        (mixed ^ (mixed >> 32)) as u8
    }
}

impl Read for DummyDataGenerator {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }

        let count = buf.len().min(self.remaining);
        for (index, slot) in buf.iter_mut().take(count).enumerate() {
            *slot = self.byte_at(self.offset + index);
        }
        self.offset += count;
        self.remaining -= count;
        Ok(count)
    }
}

pub fn cmp_readers(left: &mut impl Read, right: &mut impl Read) -> Result<bool, String> {
    let mut left_buf = [0_u8; 4096];
    let mut right_buf = [0_u8; 4096];

    loop {
        let left_read = left.read(&mut left_buf).map_err(|err| err.to_string())?;
        let right_read = right.read(&mut right_buf).map_err(|err| err.to_string())?;

        if left_read != right_read {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
        if left_buf[..left_read] != right_buf[..right_read] {
            return Ok(false);
        }
    }
}

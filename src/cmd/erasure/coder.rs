use crate::cmd::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectPartInfo {
    pub number: i32,
    pub size: i64,
    pub etag: String,
    pub actual_size: i64,
}
impl_msg_codec!(ObjectPartInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ErasureInfo {
    pub algorithm: String,
    pub data_blocks: i32,
    pub parity_blocks: i32,
    pub block_size: i64,
    pub index: i32,
    pub distribution: Option<Vec<i32>>,
}
impl_msg_codec!(ErasureInfo);

pub const BLOCK_SIZE_V2: i64 = 1024 * 1024;

#[derive(Debug)]
pub struct ErasureCoder {
    encoder: ReedSolomon,
    data_blocks: usize,
    parity_blocks: usize,
    block_size: i64,
}

impl ErasureCoder {
    pub fn new(data_blocks: usize, parity_blocks: usize, block_size: i64) -> Result<Self, String> {
        if data_blocks == 0 || data_blocks + parity_blocks == 0 {
            return Err("invalid shard count".to_string());
        }
        let encoder =
            ReedSolomon::new(data_blocks, parity_blocks).map_err(|err| err.to_string())?;
        Ok(Self {
            encoder,
            data_blocks,
            parity_blocks,
            block_size,
        })
    }

    pub fn data_blocks(&self) -> usize {
        self.data_blocks
    }

    pub fn parity_blocks(&self) -> usize {
        self.parity_blocks
    }

    pub fn total_blocks(&self) -> usize {
        self.data_blocks + self.parity_blocks
    }

    pub fn shard_size(&self) -> i64 {
        ceil_frac(self.block_size, self.data_blocks as i64)
    }

    pub fn shard_file_size(&self, total_length: i64) -> i64 {
        if total_length <= 0 {
            return total_length.max(0);
        }
        let full_blocks = total_length / self.block_size;
        let last_block_size = total_length % self.block_size;
        let last_shard_size = ceil_frac(last_block_size, self.data_blocks as i64);
        full_blocks * self.shard_size() + last_shard_size
    }

    pub fn encode_data(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        if data.is_empty() {
            return Ok(vec![Vec::new(); self.total_blocks()]);
        }

        let shard_size = ceil_frac(data.len() as i64, self.data_blocks as i64) as usize;
        let mut shards = vec![vec![0_u8; shard_size]; self.total_blocks()];
        for (index, chunk) in data.chunks(shard_size).enumerate() {
            shards[index][..chunk.len()].copy_from_slice(chunk);
        }

        self.encoder
            .encode(&mut shards)
            .map_err(|err| err.to_string())?;
        Ok(shards)
    }

    pub fn decode_data_blocks(&self, shards: &mut [Vec<u8>]) -> Result<(), String> {
        self.reconstruct(shards, true)
    }

    pub fn decode_data_and_parity_blocks(&self, shards: &mut [Vec<u8>]) -> Result<(), String> {
        self.reconstruct(shards, false)
    }

    fn reconstruct(&self, shards: &mut [Vec<u8>], data_only: bool) -> Result<(), String> {
        if shards.len() != self.total_blocks() {
            return Err("unexpected shard count".to_string());
        }
        if shards.iter().all(|shard| shard.is_empty()) {
            return Ok(());
        }

        let mut opt_shards: Vec<Option<Vec<u8>>> = shards
            .iter()
            .map(|shard| (!shard.is_empty()).then(|| shard.clone()))
            .collect();

        if data_only {
            self.encoder
                .reconstruct_data(&mut opt_shards)
                .map_err(|err| err.to_string())?;
        } else {
            self.encoder
                .reconstruct(&mut opt_shards)
                .map_err(|err| err.to_string())?;
        }

        for (dst, src) in shards.iter_mut().zip(opt_shards.into_iter()) {
            *dst = src.unwrap_or_default();
        }

        Ok(())
    }

    pub fn encode(
        &self,
        src: &mut impl Read,
        writers: &mut [Option<&mut dyn Write>],
        buf: &mut [u8],
        quorum: usize,
    ) -> Result<i64, String> {
        if writers.len() != self.total_blocks() {
            return Err("unexpected writer count".to_string());
        }

        let mut total = 0_i64;

        loop {
            let n = src.read(buf).map_err(|err| err.to_string())?;
            if n == 0 && total != 0 {
                break;
            }

            let blocks = self.encode_data(&buf[..n])?;
            let mut successes = 0_usize;
            let mut offline = 0_usize;

            for (writer, block) in writers.iter_mut().zip(blocks.iter()) {
                match writer {
                    Some(inner) => {
                        if inner.write_all(block).is_ok() {
                            successes += 1;
                        } else {
                            *writer = None;
                        }
                    }
                    None => offline += 1,
                }
            }

            if successes < quorum {
                return Err(format!(
                    "write quorum not met (successes={successes}, required={quorum}, offline-disks={offline}/{})",
                    writers.len()
                ));
            }

            total += n as i64;
            if n == 0 {
                break;
            }
        }

        Ok(total)
    }

    pub fn decode(
        &self,
        writer: &mut impl Write,
        shards: &mut [Vec<u8>],
        offset: i64,
        length: i64,
        total_length: i64,
    ) -> Result<i64, String> {
        if offset < 0 || length < 0 || offset + length > total_length {
            return Err("invalid argument".to_string());
        }
        if length == 0 {
            return Ok(0);
        }

        self.decode_data_blocks(shards)?;
        let written = write_data_blocks(writer, shards, self.data_blocks, offset, length)?;
        if written != length {
            return Err("less data than expected".to_string());
        }
        Ok(written)
    }
}

pub fn write_data_blocks(
    dst: &mut impl Write,
    encoded_blocks: &[Vec<u8>],
    data_blocks: usize,
    mut offset: i64,
    length: i64,
) -> Result<i64, String> {
    if offset < 0 || length < 0 {
        return Err("offset and length must be non-negative".to_string());
    }
    if encoded_blocks.len() < data_blocks {
        return Err("too few shards".to_string());
    }

    let data_len: i64 = encoded_blocks
        .iter()
        .take(data_blocks)
        .map(|block| block.len() as i64)
        .sum();
    if data_len < length {
        return Err("short data".to_string());
    }

    let mut remaining = length;
    let mut written = 0_i64;
    for block in encoded_blocks.iter().take(data_blocks) {
        if remaining == 0 {
            break;
        }
        if offset >= block.len() as i64 {
            offset -= block.len() as i64;
            continue;
        }

        let start = offset as usize;
        let available = &block[start..];
        let to_write = available.len().min(remaining as usize);
        dst.write_all(&available[..to_write])
            .map_err(|err| err.to_string())?;
        written += to_write as i64;
        remaining -= to_write as i64;
        offset = 0;
    }

    Ok(written)
}

pub fn ceil_frac(numerator: i64, denominator: i64) -> i64 {
    if denominator == 0 {
        return 0;
    }
    let (numerator, denominator) = if denominator < 0 {
        (-numerator, -denominator)
    } else {
        (numerator, denominator)
    };
    let mut ceil = numerator / denominator;
    if numerator > 0 && numerator % denominator != 0 {
        ceil += 1;
    }
    ceil
}

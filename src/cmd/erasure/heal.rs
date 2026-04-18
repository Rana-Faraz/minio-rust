use crate::cmd::*;

impl ErasureCoder {
    pub fn heal(
        &self,
        stale_writers: &mut [Option<&mut dyn Write>],
        readers: &mut [Option<&mut dyn Read>],
        total_length: i64,
    ) -> Result<Vec<Vec<u8>>, String> {
        if stale_writers.len() != self.total_blocks() || readers.len() != self.total_blocks() {
            return Err("unexpected shard count".to_string());
        }
        if total_length < 0 {
            return Err("invalid argument".to_string());
        }

        let mut shards = vec![Vec::new(); self.total_blocks()];
        let mut good_readers = 0usize;
        for (index, reader) in readers.iter_mut().enumerate() {
            let Some(reader) = reader.as_mut() else {
                continue;
            };
            let mut shard = Vec::new();
            if reader.read_to_end(&mut shard).is_ok() {
                shards[index] = shard;
                good_readers += 1;
            }
        }

        if total_length == 0 {
            return Ok(shards);
        }
        if good_readers < self.data_blocks() {
            return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
        }

        self.decode_data_and_parity_blocks(&mut shards)
            .map_err(|_| cmd_err(ERR_ERASURE_READ_QUORUM))?;

        let mut attempted_writes = 0usize;
        let mut successful_writes = 0usize;
        for (writer, shard) in stale_writers.iter_mut().zip(shards.iter()) {
            let Some(writer) = writer.as_mut() else {
                continue;
            };
            attempted_writes += 1;
            if writer.write_all(shard).is_ok() {
                successful_writes += 1;
            }
        }
        if attempted_writes > 0 && successful_writes == 0 {
            return Err(cmd_err(ERR_ERASURE_WRITE_QUORUM));
        }

        Ok(shards)
    }
}

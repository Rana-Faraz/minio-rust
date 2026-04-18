use std::io::Read;

use crate::internal::bucket::encryption::{parse_bucket_sse_config, BucketSseConfig};

pub fn validate_bucket_sse_config(reader: impl Read) -> Result<BucketSseConfig, String> {
    parse_bucket_sse_config(reader).map_err(|err| err.to_string())
}

use std::sync::Arc;

use crate::internal::dsync::{new_in_memory_net_locker, InMemoryNetLocker};

pub fn new_lock_rest_client(endpoint: &str) -> Arc<InMemoryNetLocker> {
    new_in_memory_net_locker(endpoint.to_string())
}

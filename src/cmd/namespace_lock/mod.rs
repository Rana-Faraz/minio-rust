use crate::internal::dsync::{DRWMutex, Dsync};

#[derive(Clone)]
pub struct NamespaceLockMap {
    dsync: Dsync,
}

impl NamespaceLockMap {
    pub fn new(nodes: usize) -> Self {
        Self {
            dsync: Dsync::new_in_memory(nodes),
        }
    }

    pub fn new_lock(&self, bucket: &str, object: &str) -> DRWMutex {
        let resource = if object.is_empty() {
            bucket.to_string()
        } else {
            format!("{bucket}/{object}")
        };
        DRWMutex::new(self.dsync.clone(), vec![resource])
    }
}

#[track_caller]
pub fn get_source(_skip: usize) -> String {
    let caller = std::panic::Location::caller();
    format!("{}:{}", caller.file(), caller.line())
}

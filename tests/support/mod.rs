#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;

pub fn pending_reference_test(message: &str) -> ! {
    panic!("{}", message);
}

pub fn workspace_tempdir(tag: &str) -> TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(tag);
    fs::create_dir_all(&root).expect("create workspace temp root");
    TempDir::new_in(root).expect("create workspace tempdir")
}

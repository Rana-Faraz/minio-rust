use minio_rust::internal::dsync::Dsync;

#[test]
fn missing_extracted_entries() {
    let dsync = Dsync::new_in_memory(2);
    assert_eq!(dsync.lockers().len(), 2);
    assert!(dsync.lockers()[0].is_local());
    assert!(dsync.lockers()[0].is_online());
    assert!(dsync.lockers()[0].endpoint().contains("locker-0"));
}

// Rust test snapshot derived from cmd/leak-detect_test.go.

use minio_rust::cmd::{is_ignored_stack_fn, pick_relevant_goroutines_from_dump, LeakDetect};

pub const SOURCE_FILE: &str = "cmd/leak-detect_test.go";

#[test]
fn test_pick_relevant_goroutines_from_dump() {
    let dump = "\
goroutine 1 [running]:
testing.RunTests()

goroutine 2 [running]:
cmd.actualWorker()
created by app.main

goroutine 3 [running]:
runtime.goexit

goroutine 4 [running]:
cmd.backgroundHeal()
created by app.main
";

    let relevant = pick_relevant_goroutines_from_dump(dump);
    assert_eq!(relevant.len(), 2);
    assert!(relevant
        .iter()
        .any(|entry| entry.contains("cmd.actualWorker()")));
    assert!(relevant
        .iter()
        .any(|entry| entry.contains("cmd.backgroundHeal()")));
}

#[test]
fn test_leak_detect_compare_snapshot() {
    assert!(is_ignored_stack_fn("runtime.goexit"));
    assert!(!is_ignored_stack_fn("cmd.workerLoop()"));

    let initial = "\
goroutine 1 [running]:
cmd.workerLoop()

goroutine 2 [running]:
runtime.goexit
";
    let later = "\
goroutine 1 [running]:
cmd.workerLoop()

goroutine 3 [running]:
cmd.newLeakedWorker()
";

    let snapshot = LeakDetect::new_from_dump(initial);
    let leaked = snapshot.compare_snapshot_from_dump(later);
    assert_eq!(leaked.len(), 1);
    assert!(leaked[0].contains("cmd.newLeakedWorker()"));
}

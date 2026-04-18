use std::collections::BTreeSet;

pub const LEAK_DETECT_DEADLINE_SECONDS: u64 = 5;
pub const LEAK_DETECT_PAUSE_TIME_MS: u64 = 50;

pub static IGNORED_STACK_FNS: &[&str] = &[
    "",
    "testing.Main(",
    "testing.tRunner(",
    "runtime.goexit",
    "created by runtime.gc",
    "pickRelevantGoroutines",
    "runtime.MHeap_Scavenger",
    "signal.signal_recv",
    "sigterm.handler",
    "runtime_mcall",
    "goroutine in C code",
];

pub fn is_ignored_stack_fn(stack: &str) -> bool {
    IGNORED_STACK_FNS
        .iter()
        .filter(|stack_fn| !stack_fn.is_empty())
        .any(|stack_fn| stack.contains(stack_fn))
}

pub fn pick_relevant_goroutines_from_dump(dump: &str) -> Vec<String> {
    let mut goroutines = Vec::new();
    for goroutine in dump.split("\n\n") {
        let mut parts = goroutine.splitn(2, '\n');
        let _header = parts.next();
        let Some(stack) = parts.next() else {
            continue;
        };
        let stack = stack.trim();
        if stack.starts_with("testing.RunTests") {
            continue;
        }
        if is_ignored_stack_fn(stack) {
            continue;
        }
        goroutines.push(goroutine.trim().to_string());
    }
    goroutines.sort();
    goroutines
}

#[derive(Debug, Clone, Default)]
pub struct LeakDetect {
    relevant_routines: BTreeSet<String>,
}

impl LeakDetect {
    pub fn new_from_dump(dump: &str) -> Self {
        Self {
            relevant_routines: pick_relevant_goroutines_from_dump(dump)
                .into_iter()
                .collect(),
        }
    }

    pub fn compare_snapshot_from_dump(&self, dump: &str) -> Vec<String> {
        pick_relevant_goroutines_from_dump(dump)
            .into_iter()
            .filter(|goroutine| !self.relevant_routines.contains(goroutine))
            .collect()
    }
}

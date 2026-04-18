use std::sync::Arc;
use std::thread;

use minio_rust::cmd::{redact_ldap_password, HttpStats};

pub const SOURCE_FILE: &str = "cmd/http-tracer_test.go";

#[test]
fn test_redact_ldappwd_line_29() {
    let input = "ldap://server?LDAPPassword=secret&user=alice";
    let redacted = redact_ldap_password(input);
    assert!(redacted.contains("LDAPPassword=*REDACTED*"));
    assert!(!redacted.contains("secret"));
}

#[test]
fn test_raul_stats_race_condition_line_64() {
    let stats = Arc::new(HttpStats::new());
    let mut joins = Vec::new();
    for _ in 0..8 {
        let stats = Arc::clone(&stats);
        joins.push(thread::spawn(move || {
            for _ in 0..500 {
                stats.record_request(None, false);
            }
        }));
    }
    for join in joins {
        join.join().expect("join");
    }
    assert_eq!(stats.total_requests(), 4_000);
    assert_eq!(stats.total_errors(), 0);
}

#[test]
fn test_raul_httpapistats_race_condition_line_120() {
    let stats = Arc::new(HttpStats::new());
    let mut joins = Vec::new();
    for i in 0..6 {
        let stats = Arc::clone(&stats);
        joins.push(thread::spawn(move || {
            for _ in 0..250 {
                stats.record_request(None, i % 2 == 0);
            }
        }));
    }
    for join in joins {
        join.join().expect("join");
    }
    assert_eq!(stats.total_requests(), 1_500);
    assert_eq!(stats.total_errors(), 750);
}

#[test]
fn test_raul_bucket_httpstats_race_condition_line_157() {
    let stats = Arc::new(HttpStats::new());
    let mut joins = Vec::new();
    for _ in 0..4 {
        let stats = Arc::clone(&stats);
        joins.push(thread::spawn(move || {
            for _ in 0..300 {
                stats.record_request(Some("photos"), false);
                stats.record_request(Some("videos"), false);
            }
        }));
    }
    for join in joins {
        join.join().expect("join");
    }
    assert_eq!(stats.bucket_requests("photos"), 1_200);
    assert_eq!(stats.bucket_requests("videos"), 1_200);
    assert_eq!(stats.total_requests(), 2_400);
}

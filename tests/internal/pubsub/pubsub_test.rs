use std::sync::{mpsc, Arc};
use std::time::Duration;

use minio_rust::internal::pubsub::{Maskable, PubSub, MASK_ALL};

pub const SOURCE_FILE: &str = "internal/pubsub/pubsub_test.go";

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaskString(&'static str);

impl Maskable for MaskString {
    fn mask(&self) -> u64 {
        1
    }
}

#[test]
fn pubsub_subscribe_tracks_subscribers() {
    let pubsub = PubSub::<MaskString>::new(2);
    let (tx1, _rx1) = mpsc::sync_channel(1);
    let (tx2, _rx2) = mpsc::sync_channel(1);
    let _sub1 = pubsub.subscribe(MASK_ALL, tx1, None).expect("subscribe");
    let _sub2 = pubsub.subscribe(MASK_ALL, tx2, None).expect("subscribe");

    assert_eq!(pubsub.subscriber_slots(), 2);
    assert_eq!(pubsub.num_subscribers(MASK_ALL), 2);
    assert_eq!(pubsub.subscribers(), 2);
}

#[test]
fn pubsub_num_subscribers_honors_masks() {
    let pubsub = PubSub::<MaskString>::new(2);
    let (tx1, _rx1) = mpsc::sync_channel(1);
    let (tx2, _rx2) = mpsc::sync_channel(1);
    let _sub1 = pubsub.subscribe(1, tx1, None).expect("subscribe");
    let _sub2 = pubsub.subscribe(2, tx2, None).expect("subscribe");

    assert_eq!(pubsub.subscriber_slots(), 2);
    assert_eq!(pubsub.num_subscribers(1), 2);
    assert_eq!(pubsub.num_subscribers(2), 2);
    assert_eq!(pubsub.num_subscribers(1 | 2), 2);
    assert_eq!(pubsub.num_subscribers(MASK_ALL), 2);
    assert_eq!(pubsub.num_subscribers(4), 0);
}

#[test]
fn pubsub_enforces_subscriber_limit() {
    let pubsub = PubSub::<MaskString>::new(2);
    let (tx1, _rx1) = mpsc::sync_channel(1);
    let (tx2, _rx2) = mpsc::sync_channel(1);
    let (tx3, _rx3) = mpsc::sync_channel(1);
    let _sub1 = pubsub.subscribe(MASK_ALL, tx1, None).expect("subscribe");
    let _sub2 = pubsub.subscribe(MASK_ALL, tx2, None).expect("subscribe");
    let third = pubsub.subscribe(MASK_ALL, tx3, None);
    assert!(third.is_err());
}

#[test]
fn pubsub_unsubscribe_removes_subscriber() {
    let pubsub = PubSub::<MaskString>::new(2);
    let (tx1, _rx1) = mpsc::sync_channel(1);
    let (tx2, _rx2) = mpsc::sync_channel(1);
    let sub1 = pubsub.subscribe(MASK_ALL, tx1, None).expect("subscribe");
    let sub2 = pubsub.subscribe(MASK_ALL, tx2, None).expect("subscribe");

    drop(sub1);
    assert_eq!(pubsub.subscriber_slots(), 1);
    drop(sub2);
    assert_eq!(pubsub.subscriber_slots(), 0);
}

#[test]
fn pubsub_delivers_to_single_subscriber() {
    let pubsub = PubSub::<MaskString>::new(1);
    let (tx1, rx1) = mpsc::sync_channel(1);
    let _sub1 = pubsub
        .subscribe(MASK_ALL, tx1, Some(Arc::new(|_: &MaskString| true)))
        .expect("subscribe");

    let value = MaskString("hello");
    pubsub.publish(value.clone());
    let message = rx1
        .recv_timeout(Duration::from_millis(100))
        .expect("message");
    assert_eq!(message, value);
}

#[test]
fn pubsub_delivers_to_multiple_subscribers() {
    let pubsub = PubSub::<MaskString>::new(2);
    let (tx1, rx1) = mpsc::sync_channel(1);
    let (tx2, rx2) = mpsc::sync_channel(1);
    let filter = Arc::new(|_: &MaskString| true);
    let _sub1 = pubsub
        .subscribe(MASK_ALL, tx1, Some(filter.clone()))
        .expect("subscribe");
    let _sub2 = pubsub
        .subscribe(MASK_ALL, tx2, Some(filter))
        .expect("subscribe");

    let value = MaskString("hello");
    pubsub.publish(value.clone());
    let message1 = rx1
        .recv_timeout(Duration::from_millis(100))
        .expect("message1");
    let message2 = rx2
        .recv_timeout(Duration::from_millis(100))
        .expect("message2");
    assert_eq!(message1, value);
    assert_eq!(message2, value);
}

#[test]
fn pubsub_delivers_only_to_matching_masks() {
    let pubsub = PubSub::<MaskString>::new(3);
    let (tx1, rx1) = mpsc::sync_channel(1);
    let (tx2, rx2) = mpsc::sync_channel(1);
    let (tx3, rx3) = mpsc::sync_channel(1);
    let filter = Arc::new(|_: &MaskString| true);

    let _sub1 = pubsub
        .subscribe(1, tx1, Some(filter.clone()))
        .expect("subscribe");
    let _sub2 = pubsub
        .subscribe(1 | 2, tx2, Some(filter.clone()))
        .expect("subscribe");
    let _sub3 = pubsub.subscribe(2, tx3, Some(filter)).expect("subscribe");

    let value = MaskString("hello");
    pubsub.publish(value.clone());

    assert_eq!(
        rx1.recv_timeout(Duration::from_millis(100))
            .expect("message1"),
        value
    );
    assert_eq!(
        rx2.recv_timeout(Duration::from_millis(100))
            .expect("message2"),
        value
    );
    assert!(rx3.recv_timeout(Duration::from_millis(50)).is_err());
}

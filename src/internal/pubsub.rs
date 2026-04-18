use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc::SyncSender, Arc, Mutex, Weak};

pub type Mask = u64;
pub const MASK_ALL: Mask = u64::MAX;

pub trait Maskable {
    fn mask(&self) -> u64;
}

type Filter<T> = Arc<dyn Fn(&T) -> bool + Send + Sync + 'static>;

struct Subscriber<T> {
    id: u64,
    sender: SyncSender<T>,
    mask: Mask,
    filter: Option<Filter<T>>,
}

struct Inner<T> {
    max_subscribers: i32,
    next_id: AtomicU64,
    subs: Mutex<Vec<Subscriber<T>>>,
}

pub struct PubSub<T> {
    inner: Arc<Inner<T>>,
}

pub struct Subscription<T> {
    inner: Weak<Inner<T>>,
    id: u64,
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut subs = inner.subs.lock().expect("pubsub should lock");
            subs.retain(|sub| sub.id != self.id);
        }
    }
}

impl<T> PubSub<T>
where
    T: Maskable + Clone + Send + 'static,
{
    pub fn new(max_subscribers: i32) -> Self {
        Self {
            inner: Arc::new(Inner {
                max_subscribers,
                next_id: AtomicU64::new(1),
                subs: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn subscribe(
        &self,
        mask: Mask,
        sender: SyncSender<T>,
        filter: Option<Filter<T>>,
    ) -> Result<Subscription<T>, String> {
        let mut subs = self.inner.subs.lock().expect("pubsub should lock");
        if self.inner.max_subscribers > 0 && subs.len() as i32 >= self.inner.max_subscribers {
            return Err(format!(
                "the limit of `{}` subscribers is reached",
                self.inner.max_subscribers
            ));
        }

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        subs.push(Subscriber {
            id,
            sender,
            mask,
            filter,
        });

        Ok(Subscription {
            inner: Arc::downgrade(&self.inner),
            id,
        })
    }

    pub fn publish(&self, item: T) {
        let subs = self.inner.subs.lock().expect("pubsub should lock");
        for sub in subs.iter() {
            if contains(sub.mask, item.mask())
                && sub
                    .filter
                    .as_ref()
                    .map(|filter| filter(&item))
                    .unwrap_or(true)
            {
                let _ = sub.sender.try_send(item.clone());
            }
        }
    }

    pub fn num_subscribers(&self, mask: Mask) -> i32 {
        let subs = self.inner.subs.lock().expect("pubsub should lock");
        if !subs.iter().any(|sub| overlaps(sub.mask, mask)) {
            return 0;
        }
        subs.len() as i32
    }

    pub fn subscribers(&self) -> i32 {
        self.inner.subs.lock().expect("pubsub should lock").len() as i32
    }

    pub fn subscriber_slots(&self) -> usize {
        self.inner.subs.lock().expect("pubsub should lock").len()
    }
}

pub fn contains(mask: Mask, other: Mask) -> bool {
    mask & other == other
}

pub fn overlaps(mask: Mask, other: Mask) -> bool {
    mask & other != 0
}

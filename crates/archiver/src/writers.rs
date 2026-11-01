// Project:   dfe-archiver
// File:      crates/archiver/src/writers.rs
// Purpose:   The open archive writers, capped at archive.max_writers
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

//! The archive writers open at once, one per destination, at most
//! `archive.max_writers` of them.
//!
//! A writer holds a slot from the moment it is created until its file is
//! closed. An evicted writer keeps its slot while it closes, compressing and
//! writing its last block, so a write for a new destination with every slot
//! taken waits for an eviction to finish rather than opening past the cap.

use dfe_archiver_core::archive::ArchiveWriter;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// One destination's writer, locked for each write.
pub(crate) type SharedWriter = Arc<Mutex<ArchiveWriter>>;

/// The writers by destination, least recently used first to go.
pub(crate) struct WriterCache {
    state: parking_lot::Mutex<Slots>,
    /// Woken whenever a slot frees or a writer joins the cache.
    changed: Notify,
}

struct Slots {
    writers: LruCache<String, SharedWriter>,
    cap: usize,
    /// Slots handed out for writers not yet in `writers`.
    reserved: usize,
    /// Evicted writers still closing.
    closing: usize,
    /// Lookups waiting for a slot.
    waiting: usize,
}

impl Slots {
    fn live(&self) -> usize {
        self.writers.len() + self.reserved + self.closing
    }
}

/// What a lookup found for its destination.
pub(crate) enum Lookup<'a> {
    /// The destination's open writer.
    Open(SharedWriter),
    /// A slot for a new writer. Dropped unfilled, it is given back.
    Vacant(Reservation<'a>),
}

/// A slot for one new writer.
pub(crate) struct Reservation<'a> {
    cache: &'a WriterCache,
    filled: bool,
}

/// The slot an evicted writer holds until its close finishes.
pub(crate) struct Closing {
    cache: Arc<WriterCache>,
}

/// A lookup counted in `waiting` while it waits for a slot.
struct Waiting<'a> {
    cache: &'a WriterCache,
    counted: bool,
}

impl WriterCache {
    /// Room for `cap` writers, at least one.
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(Slots {
                writers: LruCache::unbounded(),
                cap: cap.max(1),
                reserved: 0,
                closing: 0,
                waiting: 0,
            }),
            changed: Notify::new(),
        }
    }

    /// The writer of `destination`, or a slot for a new one.
    ///
    /// With every slot taken, the least recently used writer goes to `evict`
    /// with the slot its close holds, one eviction a waiting lookup, and the
    /// lookup waits for a slot to free.
    pub(crate) async fn lookup(
        self: &Arc<Self>,
        destination: &str,
        mut evict: impl FnMut(String, SharedWriter, Closing),
    ) -> Lookup<'_> {
        let mut waiting = Waiting {
            cache: self,
            counted: false,
        };
        loop {
            // Created before the check, so a slot freed after it still wakes this lookup.
            let changed = self.changed.notified();
            let evicted = {
                let mut slots = self.state.lock();
                if let Some(writer) = slots.writers.get(destination) {
                    return Lookup::Open(Arc::clone(writer));
                }
                if slots.live() < slots.cap {
                    slots.reserved += 1;
                    return Lookup::Vacant(Reservation {
                        cache: self,
                        filled: false,
                    });
                }
                if !waiting.counted {
                    slots.waiting += 1;
                    waiting.counted = true;
                }
                if slots.closing < slots.waiting {
                    let evicted = slots.writers.pop_lru();
                    if evicted.is_some() {
                        slots.closing += 1;
                    }
                    evicted
                } else {
                    None
                }
            };
            if let Some((key, writer)) = evicted {
                evict(
                    key,
                    writer,
                    Closing {
                        cache: Arc::clone(self),
                    },
                );
            }
            changed.await;
        }
    }

    /// The writers open now, by destination.
    pub(crate) fn snapshot(&self) -> Vec<(String, SharedWriter)> {
        self.state
            .lock()
            .writers
            .iter()
            .map(|(destination, writer)| (destination.clone(), Arc::clone(writer)))
            .collect()
    }

    /// Take every open writer out, to close at shutdown.
    pub(crate) fn take_all(&self) -> Vec<(String, SharedWriter)> {
        let mut slots = self.state.lock();
        let mut all = Vec::with_capacity(slots.writers.len());
        while let Some(entry) = slots.writers.pop_lru() {
            all.push(entry);
        }
        all
    }

    /// The writers open now.
    pub(crate) fn len(&self) -> usize {
        self.state.lock().writers.len()
    }

    /// Writers open, being created and closing, which the cap bounds.
    #[cfg(test)]
    fn live(&self) -> usize {
        self.state.lock().live()
    }
}

impl Reservation<'_> {
    /// Put `writer` in the slot, and return the destination's writer: this
    /// one, or the one another lookup put there first, in which case this one
    /// is dropped.
    pub(crate) fn fill(mut self, destination: &str, writer: ArchiveWriter) -> SharedWriter {
        self.filled = true;
        let mut slots = self.cache.state.lock();
        slots.reserved = slots.reserved.saturating_sub(1);
        let open = if let Some(open) = slots.writers.get(destination) {
            Arc::clone(open)
        } else {
            let writer = Arc::new(Mutex::new(writer));
            slots
                .writers
                .push(destination.to_string(), Arc::clone(&writer));
            writer
        };
        self.cache.changed.notify_waiters();
        open
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.filled {
            let mut slots = self.cache.state.lock();
            slots.reserved = slots.reserved.saturating_sub(1);
            self.cache.changed.notify_waiters();
        }
    }
}

impl Drop for Closing {
    fn drop(&mut self) {
        let mut slots = self.cache.state.lock();
        slots.closing = slots.closing.saturating_sub(1);
        self.cache.changed.notify_waiters();
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.counted {
            let mut slots = self.cache.state.lock();
            slots.waiting = slots.waiting.saturating_sub(1);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use dfe_archiver_core::archive::RollingPolicy;
    use dfe_archiver_core::compression::create_compressor;
    use dfe_archiver_core::config::ArchiveConfig;
    use dfe_archiver_io::FileBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    fn writer() -> ArchiveWriter {
        ArchiveWriter::new(
            ArchiveConfig::default(),
            RollingPolicy::default(),
            create_compressor("none", 0).expect("compressor"),
            Box::new(FileBackend::new(std::env::temp_dir())),
        )
    }

    /// The writer of `destination`, filling a vacant slot, with each
    /// eviction's slot pushed onto `closing`.
    async fn open(
        cache: &Arc<WriterCache>,
        destination: &str,
        closing: &parking_lot::Mutex<Vec<(String, Closing)>>,
    ) -> SharedWriter {
        let lookup = cache
            .lookup(destination, |key, _, slot| closing.lock().push((key, slot)))
            .await;
        match lookup {
            Lookup::Open(writer) => writer,
            Lookup::Vacant(slot) => slot.fill(destination, writer()),
        }
    }

    /// A new destination with every slot taken evicts the least recently
    /// used writer and waits until its close gives the slot back.
    #[tokio::test]
    async fn a_new_writer_waits_for_an_evicted_close_to_finish() {
        let cache = Arc::new(WriterCache::new(2));
        let closing = parking_lot::Mutex::new(Vec::new());
        open(&cache, "a", &closing).await;
        open(&cache, "b", &closing).await;
        open(&cache, "a", &closing).await;

        let mut third = std::pin::pin!(open(&cache, "c", &closing));
        assert!(futures::poll!(third.as_mut()).is_pending());
        let evicted: Vec<String> = closing.lock().iter().map(|(key, _)| key.clone()).collect();
        assert_eq!(evicted, ["b"], "the least recently used goes");
        assert_eq!(cache.live(), 2, "b closing and a open fill the cap");

        closing.lock().clear();
        let Poll::Ready(_) = futures::poll!(third.as_mut()) else {
            panic!("the freed slot goes to c");
        };
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.live(), 2);
    }

    /// A slot left unfilled, because the writer could not be created, is
    /// given back.
    #[tokio::test]
    async fn an_unfilled_slot_is_given_back() {
        let cache = Arc::new(WriterCache::new(1));
        let Lookup::Vacant(slot) = cache.lookup("a", |_, _, _| {}).await else {
            panic!("an empty cache has a slot");
        };
        assert_eq!(cache.live(), 1);
        drop(slot);
        assert_eq!(cache.live(), 0);
    }

    /// Many new destinations at once never hold more writers than the cap,
    /// closing ones included, and every one gets its writer.
    #[tokio::test]
    async fn a_high_cardinality_batch_stays_under_the_cap() {
        let cap = 4;
        let cache = Arc::new(WriterCache::new(cap));
        let peak = AtomicUsize::new(0);
        let lookups = (0..64).map(|n| {
            let cache = &cache;
            let peak = &peak;
            async move {
                let destination = format!("org-{n}");
                let lookup = cache
                    .lookup(&destination, |_, _, slot| {
                        // The close finishes on its own task, a little later.
                        tokio::spawn(async move {
                            tokio::task::yield_now().await;
                            drop(slot);
                        });
                    })
                    .await;
                peak.fetch_max(cache.live(), Ordering::Relaxed);
                match lookup {
                    Lookup::Open(writer) => writer,
                    Lookup::Vacant(slot) => slot.fill(&destination, writer()),
                }
            }
        });
        let opened = futures::future::join_all(lookups).await;
        assert_eq!(opened.len(), 64);
        let peak = peak.load(Ordering::Relaxed);
        assert!(
            peak <= cap,
            "{peak} writers live at once, past the cap of {cap}"
        );
    }

    /// A lookup that stops waiting leaves no claim on an eviction behind.
    #[tokio::test]
    async fn a_dropped_lookup_stops_counting_as_waiting() {
        let cache = Arc::new(WriterCache::new(1));
        let closing = parking_lot::Mutex::new(Vec::new());
        open(&cache, "a", &closing).await;
        {
            let mut waiter = std::pin::pin!(open(&cache, "b", &closing));
            assert!(futures::poll!(waiter.as_mut()).is_pending());
        }
        assert_eq!(cache.state.lock().waiting, 0);
    }
}

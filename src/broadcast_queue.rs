use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use crate::message::Broadcast;

pub trait BroadcastQueue: Send + Sync {
    /// Adds a new broadcast message to the queue or replaces an existing one with the same key.
    ///
    /// This method enqueues a new broadcast message with the lowest retransmit count,
    /// or replaces an existing message if one with the same key already exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the broadcast message.
    /// * `broadcast` - The Broadcast message to be added to or updated in the queue.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or an error if the operation failed.
    fn upsert(&self, key: String, broadcast: Broadcast) -> Result<()>;

    /// Retrieves and removes the highest priority broadcast message from the queue.
    /// # Returns
    ///
    /// Returns `Ok(Some((Key, Broadcast)))` if a message was successfully retrieved,
    /// `Ok(None)` if the queue is empty, or an error if the operation failed.
    fn pop(&self) -> Result<Option<(String, Broadcast)>>;

    /// Checks if the queue is empty.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the queue is empty, `Ok(false)` if it's not,
    /// or an error if the operation failed.
    fn is_empty(&self) -> Result<bool>;

    /// Returns the number of messages currently in the queue.
    ///
    /// # Returns
    ///
    /// Returns `Ok(usize)` with the number of messages in the queue,
    /// or an error if the operation failed.
    fn len(&self) -> Result<usize>;

    /// Updates the cluster size.
    /// 
    /// # Arguments
    ///
    /// * `size` - The new cluster size.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or an error if the operation failed.
    fn set_cluster_size(&self, size: usize) -> Result<()>;

    /// Decrements the retransmit count of the broadcast with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the broadcast to decrement.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or an error if the operation failed.
    
    fn decrement_retransmit(&self, key: String) -> Result<()>;
}

/// Represents a broadcast message in the queue with additional metadata
#[derive(Debug, Clone, Eq, PartialEq)]
struct QueuedBroadcast {
    broadcast: Broadcast,
    retransmit_count: u32,
    id: u64,
}

impl Ord for QueuedBroadcast {
    fn cmp(&self, other: &Self) -> Ordering {
        // First we compare by retransmit count (lower count has higher priority)
        self.retransmit_count.cmp(&other.retransmit_count).reverse()
            // Then we compare by priority (lower value means higher priority)
            .then(self.broadcast.priority().cmp(&other.broadcast.priority()))
            // Finally by id (lower id has higher priority)
            .then(self.id.cmp(&other.id).reverse())
    }
}

impl PartialOrd for QueuedBroadcast {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The default implementation of the BroadcastQueue trait
#[derive(Clone)]
pub struct DefaultBroadcastQueue {
    queue: Arc<RwLock<BTreeMap<String, QueuedBroadcast>>>,
    cluster_size: Arc<RwLock<usize>>,
    max_retransmits: Arc<RwLock<u32>>,
    next_id: Arc<AtomicU64>,
}

impl DefaultBroadcastQueue {
    /// Creates a new [`DefaultBroadcastQueue`] with the given initial cluster size
    pub fn new(initial_cluster_size: usize) -> Self {
        let max_retransmits = Self::calculate_max_retransmits(initial_cluster_size);
        Self {
            queue: Arc::new(RwLock::new(BTreeMap::new())),
            cluster_size: Arc::new(RwLock::new(initial_cluster_size)),
            max_retransmits: Arc::new(RwLock::new(max_retransmits)),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Calculates the maximum number of retransmits based on the cluster size
    pub fn calculate_max_retransmits(cluster_size: usize) -> u32 {
        (cluster_size as f32).log2().ceil() as u32 + 1
    }
}

impl BroadcastQueue for DefaultBroadcastQueue {

    fn upsert(&self, key: String, broadcast: Broadcast) -> Result<()> {
        let id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);

        let queued_broadcast = QueuedBroadcast {
            broadcast,
            retransmit_count: 0,
            id,
        };

        let mut queue = self.queue.write();
        queue.insert(key, queued_broadcast);
        Ok(())
    }

    fn pop(&self) -> Result<Option<(String, Broadcast)>> {
        let mut queue = self.queue.write();
        let max_retransmits = *self.max_retransmits.read();
        let mut items: Vec<_> = queue.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        items.sort_by(|a, b| a.1.cmp(&b.1));
        // Try to find an item that hasn't reached max_retransmits
        while let Some((key, mut broadcast)) = items.pop() {
            if broadcast.retransmit_count < max_retransmits {
                broadcast.retransmit_count += 1;
                queue.insert(key.clone(), broadcast.clone());
                return Ok(Some((key, broadcast.broadcast)));
            } else {
                // Remove items that have reached max retransmits
                queue.remove(&key);
            }
        }

        Ok(None)
    }

    fn is_empty(&self) -> Result<bool> {
        let queue = self.queue.read();
        Ok(queue.is_empty())
    }

    fn len(&self) -> Result<usize> {
        let queue = self.queue.read();
        Ok(queue.len())
    }

    fn decrement_retransmit(&self, key: String) -> Result<()> {
        let mut queue = self.queue.write();
        if let Some(broadcast) = queue.get_mut(&key) {
            if broadcast.retransmit_count > 0 {
                broadcast.retransmit_count -= 1;
            }
        }
        Ok(())
    }

    fn set_cluster_size(&self, size: usize) -> Result<()> {
        let mut cluster_size = self.cluster_size.write();
        *cluster_size = size;
        let new_max_retransmits = Self::calculate_max_retransmits(size);
        let mut max_retransmits = self.max_retransmits.write();
        *max_retransmits = new_max_retransmits;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    #[test]
    fn test_push_pop() -> Result<()> {
        let queue = Arc::new(DefaultBroadcastQueue::new(5));
        let broadcast1 = Broadcast::Suspect { incarnation: 1, member: "node1".to_string() };
        let broadcast2 = Broadcast::Suspect { incarnation: 2, member: "node2".to_string() };

        queue.upsert("node1".to_string(), broadcast1.clone())?;
        queue.upsert("node2".to_string(), broadcast2.clone())?;

        assert_eq!(queue.pop()?.map(|(_, b)| b), Some(broadcast1.clone()));
        assert_eq!(queue.pop()?.map(|(_, b)| b), Some(broadcast2.clone()));
        assert_eq!(queue.pop()?.map(|(_, b)| b), Some(broadcast1.clone()));
        Ok(())
    }

    #[test]
    fn test_is_empty_and_len() -> Result<()> {
        let queue = Arc::new(DefaultBroadcastQueue::new(5));
        assert!(queue.is_empty()?);
        assert_eq!(queue.len()?, 0);

        queue.upsert("node1".to_string(), Broadcast::Suspect { incarnation: 1, member: "node1".to_string() })?;
        assert!(!queue.is_empty()?);
        assert_eq!(queue.len()?, 1);

        queue.pop()?;
        assert!(!queue.is_empty()?);
        assert_eq!(queue.len()?, 1);
        Ok(())
    }

    #[test]
    fn test_priority() -> Result<()> {
        let queue = Arc::new(DefaultBroadcastQueue::new(5));
        queue.upsert("node1".to_string(),Broadcast::Alive { incarnation: 1, member: "node1".to_string() })?;
        queue.upsert("node2".to_string(), Broadcast::Confirm { incarnation: 2, member: "node2".to_string() })?;
        queue.upsert("node3".to_string(), Broadcast::Suspect { incarnation: 3, member: "node3".to_string() })?;

        assert!(matches!(queue.pop()?.map(|(_, b)| b), Some(Broadcast::Confirm { .. })));
        assert!(matches!(queue.pop()?.map(|(_, b)| b), Some(Broadcast::Suspect { .. })));
        assert!(matches!(queue.pop()?.map(|(_, b)| b), Some(Broadcast::Alive { .. })));
        Ok(())
    }

    #[test]
    fn test_max_retransmits() -> Result<()> {
        let queue = Arc::new(DefaultBroadcastQueue::new(8));
        queue.upsert("node1".to_string(), Broadcast::Suspect { incarnation: 1, member: "node1".to_string() })?;

        for _ in 0..4 {
            assert!(matches!(queue.pop()?.map(|(_, b)| b), Some(Broadcast::Suspect { .. })));
        }
        assert!(queue.pop()?.is_none());
        Ok(())
    }

    #[test]
    fn test_concurrent_access() -> Result<()> {
        let queue = Arc::new(DefaultBroadcastQueue::new(5));
        let threads: Vec<_> = (0..10)
            .map(|i| {
                let queue = Arc::clone(&queue);
                thread::spawn(move || -> Result<()> {
                    queue.upsert("node1".to_string(), Broadcast::Suspect { incarnation: i, member: format!("node{}", i) })?;
                    queue.pop()?;
                    Ok(())
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap()?;
        }

        assert!(!queue.is_empty()?);
        assert!(queue.len()? > 0);
        Ok(())
    }
}
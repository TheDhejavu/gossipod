use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use dashmap::DashMap;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::node::{Node, NodeMetadata, NodePriority};
use crate::state::NodeState;

#[derive(Debug)]
pub(crate) struct Membership<M: NodeMetadata> {
    nodes: Arc<DashMap<String, Node<M>>>,
    nodes_priority_queue: Arc<RwLock<BinaryHeap<Reverse<NodePriority<M>>>>>,
    gossip_index: AtomicUsize,
    probe_index: AtomicUsize,
}

#[derive(Debug, PartialEq)]
pub enum MergeAction {
    Added,
    Updated,
    Unchanged,
    Removed,
    Ignored,
}

#[derive(Debug)]
pub(crate) struct MergeResult {
    pub action: MergeAction,
    pub old_state: Option<NodeState>,
    pub new_state: NodeState,
}

impl<M: NodeMetadata> Membership<M> {
    /// Creates a new, empty Membership list.
    pub(crate) fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
            nodes_priority_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            gossip_index: AtomicUsize::new(0),
            probe_index: AtomicUsize::new(0),
        }
    }

    /// Adds a new node to the membership list.
    pub(crate) fn add_node(&self, node: Node<M>) -> Result<()> {
        let name = node.name.clone();
        self.nodes.insert(name.clone(), node);

        // Here we are setting last_piggybacked to UNIX_EPOCH (January 1, 1970), 
        // and we're effectively saying this node has never been probed before by
        // giving it the oldest possible timestamp.
        let mut priority_queue = self.nodes_priority_queue.write();
        priority_queue.push(Reverse(NodePriority {
            last_piggybacked: UNIX_EPOCH,
            name: name.clone(),
            _marker: std::marker::PhantomData,
        }));
        Ok(())
    }

    /// Removes a node from the membership list by its name.
    pub(crate) fn remove_node(&self, name: &str) -> Result<Option<Node<M>>> {
        let removed = self.nodes.remove(name).map(|(_, node)| node);
        if removed.is_some() {
            let mut priority_queue = self.nodes_priority_queue.write();
            priority_queue.retain(|Reverse(np)| np.name != name);
        }
        Ok(removed)
    }

    // Iterator for least recently piggybacked nodes
    pub fn least_recently_piggybacked_iter(&self) -> LeastRecentlyPiggybackedIter<M> {
        LeastRecentlyPiggybackedIter::new(self)
    }

    /// Retrieves a node from the membership list by its name.
    pub(crate) fn get_node(&self, name: &str) -> Result<Option<Node<M>>> {
        Ok(self.nodes.get(name).map(|r| r.value().clone()))
    }

    /// Retrieves all nodes from the membership list, optionally excluding nodes based on a predicate.
    pub(crate) fn get_nodes<F>(&self, exclude: Option<F>) -> Result<Vec<Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        Ok(self.nodes.iter()
            .filter(|r| exclude.as_ref().map_or(true, |f| !f(r.value())))
            .map(|r| r.value().clone())
            .collect())
    }

    /// Retrieves all nodes from the membership list without any exclusion.
    pub(crate) fn get_all_nodes(&self) -> Result<Vec<Node<M>>> {
        self.get_nodes::<fn(&Node<M>) -> bool>(None)
    }

    /// Selects the next node for gossip using a round-robin approach, with an optional exclusion predicate.
    pub(crate) fn next_gossip_node<F>(&self, exclude: Option<F>) -> Result<Option<Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        let eligible_nodes: Vec<_> = self.nodes.iter()
            .filter(|r| exclude.as_ref().map_or(true, |f| !f(r.value())))
            .map(|r| r.value().clone())
            .collect();

        if eligible_nodes.is_empty() {
            return Ok(None);
        }

        let index = self.gossip_index.fetch_add(1, Ordering::Relaxed) % eligible_nodes.len();
        Ok(Some(eligible_nodes[index].clone()))
    }

    /// Selects the next node for probe using a round-robin approach, with an optional exclusion predicate.
    pub(crate) fn next_probe_node<F>(&self, exclude: Option<F>) -> Result<Option<Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        let eligible_nodes: Vec<_> = self.nodes.iter()
            .filter(|r| exclude.as_ref().map_or(true, |f| !f(r.value())))
            .map(|r| r.value().clone())
            .collect();

        if eligible_nodes.is_empty() {
            return Ok(None);
        }

        let index = self.probe_index.fetch_add(1, Ordering::Relaxed) % eligible_nodes.len();
        Ok(Some(eligible_nodes[index].clone()))
    }
    
   /// Selects a specified number of nodes for gossip from the membership list using round-robin,
    /// with an optional exclusion predicate.
    pub(crate) fn select_random_gossip_nodes<F>(&self, count: usize, exclude: Option<F>) -> Result<Vec<Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        let eligible_nodes: Vec<_> = self.nodes.iter()
            .filter(|r| exclude.as_ref().map_or(true, |f| !f(r.value())))
            .map(|r| r.value().clone())
            .collect();

        let mut selected = HashSet::new();
        let mut result = Vec::new();
        let max_attempts = eligible_nodes.len() * 2;
    
        for _ in 0..max_attempts {
            if selected.len() >= count {
                break;
            }
            let index = self.gossip_index.fetch_add(1, Ordering::Relaxed) % eligible_nodes.len();
            let node = &eligible_nodes[index];
            if selected.insert(node.name.clone()) {
                result.push(node.clone());
            }
        }
    
        Ok(result)
    }

    /// Selects a specified number of nodes for probe from the membership list using round-robin,
    /// with an optional exclusion predicate.
    pub(crate) fn select_random_probe_nodes<F>(&self, count: usize, exclude: Option<F>) -> Result<Vec<Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        let eligible_nodes: Vec<_> = self.nodes.iter()
            .filter(|r| exclude.as_ref().map_or(true, |f| !f(r.value())))
            .map(|r| r.value().clone())
            .collect();

            if eligible_nodes.is_empty() {
                return Ok(Vec::new());
            }
    
        let mut selected = HashSet::new();
        let mut result = Vec::new();
        let max_attempts = eligible_nodes.len() * 2;
    
        for _ in 0..max_attempts {
            if selected.len() >= count {
                break;
            }
            let index = self.probe_index.fetch_add(1, Ordering::Relaxed) % eligible_nodes.len();
            let node = &eligible_nodes[index];
            if selected.insert(node.name.clone()) {
                result.push(node.clone());
            }
        }
    
        Ok(result)
    }

    /// Returns the number of active nodes in the membership list.
    pub(crate) fn total_active(&self) -> Result<usize> {
        Ok(self.nodes.iter().filter(|r| r.value().is_alive()).count())
    }

    /// Returns the total number of nodes in the membership list.
    pub(crate) fn len(&self) -> Result<usize> {
        Ok(self.nodes.len())
    }

    /// Checks if the membership list is empty.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        Ok(self.nodes.is_empty())
    }
    
    /// Advances the state of a node in the membership list.
    pub(crate) fn advance_node_state(&self, node_name: &str) -> Result<()> {
        self.nodes.get_mut(node_name)
            .map(|mut node| node.advance_state())
            .ok_or_else(|| anyhow!("node not found: {}", node_name))
    }

    /// Merges the state of another node into the membership list.
    /// If the node already exists, its state is updated. If it doesn't exist, it's added to the list.
    pub(crate) fn merge(&self, other_node: &Node<M>) -> Result<MergeResult> {
        if let Some(mut entry) = self.nodes.get_mut(&other_node.name) {
            let existing_node = entry.value_mut();
            let old_state = existing_node.state();
            let changed = existing_node.merge(other_node)?;
            let new_state = existing_node.state();

            if changed && matches!(new_state, NodeState::Leaving | NodeState::Left) {
                drop(entry);
                
                self.nodes.remove(&other_node.name);
                let mut priority_queue = self.nodes_priority_queue.write();
                priority_queue.retain(|Reverse(np)| np.name != other_node.name);
                return Ok(MergeResult {
                    action: MergeAction::Removed,
                    old_state: Some(old_state),
                    new_state,
                });
            }
            
            Ok(MergeResult {
                action: if changed { MergeAction::Updated } else { MergeAction::Unchanged },
                old_state: Some(old_state),
                new_state,
            })
        } else {
            if matches!(other_node.state(), NodeState::Leaving | NodeState::Left) {
                return Ok(MergeResult {
                    action: MergeAction::Ignored,
                    old_state: None,
                    new_state: other_node.state(),
                });
            }

            self.nodes.insert(other_node.name.clone(), other_node.clone());
            let mut priority_queue = self.nodes_priority_queue.write();
            priority_queue.push(Reverse(NodePriority {
                last_piggybacked: UNIX_EPOCH,
                name: other_node.name.clone(),
                _marker: std::marker::PhantomData,
            }));
            Ok(MergeResult {
                action: MergeAction::Added,
                old_state: None,
                new_state: other_node.state(),
            })
        }
    }
}

pub struct LeastRecentlyPiggybackedIter<'a, M: NodeMetadata> {
    membership: &'a Membership<M>,
    piggybacked_state: std::collections::HashSet<String>,
    iteration_start_time: SystemTime,
}

impl<'a, M: NodeMetadata> LeastRecentlyPiggybackedIter<'a, M> {
    fn new(membership: &'a Membership<M>) -> Self {
        Self {
            membership,
            piggybacked_state: std::collections::HashSet::new(),
            iteration_start_time: SystemTime::now(),
        }
    }
}

impl<'a, M: NodeMetadata> Iterator for LeastRecentlyPiggybackedIter<'a, M> {
    type Item = Node<M>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut priority_queue = self.membership.nodes_priority_queue.write();
        while let Some(Reverse(node_priority)) = priority_queue.peek() {
            if node_priority.last_piggybacked > self.iteration_start_time {
                // We've gone through all nodes probed before this iteration started
                return None;
            }

            if let Some(node) = self.membership.nodes.get(&node_priority.name) {
                if !self.piggybacked_state.contains(&node.name) {
                    self.piggybacked_state.insert(node.name.clone());

                    let mut removed_node_priority = None;
                    priority_queue.retain(|Reverse(np)| {
                        if np.name == node.name {
                            removed_node_priority = Some(np.clone());
                            false
                        } else {
                            true
                        }
                    });

                    // If we found and removed the entry, add it back with updated time
                    if let Some(mut node_priority) = removed_node_priority {
                        node_priority.last_piggybacked = SystemTime::now();
                        priority_queue.push(Reverse(node_priority));
                    }
                    return Some(node.value().clone());
                }
            }
        }
        None
    }
}
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use crate::DefaultMetadata;

    use super::*;

    #[test]
    fn test_merge_add_node() {
        let membership = Membership::new();
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0, DefaultMetadata::default());
        node.update_state(NodeState::Alive).expect("unable to update node state");

        let result = membership.merge(&node).unwrap();
        assert_eq!(result.action, MergeAction::Added);
        assert_eq!(result.old_state, None);
        assert_eq!(result.new_state, NodeState::Alive);
    }

    #[test]
    fn test_merge_ignore_leaving_node() {
        let membership = Membership::new();
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0, DefaultMetadata::default());
        node.update_state(NodeState::Leaving).expect("unable to update node state");

        let result = membership.merge(&node).unwrap();
        assert_eq!(result.action, MergeAction::Ignored);
        assert_eq!(result.old_state, None);
        assert_eq!(result.new_state, NodeState::Leaving);
    }

    #[test]
    fn test_merge_remove_leaving_node() {
        let membership = Membership::new();
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0, DefaultMetadata::default());
        node.update_state(NodeState::Alive).expect("unable to update node state");
        membership.add_node(node.clone()).expect("unable to add node");

        let mut new_node = node.clone();
        new_node.update_state(NodeState::Leaving).expect("unable to update node state");
        
        let result = membership.merge(&new_node).unwrap();
        assert_eq!(result.action, MergeAction::Removed);
        assert_eq!(result.old_state, Some(NodeState::Alive));
        assert_eq!(result.new_state, NodeState::Leaving);
    }
}
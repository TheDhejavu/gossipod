use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Result};
use log::debug;
use crate::node::{Node, NodeMetadata, NodePriority};
use crate::state::NodeState;

#[derive(Debug)]
pub(crate) struct Membership<M: NodeMetadata> {
    nodes: Arc<RwLock<HashMap<String, Node<M>>>>,
    current_index: AtomicUsize,
    nodes_priority_queue: Arc<RwLock<BinaryHeap<Reverse<NodePriority<M>>>>>,
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
            nodes: Arc::new(RwLock::new(HashMap::new())),
            nodes_priority_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            current_index: AtomicUsize::new(0),
        }
    }

    /// Adds a new node to the membership list.
    pub(crate) fn add_node(&self, node: Node<M>) -> Result<()> {
        self.write_operation(|nodes, nodes_priority_queue| {
            let name = node.name.clone();
            nodes.insert(name.clone(), node);

            // Here we are setting last_piggybacked to UNIX_EPOCH (January 1, 1970), 
            // and we're effectively saying this node has never been probed before by
            // giving it the oldest possible timestamp.
            nodes_priority_queue.push(Reverse(NodePriority {
                last_piggybacked: UNIX_EPOCH,
                name: name.clone(),
                _marker: std::marker::PhantomData,
            }));
            Ok(())
        })
    }

    /// Removes a node from the membership list by its name.
    pub(crate) fn remove_node(&self, name: &str) -> Result<Option<Node<M>>> {
        self.write_operation(|nodes, nodes_priority_queue| {
            let removed = nodes.remove(name);
            nodes_priority_queue.retain(|Reverse(np)| np.name != name);
            Ok(removed)
        })
    }

    // Iterator for least recently piggybacked nodes
    pub fn least_recently_piggybacked_iter(&self) -> LeastRecentlyPiggybackedIter<M> {
        LeastRecentlyPiggybackedIter::new(self)
    }

    /// Retrieves a node from the membership list by its name.
    pub(crate) fn get_node(&self, name: &str) -> Result<Option<Node<M>>> {
        self.read_operation(|nodes, _| Ok(nodes.get(name).cloned()))
    }

    /// Retrieves all nodes from the membership list, optionally excluding nodes based on a predicate.
    pub(crate) fn get_nodes<F>(&self, exclude: Option<F>) -> Result<Vec<Node<M>>>
    where
        F: Fn(& Node<M>) -> bool,
    {
        self.read_operation(|nodes, _| {
            Ok(nodes.values()
                .filter(|n| exclude.as_ref().map_or(true, |f| !f(n)))
                .cloned()
                .collect())
        })
    }

    /// Retrieves all nodes from the membership list without any exclusion.
    pub(crate) fn get_all_nodes(&self) -> Result<Vec< Node<M>>> {
        self.get_nodes::<fn(& Node<M>) -> bool>(None)
    }

    /// Selects the next node for gossip using a round-robin approach, with an optional exclusion predicate.
    pub(crate) fn next_node<F>(&self, exclude: Option<F>) -> Result<Option< Node<M>>>
    where
        F: Fn(& Node<M>) -> bool,
    {
        self.read_operation(|nodes, _| {
            let mut active_nodes: Vec<_> = nodes.values()
                .filter(|n| n.is_alive() && exclude.as_ref().map_or(true, |f| !f(n)))
                .collect();
            
            if active_nodes.is_empty() {
                return Ok(None);
            }

            active_nodes.sort_by_key(|node| &node.name);
            let index = self.current_index.fetch_add(1, Ordering::Relaxed) % active_nodes.len();
            Ok(Some(active_nodes[index].clone()))
        })
    }

    /// Selects a specified number of random nodes from the membership list, with an optional exclusion predicate.
    pub(crate) fn select_random_nodes<F>(&self, count: usize, exclude: Option<F>) -> Result<Vec< Node<M>>>
    where
        F: Fn(&Node<M>) -> bool,
    {
        self.read_operation(|nodes, _| {
            let eligible_nodes: Vec<_> = nodes.values()
                .filter(|n| n.is_alive() && exclude.as_ref().map_or(true, |f| !f(n)))
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
                let index = self.current_index.fetch_add(1, Ordering::Relaxed) % eligible_nodes.len();
                let node = eligible_nodes[index];
                if selected.insert(node.name.clone()) {
                    result.push(node.clone());
                }
            }

            Ok(result)
        })
    }

    /// Returns the number of active nodes in the membership list.
    pub(crate) fn total_active(&self) -> Result<usize> {
        self.read_operation(|nodes, _| Ok(nodes.values().filter(|node| node.is_alive()).count()))
    }

    /// Returns the total number of nodes in the membership list.
    pub(crate) fn len(&self) -> Result<usize> {
        self.read_operation(|nodes, _| Ok(nodes.len()))
    }

    /// Checks if the membership list is empty.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }
    
    /// Advances the state of a node in the membership list.
    pub(crate) fn advance_node_state(&self, node_name: &str) -> Result<()> {
        self.write_operation(|nodes, _| {
            nodes.get_mut(node_name)
                .map(|node| node.advance_state())
                .ok_or_else(|| anyhow!("node not found: {}", node_name))
        })
    }

    /// Merges the state of another node into the membership list.
    /// If the node already exists, its state is updated. If it doesn't exist, it's added to the list.
    pub(crate) fn merge(&self, other_node: & Node<M>) -> Result<MergeResult> {
        self.write_operation(|nodes, probe_priority| {
            if let Some(existing_node) = nodes.get_mut(&other_node.name) {
                let old_state = existing_node.state();
                let changed = existing_node.merge(other_node)?;
                let new_state = existing_node.state();

                if changed && matches!(new_state, NodeState::Leaving | NodeState::Left) {
                    nodes.remove(&other_node.name);
                    probe_priority.retain(|Reverse(np)| np.name != other_node.name);
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

                nodes.insert(other_node.name.clone(), other_node.clone());
                probe_priority.push(Reverse(NodePriority {
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
        })
    }

    /// Helper method for performing read operations on the nodes map.
    fn read_operation<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&HashMap<String, Node<M>>, &BinaryHeap<Reverse<NodePriority<M>>>) -> Result<R>,
    {
        let nodes = self.nodes.read().map_err(|e| anyhow!("unable to acquire nodes lock: {}", e))?;
        let nodes_priority_queue = self.nodes_priority_queue.read().map_err(|e| anyhow!("unable to acquire probe_priority lock: {}", e))?;
        f(&nodes, &nodes_priority_queue)
    }

    /// Helper method for performing write operations on the nodes map.
    fn write_operation<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut HashMap<String, Node<M>>, &mut BinaryHeap<Reverse<NodePriority<M>>>) -> Result<R>,
    {
        let mut nodes = self.nodes.write().map_err(|e| anyhow!("unable to acquire nodes lock: {}", e))?;
        let mut nodes_priority_queue = self.nodes_priority_queue.write().map_err(|e| anyhow!("unable to acquire probe_priority lock: {}", e))?;
        f(&mut nodes, &mut nodes_priority_queue)
    }
}


pub struct LeastRecentlyPiggybackedIter<'a, M: NodeMetadata> {
    membership: &'a Membership<M>,
    piggybacked_state: HashSet<String>,
    iteration_start_time: SystemTime,
}

impl<'a, M: NodeMetadata> LeastRecentlyPiggybackedIter<'a, M> {
    fn new(membership: &'a Membership<M>) -> Self {
        Self {
            membership,
            piggybacked_state: HashSet::new(),
            iteration_start_time: SystemTime::now(),
        }
    }
}

impl<'a, M: NodeMetadata> Iterator for LeastRecentlyPiggybackedIter<'a, M> {
    type Item = Node<M>;

    fn next(&mut self) -> Option<Self::Item> {
        self.membership.write_operation(|nodes, nodes_priority_queue| {
            while let Some(Reverse(node_priority)) = nodes_priority_queue.peek() {
                if node_priority.last_piggybacked > self.iteration_start_time {
                    // We've gone through all nodes probed before this iteration started
                    return Ok(None);
                }

                if let Some(node) = nodes.get(&node_priority.name) {
                    if !self.piggybacked_state.contains(&node.name) {
                        self.piggybacked_state.insert(node.name.clone());

                        let mut removed_node_priority = None;
                        nodes_priority_queue.retain(|Reverse(np)| {
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
                            nodes_priority_queue.push(Reverse(node_priority));
                        }
                        return Ok(Some(node.clone()));
                    }
                }
                
                // Remove this entry as it's either not in nodes or not alive
                nodes_priority_queue.pop();
            }
            Ok(None)
        }).unwrap_or(None)
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
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0,DefaultMetadata::new());
        node.update_state(NodeState::Alive).expect("unable to update node state");

        let result = membership.merge(&node).unwrap();
        assert_eq!(result.action, MergeAction::Added);
        assert_eq!(result.old_state, None);
        assert_eq!(result.new_state, NodeState::Alive);
    }

    #[test]
    fn test_merge_ignore_leaving_node() {
        let membership = Membership::new();
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0,DefaultMetadata::new());
        node.update_state(NodeState::Leaving).expect("unable to update node state");

        let result = membership.merge(&node).unwrap();
        assert_eq!(result.action, MergeAction::Ignored);
        assert_eq!(result.old_state, None);
        assert_eq!(result.new_state, NodeState::Leaving);
    }

    #[test]
    fn test_merge_remove_leaving_node() {
        let membership = Membership::new();
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0,DefaultMetadata::new());
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

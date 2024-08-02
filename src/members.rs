use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use anyhow::{anyhow, Result};
use crate::node::{Node, NodeMetadata};
use crate::state::NodeState;

#[derive(Debug)]
pub(crate) struct MembershipList<M: NodeMetadata> {
    nodes: Arc<RwLock<HashMap<String, Node<M>>>>,
    current_index: AtomicUsize,
}

#[derive(Debug, PartialEq)]
pub(crate) enum MergeAction {
    Added,
    Updated,
    Unchanged,
}

#[derive(Debug)]
pub(crate) struct MergeResult {
    pub action: MergeAction,
    pub old_state: Option<NodeState>,
    pub new_state: NodeState,
}

impl<M: NodeMetadata> MembershipList<M> {
    /// Creates a new, empty Membership list.
    pub(crate) fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_index: AtomicUsize::new(0),
        }
    }

    /// Adds a new node to the membership list.
    pub(crate) fn add_node(&self, node: Node<M>) -> Result<()> {
        self.write_operation(|nodes| {
            nodes.insert(node.name.clone(), node);
            Ok(())
        })
    }

    /// Removes a node from the membership list by its name.
    pub(crate) fn remove_node(&self, name: &str) -> Result<Option<Node<M>>> {
        self.write_operation(|nodes| Ok(nodes.remove(name)))
    }

    /// Retrieves a node from the membership list by its name.
    pub(crate) fn get_node(&self, name: &str) -> Result<Option<Node<M>>> {
        self.read_operation(|nodes| Ok(nodes.get(name).cloned()))
    }

    /// Retrieves all nodes from the membership list, optionally excluding nodes based on a predicate.
    pub(crate) fn get_nodes<F>(&self, exclude: Option<F>) -> Result<Vec<Node<M>>>
    where
        F: Fn(& Node<M>) -> bool,
    {
        self.read_operation(|nodes| {
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
        self.read_operation(|nodes| {
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
        F: Fn(& Node<M>) -> bool,
    {
        self.read_operation(|nodes| {
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
        self.read_operation(|nodes| Ok(nodes.values().filter(|node| node.is_alive()).count()))
    }

    /// Returns the total number of nodes in the membership list.
    pub(crate) fn len(&self) -> Result<usize> {
        self.read_operation(|nodes| Ok(nodes.len()))
    }

    /// Checks if the membership list is empty.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }
    
    /// Advances the state of a node in the membership list.
    pub(crate) fn advance_node_state(&self, node_name: &str) -> Result<()> {
        self.write_operation(|nodes| {
            nodes.get_mut(node_name)
                .map(|node| node.advance_state())
                .ok_or_else(|| anyhow!("node not found: {}", node_name))
        })
    }

    /// Merges the state of another node into the membership list.
    /// If the node already exists, its state is updated. If it doesn't exist, it's added to the list.
    pub(crate) fn merge(&self, other_node: & Node<M>) -> Result<MergeResult> {
        self.write_operation(|nodes| {
            if let Some(existing_node) = nodes.get_mut(&other_node.name) {
                let old_state = existing_node.state();
                let changed = existing_node.merge(other_node)?;
                let new_state = existing_node.state();
                
                Ok(MergeResult {
                    action: if changed { MergeAction::Updated } else { MergeAction::Unchanged },
                    old_state: Some(old_state),
                    new_state,
                })
            } else {
                nodes.insert(other_node.name.clone(), other_node.clone());
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
        F: FnOnce(&HashMap<String, Node<M>>) -> Result<R>,
    {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("unable to acquire lock: {}", e))?;
        f(&nodes)
    }

    /// Helper method for performing write operations on the nodes map.
    fn write_operation<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut HashMap<String, Node<M>>) -> Result<R>,
    {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow!("unable to acquire lock: {}", e))?;
        f(&mut nodes)
    }
}
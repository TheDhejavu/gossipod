use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use anyhow::{anyhow, Result};
use crate::node::Node;
use crate::state::NodeState;

#[derive( Debug)]
pub(crate) struct MembershipList {
    nodes: Arc<RwLock<HashMap<String, Node>>>,
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

impl MembershipList {
    // Instantiate new members
    pub(crate) fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_index: AtomicUsize::new(0),
        }
    }

    // adds a new node to the membershiplist 
    pub(crate) fn add_node(&self, node: Node) -> Result<()> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {}", e))?;
        
        nodes.insert(node.name.clone(), node);
        Ok(())
    }

    // removes a new node from the membershiplist
    pub(crate) fn remove_node(&self, name: String) -> Result<Option<Node>> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        
        Ok(nodes.remove(&name))
    }

    // get a new node from the membershiplist
    pub(crate) fn get_node(&self, name: String) -> Result<Option<Node>> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.get(&name).cloned())
    }

    // get nodes from the membershiplist with exclusion
    pub(crate) fn get_nodes<F>(&self, exclude: Option<F>) -> Result<Vec<Node>>  
    where
        F: Fn(&Node) -> bool,
    {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.values().filter(|n| exclude.as_ref().map_or(true, |f| !f(n))).cloned().collect())
    }

    // get all nodes from the membership list without exclusion
    pub(crate) fn get_all_nodes(&self) -> Result<Vec<Node>> {
        self.get_nodes::<fn(&Node) -> bool>(None)
    }

    /// Select a node for gossip using a round-robin approach, with a custom exclusion function
    pub(crate) fn next_node<F>(&self, exclude: Option<F>) -> Result<Option<Node>>
    where
        F: Fn(&Node) -> bool,
    {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        let mut active_nodes: Vec<_> = nodes.values()
            .filter(|n| n.is_alive() && exclude.as_ref().map_or(true, |f| !f(n)))
            .collect();
        
        let len = active_nodes.len();
        if len == 0 {
            return Ok(None);
        }

        active_nodes.sort_by_key(|node| &node.name);

        let index = self.current_index.fetch_add(1, Ordering::Relaxed) % len;
        Ok(Some(active_nodes[index].clone()))
    }

    pub(crate) fn select_random_nodes<F>(&self, count: usize, exclude: Option<F>) -> Result<Vec<Node>> where
        F: Fn(&Node) -> bool,
    {
        
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        let mut eligible_nodes: Vec<_> = nodes.values()
            .filter(|n| n.is_alive() && exclude.as_ref().map_or(true, |f| !f(n)))
            .collect();

        eligible_nodes.sort_by_key(|node| &node.name);
        let len = eligible_nodes.len();

        if len == 0 {
            return Ok(Vec::new());
        }

        let mut selected = HashSet::new();
        let mut result = Vec::new();
        let mut attempts = 0;
        let max_attempts = len * 2;

        while selected.len() < count && attempts < max_attempts {
            let index = self.current_index.fetch_add(1, Ordering::Relaxed) % len;
            let node = eligible_nodes[index];
            
            if selected.insert(node.name.clone()) {
                result.push(node.clone());
            }
            attempts += 1;
        }

        Ok(result)
    }

    /// Returns the number of nodes in the membership list that is active
    pub(crate) fn total_active(&self) -> Result<usize> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.values().filter(|node| node.is_alive()).count())
    }

    /// Returns the number of nodes in the membership list.
    pub(crate) fn len(&self) -> Result<usize> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.len())
    }

    /// Checks if the membership list is empty.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
    
    /// Move a node to its next state
    pub(crate) fn advance_node_state(&self, node: &Node) -> Result<()> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        
        if let Some(node) = nodes.get_mut(&node.name) {
            node.advance_state();
            Ok(())
        } else {
            Err(anyhow!("Node not found: {}", node.name))
        }
    }

    /// Merges the state of another node into the membership list.
    /// If the node already exists, its state is updated.
    /// If the node doesn't exist, it's added to the list.
    /// Returns a MergeResult detailing the action taken and state changes.
    pub(crate) fn merge(&self, other_node: &Node) -> Result<MergeResult> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        
        if let Some(existing_node) = nodes.get_mut(&other_node.name) {
            let old_state = existing_node.state();
            let changed = existing_node.merge(other_node);
            let new_state = existing_node.state();
            
            if changed {
                Ok(MergeResult {
                    action: MergeAction::Updated,
                    old_state: Some(old_state),
                    new_state,
                })
            } else {
                Ok(MergeResult {
                    action: MergeAction::Unchanged,
                    old_state: Some(old_state),
                    new_state,
                })
            }
        } else {
            nodes.insert(other_node.name.clone(), other_node.clone());
            Ok(MergeResult {
                action: MergeAction::Added,
                old_state: None,
                new_state: other_node.state(),
            })
        }
    }

}
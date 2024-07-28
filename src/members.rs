use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use anyhow::Result;
use crate::node::Node;


/*
 *
 * ===== Members =====
 *
 */
#[derive(Clone)]
pub struct Members {
    nodes: Arc<RwLock<HashMap<String, Node>>>,
}

impl Members {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_node(&self, node: Node) -> Result<()> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {}", e))?;
        
        nodes.insert(node.name.clone(), node);
        Ok(())
    }

    pub fn remove_node(&self, name: String) -> Result<Option<Node>> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {}", e))?;
        
        Ok(nodes.remove(&name))
    }

    pub fn get_node(&self, name: String) -> Result<Option<Node>> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.get(&name).cloned())
    }

    pub fn get_all_nodes(&self) -> Result<Vec<Node>> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.values().cloned().collect())
    }
}
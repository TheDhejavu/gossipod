use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use anyhow::Result;
use crate::node::Node;


#[derive(Clone)]
pub struct Members {
    nodes: Arc<RwLock<HashMap<IpAddr, Node>>>,
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
        
        nodes.insert(node.ip_addr, node);
        Ok(())
    }

    pub fn remove_node(&self, ip_addr: &IpAddr) -> Result<Option<Node>> {
        let mut nodes = self.nodes.write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {}", e))?;
        
        Ok(nodes.remove(ip_addr))
    }

    pub fn get_node(&self, ip_addr: &IpAddr) -> Result<Option<Node>> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.get(ip_addr).cloned())
    }

    pub fn get_all_nodes(&self) -> Result<Vec<Node>> {
        let nodes = self.nodes.read()
            .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {}", e))?;
        
        Ok(nodes.values().cloned().collect())
    }
}
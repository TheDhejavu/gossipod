use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::{Context as _, Result};
use serde::{Serialize, Deserialize};
use sysinfo::System;
use std::cmp::Ordering;
use std::any::{type_name, TypeId};
use crate::state::NodeState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeStatus {
    incarnation: u64,
    pub state: NodeState,
    last_updated: SystemTime,
}

impl NodeStatus {
    /// Creates a new `NodeStatus` with default values.
    fn new(incarnation: u64) -> Self {
        let now = SystemTime::now();
        Self {
            incarnation,
            state: NodeState::Unknown,
            last_updated: now,
        }
    }

    /// Updates the state of the Node and records the time of change.
    pub(crate) fn update_state(&mut self, new_state: NodeState) {
        self.state = new_state;
        self.last_updated = SystemTime::now();
    }

    /// Increments the incarnation number and updates the last change time.
    /// this should only be used in test.
    fn increment_incarnation(&mut self) {
        self.incarnation += 1;
        self.last_updated = SystemTime::now();
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DefaultMetadata {
    hostname: String,
    os_name: String,
    os_version: String,
    cpu_count: usize,
    total_memory: u64,
    timestamp: u64,
}

impl NodeMetadata for DefaultMetadata {}

impl DefaultMetadata {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        let hostname = System::host_name().unwrap_or_else(|| "Unknown".to_string());
        let os_name = System::name().unwrap_or_else(|| "Unknown".to_string());
        let os_version = System::os_version().unwrap_or_else(|| "Unknown".to_string());
        let cpu_count = sys.cpus().len();
        let total_memory = sys.total_memory();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unable to get startup timestamp")
            .as_secs();

        Self {
            hostname,
            os_name,
            os_version,
            cpu_count,
            total_memory,
            timestamp,
        }
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn os_name(&self) -> &str {
        &self.os_name
    }

    pub fn os_version(&self) -> &str {
        &self.os_version
    }

    pub fn cpu_count(&self) -> usize {
        self.cpu_count
    }

    pub fn total_memory(&self) -> u64 {
        self.total_memory
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl PartialEq for DefaultMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.hostname == other.hostname &&
        self.os_name == other.os_name &&
        self.os_version == other.os_version &&
        self.cpu_count == other.cpu_count &&
        self.total_memory == other.total_memory &&
        self.timestamp == other.timestamp
    }
}

impl Eq for DefaultMetadata {}

impl Hash for DefaultMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hostname.hash(state);
        self.os_name.hash(state);
        self.os_version.hash(state);
        self.cpu_count.hash(state);
        self.total_memory.hash(state);
        self.timestamp.hash(state);
    }
}

pub trait NodeMetadata: Send + Sync + 'static + Clone + Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + Hash {
    fn type_name(&self) -> &'static str {
        type_name::<Self>()
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        #[derive(Serialize)]
        struct Wrapper<T> {
            type_name: &'static str,
            data: T,
        }

        let wrapper = Wrapper {
            type_name: self.type_name(),
            data: self,
        };

        bincode::serialize(&wrapper).context("failed to serialize metadata to bytes")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        #[derive(Deserialize)]
        struct Wrapper<T> {
            type_name: String,
            data: T,
        }

        let wrapper: Wrapper<Self> = bincode::deserialize(bytes)
            .context("failed to deserialize metadata from bytes")?;

        if wrapper.type_name != Self::type_name(&wrapper.data) {
            anyhow::bail!("Type mismatch: expected {}, found {}", Self::type_name(&wrapper.data), wrapper.type_name);
        }

        Ok(wrapper.data)
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Node<M> {
    pub name: String,
    pub port: u16,
    pub ip_addr: IpAddr,
    pub status: NodeStatus,
    pub metadata: M,
}

impl<M: NodeMetadata> Node<M> {
  
    /// Creates a new `Node` with the given IP address, port, name, and metadata.
    pub fn new(ip_addr: IpAddr, port: u16, name: String, incarnation: u64, metadata: M) -> Self {
        Self {
            ip_addr,
            port,
            name,
            status: NodeStatus::new(incarnation),
            metadata: metadata,
        }
    }

    /// Returns the `SocketAddr` for this Node.
    pub fn with_state(&mut self, new_state: NodeState) -> Self {
        self.status.update_state(new_state);
        self.clone()
    }

    /// Returns the `SocketAddr` for this Node.
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        Ok(SocketAddr::new(self.ip_addr, self.port))
    }

    /// Checks if the Node state is Alive.
    pub fn is_alive(&self) -> bool {
        self.status.state == NodeState::Alive
    }

    /// Checks if the Node state is Suspect.
    pub fn is_suspect(&self) -> bool {
        self.status.state == NodeState::Suspect
    }

    /// Checks if the Node state is Dead.
    pub fn is_dead(&self) -> bool {
        self.status.state == NodeState::Dead
    }

    /// Returns current Node state
    pub fn state(&self) -> NodeState {
        self.status.state
    }

    /// Updates the state of the Node.
    pub(crate) fn update_state(&mut self, new_state: NodeState) {
        self.status.update_state(new_state);
    }

    /// Advances the Node to the next state.
    pub fn advance_state(&mut self) {
        self.status.state = self.status.state.next_state();
    }

    /// Returns incarnation number of the node
    pub(crate) fn incarnation(&self) -> u64 {
        self.status.incarnation
    }

    /// Merges the state of another Node into this Node.
    /// Returns true if any changes were made.
    pub fn merge(&mut self, other: &Node<M>) -> bool {
        let self_status = &mut self.status;
        let other_status = &other.status;

        match self_status.incarnation.cmp(&other_status.incarnation) {
            Ordering::Less => {
                *self_status = other_status.clone();
                self.metadata = other.metadata.clone();
                true
            }
            Ordering::Equal => {
                if other_status.state.precedence() > self_status.state.precedence() {
                    self_status.update_state(other_status.state.clone());
                    self_status.last_updated = other_status.last_updated;
                    self.metadata = other.metadata.clone();
                    true
                } else if other_status.state == self_status.state && other_status.last_updated > self_status.last_updated {
                    self_status.last_updated = other_status.last_updated;
                    self.metadata = other.metadata.clone();
                    true
                } else {
                    println!("{}", "true");
                    false
                }
            }
            Ordering::Greater => false, // No-Op
        }
    }

    /// Checks if the Node has been in the Suspect state for longer than the given timeout.
    pub fn suspect_timeout(&self, timeout: Duration) -> bool {
        self.status.state == NodeState::Suspect && 
        SystemTime::now().duration_since(self.status.last_updated).unwrap_or_default() > timeout
    }
}

impl<M> PartialOrd for Node<M>
where
    M: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + std::hash::Hash,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<M> Ord for Node<M>
where
    M: Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq + std::hash::Hash,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
    struct DefaultMetadata;

    impl DefaultMetadata {
        fn new() -> Self {
            DefaultMetadata
        }
    }

    impl NodeMetadata for DefaultMetadata {}

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
    struct Metadata {
        region: String,
        datacenter: String,
    }

    impl Metadata {
        pub fn region(&self) -> String {
            self.region.clone()
        }
        pub fn datacenter(&self) -> String {
            self.datacenter.clone()
        }
    }

    impl NodeMetadata for Metadata {}

    #[test]
    fn test_node_without_metadata() {
        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0,DefaultMetadata::new());
        
        node.update_state(NodeState::Alive);
        
        assert_eq!(node.name, "node1");
        assert_eq!(node.port, 8000);
        assert_eq!(node.ip_addr, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(node.status.state, NodeState::Alive);
    }

    #[test]
    fn test_node_with_metadata() {
        let metadata = Metadata { 
            region: "aws-west-1".to_string(),
            datacenter: "dc1".to_string(),
        };

        let mut node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(), 0,metadata);
        node.update_state(NodeState::Alive);
        
        assert_eq!(node.name, "node1");
        assert_eq!(node.port, 8000);
        assert_eq!(node.ip_addr, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(node.status.state, NodeState::Alive);
        
        let metadata = node.metadata;
        assert_eq!(metadata.region(), "aws-west-1".to_string());
        assert_eq!(metadata.datacenter(), "dc1".to_string());
    }

    #[test]
    fn test_node_merge() {
        let mut prev_node = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string(),0, DefaultMetadata::new());
        
        prev_node.update_state(NodeState::Alive);
        let mut new_node = prev_node.clone();

        // Test: Higher incarnation wins
        prev_node.status.increment_incarnation();
        new_node.update_state(NodeState::Suspect);
        assert!(!prev_node.merge(&new_node));
        assert_eq!(prev_node.status.state, NodeState::Alive);

        // Test: Equal incarnation, higher precedence state wins
        prev_node.update_state(NodeState::Alive);
        new_node.status.increment_incarnation();
        new_node.update_state(NodeState::Dead);
        assert!(prev_node.merge(&new_node));
        assert_eq!(new_node.status.state, NodeState::Dead);

        // Test: Equal incarnation and state, more recent change wins
        std::thread::sleep(Duration::from_millis(10));
        new_node.update_state(NodeState::Dead);
        assert!(prev_node.merge(&new_node));
        assert!(prev_node.status.last_updated == new_node.status.last_updated);

        // Test: Lower incarnation doesn't overwrite (no-op)
        prev_node.status.increment_incarnation();
        prev_node.update_state(NodeState::Alive);
        assert!(!prev_node.merge(&new_node));
        assert_eq!(prev_node.status.state, NodeState::Alive);
    }
}
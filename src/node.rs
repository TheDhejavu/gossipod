use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;

use crate::state::NodeState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct NodeStateInfo {
    incarnation: u32,
    state: NodeState,
    last_state_change: SystemTime,
}

impl NodeStateInfo {
    /// Creates a new `NodeStateInfo` with default values.
    fn new() -> Self {
        let now = SystemTime::now();
        Self {
            incarnation: 0,
            state: NodeState::Unknown,
            last_state_change: now,
        }
    }

    /// Updates the state of the node and records the time of change.
    fn update_state(&mut self, new_state: NodeState) {
        self.state = new_state;
        println!("{}", new_state);
        self.last_state_change = SystemTime::now();
    }

    /// Increments the incarnation number and updates the last change time.
    fn increment_incarnation(&mut self) {
        self.incarnation += 1;
        self.last_state_change = SystemTime::now();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Node {
    pub port: u16,
    pub ip_addr: IpAddr,
    pub name: String,
    pub region: Option<String>,
    pub info: NodeStateInfo,
}

impl Node {
    /// Creates a new `Node` with the given IP address, port, and name.
    pub(crate) fn new(ip_addr: IpAddr, port: u16, name: String) -> Self {
        Self {
            ip_addr,
            port,
            name,
            region: None,
            info: NodeStateInfo::new(),
        }
    }

    /// Sets the name for the node and returns the modified node.
    pub(crate) fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    /// Sets the region for the node and returns the modified node.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets the initial state for the node and returns the modified node.
    pub fn with_state(mut self, state: NodeState) -> Self {
        self.info.update_state(state);
        self
    }

    /// Returns the region of the node, if set.
    pub fn region(&self) -> Option<String> {
        self.region.clone()
    }

    /// Returns the `SocketAddr` for this node.
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        Ok(SocketAddr::new(self.ip_addr, self.port))
    }

    /// Checks if the node state is Alive.
    pub fn is_alive(&self) -> bool {
        self.info.state == NodeState::Alive
    }

    /// Checks if the node state is Suspect.
    pub fn is_suspect(&self) -> bool {
        self.info.state == NodeState::Suspect
    }

    /// Checks if the node state is Dead.
    pub fn is_dead(&self) -> bool {
        self.info.state == NodeState::Dead
    }

    /// Retruns current node state
    pub fn state(&self) -> NodeState {
        self.info.state
    }

    /// Updates the state of the node.
    pub fn update_state(&mut self, new_state: NodeState) {
        self.info.update_state(new_state);
    }

    /// Returns the next state based on the current state.
    pub fn next_state(&self) -> NodeState {
        self.info.state.next_state()
    }

    /// Advances the node to the next state.
    pub fn advance_state(&mut self) {
        self.info.state = self.info.state.next_state();
    }

    /// Increments the incarnation number of the node.
    pub fn increment_incarnation(&mut self) {
        self.info.increment_incarnation();
    }

    /// Merges the state of another node into this node.
    /// Returns true if any changes were made.
    pub fn merge(&mut self, other: &Node) -> bool {
        let self_info = &mut self.info;
        let other_info = &other.info;

        match self_info.incarnation.cmp(&other_info.incarnation) {
            Ordering::Less => {
                *self_info = other_info.clone();
                true
            }
            Ordering::Equal => {
                if other_info.state.precedence() > self_info.state.precedence() {
                    self_info.update_state(other_info.state.clone());
                    self_info.last_state_change = other_info.last_state_change;
                    true
                } else if other_info.state == self_info.state && other_info.last_state_change > self_info.last_state_change {
                    self_info.last_state_change = other_info.last_state_change;
                    true
                } else {
                    false
                }
            }
            Ordering::Greater => false,
        }
    }

    /// Checks if the node has been in the Suspect state for longer than the given timeout.
    pub fn suspect_timeout(&self, timeout: Duration) -> bool {
        self.info.state == NodeState::Suspect && 
        SystemTime::now().duration_since(self.info.last_state_change).unwrap_or_default() > timeout
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_node_merge() {
        let mut node1 = Node::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000, "node1".to_string())
            .with_state(NodeState::Alive);
        let mut node2 = node1.clone();

        // Test: Higher incarnation overwrites
        node2.increment_incarnation();
        node2.update_state(NodeState::Suspect);
        assert!(node1.merge(&node2));
        assert_eq!(node1.info.state, NodeState::Suspect);

        // Test: Equal incarnation, higher precedence state
        node1.update_state(NodeState::Alive);
        node2.update_state(NodeState::Dead);
        assert!(node1.merge(&node2));
        assert_eq!(node1.info.state, NodeState::Dead);

        // Test: Equal incarnation and state, more recent change
        std::thread::sleep(Duration::from_millis(10));
        node2.update_state(NodeState::Dead);
        assert!(node1.merge(&node2));
        assert!(node1.info.last_state_change == node2.info.last_state_change);

        // Test: Lower incarnation doesn't overwrite (no-op)
        node1.increment_incarnation();
        node1.update_state(NodeState::Alive);
        assert!(!node1.merge(&node2));
        assert_eq!(node1.info.state, NodeState::Alive);
    }
}
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum NodeState {
    Alive,
    Suspect,
    Dead,
    Unknown,
}
impl NodeState {
    /// Transition to the next state based on SWIM protocol rules
    pub(crate)  fn next_state(&self) -> Self {
        match self {
            NodeState::Alive => NodeState::Suspect,
            NodeState::Suspect => NodeState::Dead,
            NodeState::Dead => NodeState::Dead, 
            NodeState::Unknown => NodeState::Alive, 
        }
    }
    

    /// Check if the state is considered active (Alive or Suspect)
    pub(crate)  fn is_active(&self) -> bool {
        matches!(self, NodeState::Alive | NodeState::Suspect)
    }

    /// Convert a string to NodeState
    pub(crate)  fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "alive" => Some(NodeState::Alive),
            "suspect" => Some(NodeState::Suspect),
            "dead" => Some(NodeState::Dead),
            "unknown" => Some(NodeState::Unknown),
            _ => None,
        }
    }

    pub(crate) fn precedence(&self) -> u8 {
        match self {
            NodeState::Dead => 3,
            NodeState::Suspect => 2,
            NodeState::Alive => 1,
            NodeState::Unknown => 0,
        }
    }
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Unknown
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Alive => write!(f, "Alive"),
            NodeState::Suspect => write!(f, "Suspect"),
            NodeState::Dead => write!(f, "Dead"),
            NodeState::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_state() {
        assert_eq!(NodeState::Alive.next_state(), NodeState::Suspect);
        assert_eq!(NodeState::Suspect.next_state(), NodeState::Dead);
        assert_eq!(NodeState::Dead.next_state(), NodeState::Dead);
        assert_eq!(NodeState::Unknown.next_state(), NodeState::Alive);
    }

    #[test]
    fn test_is_active() {
        assert!(NodeState::Alive.is_active());
        assert!(NodeState::Suspect.is_active());
        assert!(!NodeState::Dead.is_active());
        assert!(!NodeState::Unknown.is_active());
    }

    #[test]
    fn test_from_str() {
        assert_eq!(NodeState::from_str("alive"), Some(NodeState::Alive));
        assert_eq!(NodeState::from_str("SUSPECT"), Some(NodeState::Suspect));
        assert_eq!(NodeState::from_str("Dead"), Some(NodeState::Dead));
        assert_eq!(NodeState::from_str("unknown"), Some(NodeState::Unknown));
        assert_eq!(NodeState::from_str("invalid"), None);
    }
}
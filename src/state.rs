use serde::{Deserialize, Serialize};


// Swim Protocol Node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum NodeState {
    Alive,
    Dead,
    Suspect,
    Unknown
}

use serde::{Deserialize, Serialize};

/*
 *
 * ===== NodeState =====
 *
 */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum NodeState {
    Alive,
    Dead,
    Suspect,
    Unknown
}
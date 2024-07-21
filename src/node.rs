use std::net::{IpAddr, SocketAddr};
use anyhow::{anyhow, Result};
use crate::state::NodeState;


#[derive(Clone, Debug)]
pub(crate) struct Node {
    // the current node to a port
    pub(crate) port: u16,

    // IP Address of peer
    pub(crate) ip_addr: IpAddr,

    // optional name for the member
    pub(crate) name: Option<String>,

    // optional location for the member E.G aws-west-2
    pub(crate) location: Option<String>,

    // optional node state.
    pub(crate) state: Option<NodeState>,
}

impl Node {
    pub(crate) fn socket_addr(&self) -> Result<SocketAddr> {
        let full_address = format!("{}:{}", self.ip_addr, self.port);
        full_address.parse::<SocketAddr>().map_err(|e| anyhow!(e.to_string()))
    }

    pub(crate) fn new(ip_addr: IpAddr, port: u16) -> Self {
        Self {
            ip_addr,
            port,
            name: None,
            location: None,
            state: None,
        }
    }

    pub(crate) fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub(crate) fn with_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }

    pub(crate) fn with_state(mut self, state: NodeState) -> Self {
        self.state = Some(state);
        self
    }
}
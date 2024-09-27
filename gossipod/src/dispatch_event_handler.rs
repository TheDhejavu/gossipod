use async_trait::async_trait;
use crate::{node::Node, NodeMetadata};
use std::net::SocketAddr;
use std::error::Error;

/// [`DispatchEventHandler`] trait is used for dispatching events.
///
/// This trait defines methods for handling core SWIM events:
/// node death detection, node departure, node joining, and message handling.
/// Implementations of this trait can be used to respond to these key
/// events.
#[async_trait]
pub trait DispatchEventHandler<M: NodeMetadata>: Send + Sync {
    /// Notifies the handler that a node has been detected as dead.
    ///
    /// In SWIM, a node is marked as dead when it has failed to respond to
    /// both direct and indirect pings within the protocol's timeout period.
    async fn notify_dead(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Notifies the handler that a node is leaving the cluster.
    ///
    /// This method is called when a node is intentionally leaving the cluster.
    /// This represents a graceful shutdown where the node explicitly
    /// notifies others of its departure.
    async fn notify_leave(&self, node: &Node<M>) ->  Result<(), Box<dyn Error + Send + Sync>>;

    /// Notifies the handler that a new node has joined the cluster.
    ///
    /// This method is called when a new node successfully joins the SWIM cluster.
    /// It allows the implementation to react to cluster growth and potentially
    /// update its internal state or external systems.
    async fn notify_join(&self, node: &Node<M>) ->  Result<(), Box<dyn Error + Send + Sync>>;

    /// Notifies the handler that a message has been received from a peer via TCP.
    ///
    /// This method is called when a TCP message is received from another node in the cluster.
    /// It allows the implementation to process and react to various types of messages
    /// that may be sent between nodes, such as application-specific data.
    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) ->  Result<(), Box<dyn Error + Send + Sync>>;
}
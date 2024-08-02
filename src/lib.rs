use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use anyhow::{anyhow, Context as _, Result};
use config::BROADCAST_FANOUT;
use log::*;
use members::MergeAction;
use message::{Broadcast, MessagePayload};
pub use node::{DefaultMetadata, NodeMetadata};
use notifer::Notifier;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time;
use utils::pretty_debug;
use std::net::SocketAddr;
use std::time::Duration;
use crate::{
    config::{GossipodConfig, DEFAULT_IP_ADDR, DEFAULT_TRANSPORT_TIMEOUT, MAX_CONSECUTIVE_FAILURES},
    event::EventManager,
    ip_addr::IpAddress,
    listener::EventListener,
    members::Membership,
    message::{Message, NetSvc, RemoteNode},
    node::Node,
    retry_state::RetryState,
    state::NodeState,
    transport::Transport,
};

mod transport;
mod message;
mod ip_addr;
pub mod config;
mod state;
mod node;
mod members;
mod listener;
mod codec;
mod retry_state;
mod event;
mod notifer;
mod utils;

//  # SWIM Protocol Implementation for GOSSIPOD

/// This module implements an asynchronous SWIM (Scalable Weakly-consistent
/// Infection-style Membership) protocol. The implementation is modularized
/// for clarity, maintainability, and potential code reuse:
///
/// * Gossipod: Core protocol logic handler that manages messaging requests from 
/// cluster nodes and executes protocol-specific actions.
///
/// * Listeners: Network event processors that handle incoming TCP and UDP connections
/// and process messages according to the Gossipod protocol.
///
/// * Membership: Local membership state manager that manages membership 
/// information changes through a single 'merge' entry point. It compares existing 
/// data with new data and applies necessary updates.
///
/// * Transport: Network communication layer that provides out-of-the-box UDP and TCP 
/// communication for inter-node interaction. It utilizes Tokio runtime for efficient 
/// concurrency, continuously listens to streams and socket packets, and forwards data 
/// to listeners for protocol-specific actions. A future improvement plan is to 
/// implement `io_uring`` for enhanced performance.
///
/// * NetService (NetSvc): Intermediary between Gossipod and Transport that constructs 
/// messages and forwards them to the transport layer.
///
/// Protocol Implementation Details:
/// `PING`` and `PING-REQ` wil be used for failure detections and information
/// dissemination , which means messages have to be piggybacked while we wait or we can 
/// create a future event that will wait for ack message based on sequence number within a time
/// frame. PIN or PING-REQ will happen via constant probing

/// `BROADCAST`` is used to disseminate messages (JOIN, LEAVE, SUSPECT, CONFIRM)
/// node triggers this when it discovers a state change of a node or receives
/// a voluntarity requests from a node that changes it state, E.G when a node shuts down
/// it sends a leave announcement to x random nodes notifying them of is dispature, when 
/// the nodes receives this request they immediately take action by removng the node from 
/// their local membership list and in the long run information is dissiemnated via piggybacking
/// on the failure detection infection style logic during the regular PING or PING_REQ 

/// In a nutshell gossipod will have 3 types of messages:
/// PING, PING-REQ, ANNOUNCE (JOIN, LEAVE, SUSPECT, ALIVE, CONFIRM) 
/// PING & PING-REQ: handles constant stat exchange via information dissemination piggybacked on failure detection logic
/// BROADCAST: handles random broadcast when a state changes either through volunrary request or through regular failure detection
/// Each node in the network maintains an incarnation number, starting at zero, which can only be incremented by the node itself. 
/// This number is crucial for managing the node's state in other nodes' local membership lists and serves as a means to refute suspicions 
/// `(SWIM+Inf.+Susp.)` from other nodes.

pub struct Gossipod<M: NodeMetadata = DefaultMetadata> {
    inner: Arc<InnerGossipod<M>>,
}

enum GossipodState {
    Idle,
    Running,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ShutdownReason {
    Termination,
    TcpFailure,
    UdpFailure,
    SchedulerFailure,
}


pub(crate) struct InnerGossipod<M: NodeMetadata>  {
    /// The local node metadata
    metadata: M,

    /// Configuration settings for the Gossipod
    config: GossipodConfig,

    /// Map of all known members and their current state.
    /// Each node maintains information about all other nodes.
    members: Membership<M>,

    /// Communication layer for sending and receiving messages
    transport: Transport,

    /// Current state of the Gossipod,
    state: RwLock<GossipodState>,

    /// Channel sender for initiating shutdown
    shutdown: broadcast::Sender<()>,

    /// Monotonically increasing sequence number for events
    sequence_number: AtomicU64,

    /// Incarnation number, used to detect and handle node restarts
    incarnation: AtomicU64,

    /// Manager for handling and creating of events
    event_manager: EventManager,

    /// Counter for synchronization requests
    sync_req: AtomicU64,
    notifier: Arc<Notifier>,

    /// Network service for handling communication
    pub(crate) net_svc: NetSvc,
}

impl<M: NodeMetadata> Clone for Gossipod<M>{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Gossipod {
    /// Creates a new instance of Gossipod with the default metadata.
    pub async fn new(config: GossipodConfig) -> Result<Self> {
        Gossipod::with_metadata(config, DefaultMetadata::new()).await
    }
}


impl<M: NodeMetadata> Gossipod<M> {
    /// Creates a new instance of Gossipod with the provided metadata.
    pub async fn with_metadata(config: GossipodConfig, metadata: M) -> Result<Self> {
        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .filter_level(::log::LevelFilter::Debug)
            .init();

        let (shutdown_tx, _) = broadcast::channel(1);
        let (transport, transport_channel) = Transport::new(
            config.port(), 
            config.addr(),  
            Duration::from_millis(DEFAULT_TRANSPORT_TIMEOUT),
        );

        let net_svc = NetSvc::new(Box::new(transport.clone()));
        
        let swim = Self {
            inner: Arc::new(InnerGossipod {
                config,
                net_svc,
                metadata,
                members: Membership::new(),
                state: RwLock::new(GossipodState::Idle),
                shutdown: shutdown_tx.clone(),
                transport,
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(0),
                sync_req: AtomicU64::new(0),
                notifier: Arc::new(Notifier::new()),
                event_manager: EventManager::new(Duration::from_millis(500)),
            })
        };

        Self::spawn_listeners(
            EventListener::new(swim.clone(), transport_channel, shutdown_tx.clone()),
        ).await;

        Ok(swim)
    }
    //+=========================+
    //| GOSSIPOD COMMUNICATION LOW:
    //+=========================+
    // +-----------+       +----------------+       +-------------------+----------------------+
    // | gossipod  +------->   net service  +-------> transport layer   |                      |
    // +-----------+       +----------------+       |    (TCP/UDP)      |                      |
    // |                                            +---------+---------+                      |
    // |                                                      |                                |
    // |                                                      v                                |
    // |                                            +---------+---------+                      |
    // |                                            |     listener      |                      |
    // |                                            +---------+---------+                      |
    // |                                                      |                                |
    // |                                    +-----------------+------------------------+       |
    // |                                    |                                          |       |
    // |                                    v                                          v       |                      
    // |                              +-----+------+                           +-------+-------+                           
    // |                              |  gossipod  |                           |  net service  |                      
    // |                              +------------+                           +---------------+
    // |                                                                                |                       
    // +--------------------------------------------------------------------------------+
    pub async fn start(&self) -> Result<()> {
        info!("[GOSSIPOD] Server Started with `{}`", self.inner.config.name);
        let shutdown_rx = self.inner.shutdown.subscribe();

        self.inner.transport.bind_tcp_listener().await?;
        self.inner.transport.bind_udp_socket().await?;

        self.log_ip_binding_info().await?;
        self.set_state(GossipodState::Running).await?;
        self.set_local_node_liveness().await?;

        let tcp_handle = Self::spawn_tcp_listener_with_retry(
            self.inner.transport.clone(),
            RetryState::new(),
            self.inner.shutdown.subscribe(),
        );
        let udp_handle = Self::spawn_udp_listener_with_retry(
            self.inner.transport.clone(),
            RetryState::new(),
            self.inner.shutdown.subscribe(),
        );
        let scheduler_handle = self.spawn_scheduler(self.inner.shutdown.subscribe());

        let shutdown_reason = self.handle_shutdown_signal(
            tcp_handle, 
            udp_handle, 
            scheduler_handle, 
            shutdown_rx,
        ).await?;

        if shutdown_reason != ShutdownReason::Termination {
            self.inner.shutdown.send(()).
                map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }
    
        self.set_state(GossipodState::Stopped).await?;
        self.inner.event_manager.shutdown().await?;
        self.leave().await?;

        info!("[GOSSIPOD] Gracefully shut down due to {:?}", shutdown_reason);
    
        Ok(())
    }

    async fn handle_shutdown_signal(
        &self,
        tcp_handle: tokio::task::JoinHandle<Result<()>>,
        udp_handle: tokio::task::JoinHandle<Result<()>>,
        scheduler_handle: tokio::task::JoinHandle<Result<()>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<ShutdownReason> {
        tokio::select! {
            _ = tcp_handle => Ok(ShutdownReason::TcpFailure),
            _ = udp_handle => Ok(ShutdownReason::UdpFailure),
            _ = scheduler_handle => Ok(ShutdownReason::SchedulerFailure),
            _ = shutdown_rx.recv() => {
                info!("[RECV] Initiating graceful shutdown..");
                Ok(ShutdownReason::Termination)
            }
        }
    }
    async fn log_ip_binding_info(&self)-> Result<()>{
        // Note: When the Local Host (127.0.0.1 or 0.0.0.0) is provisioned, it is automatically bound to the system's private IP.
        // If a Private IP address is bound, the local host (127.0.0.1) becomes irrelevant.
        let private_ip_addr = IpAddress::find_system_ip()?;
        for ip_addr in &self.inner.config.ip_addrs {
            if ip_addr.to_string() == DEFAULT_IP_ADDR || ip_addr.to_string() == "0.0.0.0" {
                info!(
                    " [GOSSIPOD] Binding to all network interfaces: {}:{} (Private IP: {}:{})",
                    ip_addr.to_string(),
                    self.inner.config.port,
                    private_ip_addr.to_string(),
                    self.inner.config.port
                );
            } else {
                info!(
                    "[GOSSIPOD] Binding to specific IP: {}:{}",
                    ip_addr.to_string(),
                    self.inner.config.port
                );
            }
        }
        Ok(())
    }

    async fn spawn_listeners(mut listener: EventListener<M>) {
        tokio::spawn(async move {
            listener.run_listeners().await 
        });
    }

    fn spawn_tcp_listener_with_retry(
        transport: Transport,
        mut retry_state: RetryState,
        mut shutdown_rx: broadcast::Receiver<()>
    ) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    listener_result = transport.tcp_stream_listener() => {
                        match listener_result {
                            Ok(_) => {
                                retry_state.record_success();
                            }
                            Err(e) => {
                                let failures = retry_state.record_failure();
                                error!("TCP listener error: {}. Consecutive failures: {}", e, failures);
                                
                                if failures >= MAX_CONSECUTIVE_FAILURES {
                                    return Err(anyhow!("TCP listener failed {} times consecutively", failures));
                                }
            
                                let delay = retry_state.calculate_delay();
                                warn!("TCP listener restarting in {:?}", delay);
                                
                                tokio::select! {
                                    _ = time::sleep(delay) => {}
                                    _ = shutdown_rx.recv() => {
                                        warn!("Shutdown signal received during TCP listener restart delay");
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        warn!("[RECV] Shutdown signal received, stopping TCP listener");
                        return Ok(());
                    }
                }
            }
        })
    }
    
    fn spawn_udp_listener_with_retry(
        transport: Transport,
        mut retry_state: RetryState,
        mut shutdown_rx: broadcast::Receiver<()>
    ) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    listener_result = transport.udp_socket_listener() => {
                        match listener_result {
                            Ok(_) => {
                                retry_state.record_success();
                            }
                            Err(e) => {
                                let failures = retry_state.record_failure();
                                error!("UDP listener error: {}. Consecutive failures: {}", e, failures);
                                
                                if failures >= MAX_CONSECUTIVE_FAILURES {
                                    return Err(anyhow!("TCP listener failed {} times consecutively", failures));
                                }
            
                                let delay = retry_state.calculate_delay();
                                warn!("UDP listener restarting in {:?}", delay);
                                
                                tokio::select! {
                                    _ = time::sleep(delay) => {}
                                    _ = shutdown_rx.recv() => {
                                        warn!("[RECV] Shutdown signal received during TCP listener restart delay");
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        warn!("[RECV] Shutdown signal received, stopping UDP listener");
                        return Ok(());
                    }
                }
            }
        })
    }

    fn spawn_scheduler(&self, mut shutdown_rx: broadcast::Receiver<()>) -> tokio::task::JoinHandle<Result<()>> {
        let gossipod = self.clone();
        
        // Probing is how we detect failure and Dissemination is how 
        // we randomly broadcast message in an infectious-style
        // E.G: When we discover a DEAD, ALIVE, SUSPECT Node, we can disseminate 
        // the state change by broadcasting it

        // x. FAILURE DETECTION USING PROBING
        // 1. Pick a node at random using round-robin 
        // 2. Send a PING message to the node
        // 3. If Received, Node send back an ACK message
        // 4. If Failed, Pick random nodes and send an INDIRECT PING-REQ for INDIRECT ACK to it specifying the target node for the request
        // 5. If Success, Do Nothing
        // 6. If Failed, Mark The node as suspicious, When a state change , disseminate message to a random node, till its propagated
        // 7. If suspicious is indeed dead, it is kicked out of the membership list
        // 8. If Not, the node will have to refute it and is alive state will be re-instated
        // 9. End
        // Perform probing action

        // x.INFORMATION DISSEMINATION THROUGH GOSSIPING && PIGGYBACKING ON PING & PING-REQ

        // Basically we have:
        // PROBING: For detecting failure
        // GOSSIPING: For disseminating node state change in one node targeting x randomly selected nodes
        tokio::spawn(async move {
            // Create a ticker for the probing interval
            let mut probe_interval = time::interval(gossipod.inner.config.probing_interval);

            // Create a ticker for the gossip interval
            let mut gossip_interval = time::interval(gossipod.inner.config.gossip_interval);
            debug!("starting scheduler.....");

            loop {
                tokio::select! {
                    _ = probe_interval.tick() => {
                        if let Err(e) = gossipod.probe().await {
                            error!("Error during probing: {}", e);
                        }
                    }
                    _ = gossip_interval.tick() => {
                        if let Err(e) = gossipod.gossip().await {
                            error!("Error during gossiping: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("[RECV] Scheduler shutting down");
                        break;
                    }
                }
            }
            Ok(())
        })
    }
    
    /// Generate the next sequence number for message ordering.
    /// This method allows wrapping around to 0 when the maximum is reached.
    fn next_sequence_number(&self) -> u64 {
        self.inner.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

   /// Generate the next incarnation number for conflict resolution.
    fn next_incarnation(&self) -> u64 {
        self.inner.incarnation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get the incarnation number
    fn incarnation(&self) -> u64 {
        self.inner.incarnation.load(Ordering::SeqCst)
    }

    // set message channel reciever for your application
    pub async fn with_receiver(&self, buffer: usize) -> mpsc::Receiver<Vec<u8>> {
        self.inner.notifier.with_receiver(buffer).await
    }

    // Send user application-specific messages to target
    pub async fn send(&self, target: SocketAddr, msg: &[u8]) -> Result<()> {
        Ok(())
    }

    // checks if server is running and checks if your have receiver initialized for 
    // accepting app-specific message
    pub async fn is_ready(&self) -> bool {
        self.inner.notifier.is_initialized().await && self.is_running().await
    }

    /// Gossipod PROBE - Probes randomly selected nodes
    pub(crate) async fn probe(&self) -> Result<()> {
        // info!("PROBING");
        // unimplemented!()
        Ok(())
    }

    /// Gossipod GOSSIP - Gossip with randomly selected nodes for state changes
    pub(crate) async fn gossip(&self) -> Result<()> {
        // info!("GOSSIP");
        Ok(())
    }

    /// Gossipod LEAVE - Notifies nodes of a peer leaving 
    pub async fn leave(&self) -> Result<()> {
        debug!("LEAVING");
        let incarnation = self.next_incarnation();
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
        let broadcast = Broadcast::Leave {  
            member: local_node.name,
            incarnation,
        };

        // Broadcast leave message to a subset of known nodes (if any)
        let known_nodes = self.inner.members.select_random_nodes(BROADCAST_FANOUT, Some(|n: &Node<M>| {
            if let Ok(addr) = n.socket_addr() {
                return !n.is_alive() || addr == local_addr
            }
            false
        })).unwrap_or_default();

        for node in &known_nodes {
            if let Err(e) = self.inner.net_svc.broadcast(node.socket_addr()?, local_addr, broadcast.clone()).await {
                warn!("failed to broadcast join message to {}: {}", node.name, e);
            }
        }

        debug!("Leave broadcast sent to {} nodes", known_nodes.len());
        Ok(())

    }

    async fn handle_ping(&self, message: Message) -> Result<()> {
        unimplemented!()
    }

    async fn handle_no_ack(&self, message: Message) -> Result<()> {
        unimplemented!()
    }

    async fn handle_ping_req(&self, message: Message) -> Result<()> {
        unimplemented!()
    }

    async fn handle_ack(&self, message: Message) -> Result<()> {
        unimplemented!()
    }
     
    async fn handle_app_msg(&self, message: Message) -> Result<()> {
        unimplemented!()
    }

    pub(crate) async fn handle_broadcast(&self, message: Message) -> Result<()> {
        match message.payload {
            MessagePayload::Broadcast(broadcast) => {
                match broadcast {
                    Broadcast::Suspect { incarnation, member } => {
                        debug!("Received SUSPECT broadcast for member: {}", member);
                        self.handle_suspect_node(incarnation, &member).await?;
                    },
                    Broadcast::Join { member } => {
                        debug!("Received JOIN broadcast for member: {}", member.name);
                        if let Err(e) = self.handle_join_node(member).await {
                            warn!("unable to handle join node: {}", e.to_string())
                        }
                    },
                    Broadcast::Leave { incarnation, member } => {
                        debug!("Received LEAVE broadcast for member: {}", member);
                        if let Err(e) = self.handle_leave_node(incarnation, &member).await {
                            warn!("unable to handle leave node request")
                        }
                    },
                    Broadcast::Confirm { incarnation, member } => {
                        debug!("Received CONFIRM broadcast for member: {}", member);
                        self.handle_confirm_node(incarnation, &member).await?;
                    },
                    Broadcast::Alive { incarnation, member } => {
                        debug!("Received ALIVE broadcast for member: {}", member);
                        self.handle_alive_node(incarnation, &member).await?;
                    },
                }
                Ok(())
            },
            _ => Err(anyhow!("Expected Broadcast payload, got {:?}", message.payload)),
        }
    }

    pub(crate) async fn handle_suspect_node(&self, incarnation: u64, member: &str) -> Result<()> {
        let mut node = match self.inner.members.get_node(member)? {
            Some(n) => n,
            None => {
                warn!("Received suspect message for unknown node: {}", member);
                return Ok(());
            }
        };

        // If the suspect message is about ourselves, we need to refute it
        if member == self.inner.config.name() {
            return self.refute_suspicion(incarnation).await;
        }

        match self.inner.members.merge(&node) {
            Ok(merge_result) => {
                match merge_result.action {
                    MergeAction::Updated => {
                        info!("Node {} is now suspected", member);
                        self.disseminate_suspect(node).await?;
                    },
                    MergeAction::Unchanged => {
                        debug!("Node {} was already suspected", member);
                    },
                    _ => {
                        warn!("Unexpected merge result when suspecting node {}", member);
                    }
                }
            },
            Err(e) => {
                info!("Unable to merge node {}: {}", member, e);
            },
        }

        Ok(())
    }

    async fn refute_suspicion(&self, incarnation: u64) -> Result<()> {
        let mut local_node = self.get_local_node().await?;
        
        // Only refute if the suspicion is for our current or future incarnation
        if incarnation >= local_node.incarnation() {
            let next_incarnation = self.next_incarnation();
            local_node.set_incarnation(next_incarnation);
            local_node.update_state(NodeState::Alive)?;

            info!("Refuting suspicion with new incarnation: {}", local_node.incarnation());

            self.inner.members.merge(&local_node)?;
            self.disseminate_alive(local_node).await?;
        }

        Ok(())
    }

    async fn disseminate_alive(&self, node: Node<M>) -> Result<()> {
        let broadcast = Broadcast::Alive {
            incarnation: node.incarnation(),
            member: node.name.clone(),
        };
        self.broadcast_to_random_nodes(broadcast).await
    }

    async fn disseminate_suspect(&self, node: Node<M>) -> Result<()> {
        let broadcast = Broadcast::Suspect {
            incarnation: node.incarnation(),
            member: node.name.clone(),
        };
        self.broadcast_to_random_nodes(broadcast).await
    }

    async fn disseminate_join(&self, node: Node<M>) -> Result<()> {
        let broadcast = Broadcast::Join {  
            member: RemoteNode { 
                name: node.name.clone(), 
                address: node.socket_addr()?, 
                metadata: node.metadata.to_bytes()?, 
                state: node.state().clone(), 
                incarnation: node.incarnation(),
            },
        };

        let local_addr = self.get_local_node().await?.socket_addr()?;

        let known_nodes = self.inner.members.select_random_nodes(BROADCAST_FANOUT, Some(|n: &Node<M>| {
            if let Ok(addr) = n.socket_addr() {
                return !n.is_alive() || addr == local_addr || n.name == node.name;
            }
            false
        })).unwrap_or_default();

        for node in &known_nodes {
            if let Err(e) = self.inner.net_svc.broadcast(node.socket_addr()?, local_addr, broadcast.clone()).await {
                warn!("Failed to propagate join message to {}: {}", node.name, e);
            }
        }

        info!("Join dissemination sent to known nodes {}", known_nodes.len());

        Ok(())
    }

    async fn confirm_node_dead(&self, node: &Node<M>) -> Result<()> {
        let mut updated_node = node.clone();
        updated_node.update_state(NodeState::Dead)?;
        
        let broadcast = Broadcast::Confirm {
            incarnation: node.incarnation(),
            member: node.name.clone(),
        };
        self.broadcast_to_random_nodes(broadcast).await
    }

    async fn set_local_node_liveness(&self) -> Result<()> {
        let ip_addr = self.inner.config.addr();
        let name = self.inner.config.name();
        let port = self.inner.config.port();
        let incarnation = self.next_incarnation();
        let mut node = Node::new(ip_addr, port, name.clone(), incarnation, self.inner.metadata.clone());
        node.update_state(NodeState::Alive)?;

        match self.inner.members.merge(&node) {
            Ok(merge_result) => {
                match merge_result.action {
                    MergeAction::Added => {
                        debug!("Added new node {} to alive state with incarnation {}", name, incarnation);
                    },
                    MergeAction::Updated => {
                        info!("Updated existing node {} to alive state with new incarnation {}", name, incarnation);
                        if let Some(old_state) = merge_result.old_state {
                            debug!("Node {} state changed from {:?} to {:?}", name, old_state, merge_result.new_state);
                        }
                    },
                    MergeAction::Unchanged => {
                        info!("No changes for node {}. Current state: {:?}, incarnation: {}", 
                               name, merge_result.new_state, incarnation);
                    },
                    _ => {}
                }
            },
            Err(e) => {
                info!("Unable to merge node {}: {}", name, e);
            },
        }

        Ok(())
    }

    pub(crate) async fn handle_join_node(&self, member: RemoteNode) -> Result<()> {
        let metadata = M::from_bytes(&member.metadata)
            .context("Failed to deserialize node metadata")?;
    
        let mut new_node = Node::new(
            member.address.ip(),
            member.address.port(),
            member.name.clone(),
            member.incarnation,
            metadata
        );
        new_node.update_state(member.state)?;

        debug!("Processing join for node: {:?}", new_node);

        // Attempt to merge the new node into our membership list
        match self.inner.members.merge(&new_node) {
            Ok(merge_result) => {
                match merge_result.action {
                    MergeAction::Added => {
                        info!("Added new node {} to the cluster", member.name);
                        self.disseminate_join(new_node).await?;
                    },
                    MergeAction::Updated => {
                        info!("Updated existing node {} in the cluster", member.name);
                        if let Some(old_state) = merge_result.old_state {
                            if old_state != merge_result.new_state {
                                debug!(
                                    "Node {} state changed from {:?} to {:?}",
                                    member.name, old_state, merge_result.new_state
                                );
                            }
                        }
                        
                        self.disseminate_join(new_node).await?;
                    },
                    MergeAction::Unchanged => {
                        debug!("No changes for node {} in the cluster", member.name);
                    },
                    _ => {}
                }
            },
            Err(e) => {
                warn!("Failed to merge node {} into cluster: {}", member.name, e);
                return Err(e.into());
            }
        }

        debug!("{}", pretty_debug("Membership:", &self.inner.members.get_all_nodes()?));

        Ok(())
    }

    async fn handle_leave_node(&self, incarnation: u64, member: &str) -> Result<()> {
        if let Ok(Some(node)) = self.inner.members.get_node(member) {
            match self.inner.members.merge(&node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Removed => {
                            info!("State changed({:?}) for node {} in the cluster", merge_result.action , node.name);
                            // Hiya!!, There is an increasing number of repetitive stuff like this, i might as well
                            // just have a Queue that a task picks from that constantly disseminate this broadcast
                            
                            // Broadcast leave message to a subset of known nodes (if any)
                            let broadcast = Broadcast::Leave {  
                                incarnation,
                                member: member.to_string(),
                            };
                            self.broadcast_to_random_nodes(broadcast).await?;
                        },
                        MergeAction::Unchanged => {
                            debug!("No changes for node {} in the cluster", node.name);
                        },
                        _ => {}
                    }
                },
                Err(e) => {
                    warn!("Failed to merge node {} into cluster: {}", node.name, e);
                }
            }
        } else {
            warn!("[WARN] node with name {} does not exist", member);
        }
        
        Ok(())
    }

    async fn handle_confirm_node(&self, incarnation: u64, member: &str) -> Result<()> {
        let local_node = self.get_local_node().await?;

        // If it's about us, we need to refute
        if member == local_node.name {
            return self.refute_node().await;
        }

        if let Ok(Some(mut node)) = self.inner.members.get_node(member) {
            // Update the node state to Dead
            node.update_state(NodeState::Dead)?;
            node.set_incarnation(incarnation);

            match self.inner.members.merge(&node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Updated => {
                            info!("Node {} confirmed dead. Updating state.", node.name);
                            let broadcast = Broadcast::Confirm {  
                                incarnation,
                                member: member.to_string(),
                            };
                            self.broadcast_to_random_nodes(broadcast).await?;
                        },
                        MergeAction::Unchanged => {
                            debug!("No changes for node {} in the cluster", node.name);
                        },
                        _ => {}
                    }
                },
                Err(e) => {
                    warn!("Failed to merge node {} into cluster: {}", node.name, e);
                }
            }
        } else {
            warn!("[WARN] node with name {} does not exist", member);
        }
        
        Ok(())
    }

    async fn refute_node(&self) -> Result<()> {
        let local_node = self.get_local_node().await?;
        let new_incarnation = self.next_incarnation();
        
        // Create an updated node with the new incarnation number
        let mut updated_node = local_node.clone();
        updated_node.set_incarnation(new_incarnation);
        updated_node.update_state(NodeState::Alive)?;

        // Merge the updated node into the membership list
        if let Err(e) = self.inner.members.merge(&updated_node) {
            warn!("Failed to update local node in membership list: {}", e);
        }

        // Broadcast an Alive message to refute the Confirm
        let broadcast = Broadcast::Alive {
            incarnation: new_incarnation,
            member: local_node.name.clone(),
        };

        self.broadcast_to_random_nodes(broadcast).await?;

        info!("Refuted death confirmation by broadcasting Alive message with new incarnation {}", new_incarnation);
        
        Ok(())
    }

    async fn broadcast_to_random_nodes(&self, broadcast: Broadcast) -> Result<()> {
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
        
        let known_nodes = self.inner.members.select_random_nodes(BROADCAST_FANOUT, Some(|n: &Node<M>| {
            if let Ok(addr) = n.socket_addr() {
                return !n.is_alive() ||  addr == local_addr
            }
            false
        })).unwrap_or_default();

        for node in &known_nodes {
            if let Err(e) = self.inner.net_svc.broadcast(node.socket_addr()?, local_addr, broadcast.clone()).await {
                warn!("Failed to broadcast message to {}: {}", node.name, e);
            }
        }

        debug!("Broadcast sent to {} known nodes", known_nodes.len());
        Ok(())
    }

    pub(crate) async fn handle_alive_node(&self, incarnation: u64, member: &str) -> Result<()> {
        match self.inner.members.get_node(member)? {
            Some(node) => {
                match self.inner.members.merge(&node) {
                    Ok(merge_result) => {
                        match merge_result.action {
                            MergeAction::Updated => {
                                info!("Node {} is now alive with incarnation {}", member, incarnation);
                                
                                // Only disseminate if the node was previously suspected or dead
                                if let Some(old_state)= merge_result.old_state {
                                    if old_state != NodeState::Alive {
                                        self.disseminate_alive(node).await?;
                                    }
                                }
                            },
                            MergeAction::Unchanged => {
                                debug!(
                                    "Received outdated alive message for node {} (received incarnation: {}, current incarnation: {})", 
                                    member, 
                                    incarnation, 
                                    node.incarnation(),
                                );
                            },
                            _ => {
                                warn!("Unexpected merge result when marking node {} as alive", member);
                            }
                        }
                    },
                    Err(e) => {
                        warn!("Failed to merge node {} into cluster: {}", node.name, e);
                    }
                }
            },
            None => {
                warn!("Received alive message for unknown node: {}", member);
                // Optionally, we could request full state from the sender
                return Ok(());
            }
        };

        Ok(())
    }

    async fn handle_sync_req(&self, mut stream: TcpStream,message: Message) -> Result<()> {
        debug!("HANDLE-SYNC-REQ");
        Ok(())
    }

    /// Notifies nodes of a peer joining the network
    pub async fn join(&self, target: SocketAddr) -> Result<()> {
        debug!("Initiating join process with initial target: {}", target);

        let incarnation = self.next_incarnation();
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
        let broadcast = Broadcast::Join {  
            member: RemoteNode { 
                name: local_node.name.clone(),
                address: local_addr,
                metadata: local_node.metadata.to_bytes()?, 
                state: local_node.state().clone(), 
                incarnation,
            },
        };

        self.inner.net_svc.broadcast(target, local_addr, broadcast.clone()).await
            .context("failed to send join broadcast to initial contact")?;

        // Broadcast join message to a subset of known nodes (if any)
        let known_nodes = self.inner.members.select_random_nodes(BROADCAST_FANOUT, Some(|n: &Node<M>| {
            if let Ok(addr) = n.socket_addr() {
                return !n.is_alive() || addr == target || addr == local_addr
            }
            false
        })).unwrap_or_default();

        for node in &known_nodes {
            if let Err(e) = self.inner.net_svc.broadcast(node.socket_addr()?, local_addr, broadcast.clone()).await {
                warn!("failed to broadcast join message to {}: {}", node.name, e);
            }
        }

        debug!(
            "Join broadcast sent to initial contact and {} other nodes", 
            known_nodes.len(),
        );
        debug!(
            "{}", 
            pretty_debug("Membership:", &self.inner.members.get_all_nodes()?),
        );

        Ok(())
    }

    pub async fn handle_piggybacked_updates(&self, updates: Vec<RemoteNode> ) -> Result<()> {
        debug!("new piggyback");

        let converted_nodes: Result<Vec<Node<M>>> = updates.into_iter()
            .map(|payload| {
                let metadata = M::from_bytes(&payload.metadata)
                    .context("failed to deserialize node metadata")?;
                
                Ok(Node::new(
                    payload.address.ip(),
                    payload.address.port(),
                    payload.name,
                    payload.incarnation,
                    metadata
                ))
            })
            .collect();

        let converted_nodes = converted_nodes?;
        for node in converted_nodes {
            match self.inner.members.merge(&node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Added | MergeAction::Updated => {
                            info!("State changed for node {} in the cluster", node.name);
                            // broadcast state changed to a random node
                        },
                        MergeAction::Unchanged => {
                            debug!("No changes for node {} in the cluster", node.name);
                        },
                        _ => {}
                    }
                },
                Err(e) => {
                    warn!("Failed to merge node {} into cluster: {}", node.name, e);
                }
            }
        }

        Ok(())
    }

    pub async fn get_local_node(&self) -> Result<Node<M>> {
        let local_node = self.inner.members.get_node(&self.inner.config.name())?;
        if let Some(node) = local_node {
            return Ok(node)
        }

        Err(anyhow!("local node is not set"))
    }

    pub async fn members(&self)-> Result<Vec<Node<M>>>  {
        self.inner.members.get_all_nodes()
    }
    
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.inner.state.write().await;
        match *state {
            GossipodState::Running => {
                self.inner.shutdown.send(()).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                *state = GossipodState::Stopped;
                Ok(())
            }
            GossipodState::Idle => Err(anyhow::anyhow!("Gossipod is not running")),
            GossipodState::Stopped => Ok(()),  // Already stopped, no-op
        }
    }
    
    pub async fn is_running(&self) -> bool {
        matches!(*self.inner.state.read().await, GossipodState::Running)
    }

    async fn set_state(&self, gossipod_state: GossipodState) -> Result<()> {
        let mut state = self.inner.state.write().await;
        *state = gossipod_state;
        Ok(())
    }
}
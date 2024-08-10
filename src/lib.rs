use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use anyhow::{anyhow, Context as _, Result};
use codec::MessageCodec;
pub use broadcast_queue::{BroadcastQueue, DefaultBroadcastQueue};
use rand::{thread_rng, Rng};
use config::{BROADCAST_FANOUT, INDIRECT_REQ, MAX_UDP_PACKET_SIZE};
use event_scheduler::{EventState, EventType};
use log::*;
use members::MergeAction;
use message::{AckPayload, Broadcast, MessagePayload, MessageType, PingPayload, PingReqPayload};
pub use node::{DefaultMetadata, NodeMetadata};
use notifer::Notifier;
use tokio::sync::{broadcast, mpsc};
use parking_lot::RwLock;
use tokio::time::{self, Instant};
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use transport::NodeTransport;
use utils::pretty_debug;
use std::net::SocketAddr;
use std::time::Duration;
use crate::{
    config::{GossipodConfig, DEFAULT_IP_ADDR, DEFAULT_TRANSPORT_TIMEOUT},
    event_scheduler::EventScheduler,
    ip_addr::IpAddress,
    listener::EventListener,
    members::Membership,
    message::{Message, NetSvc, RemoteNode},
    node::Node,
    backoff::BackOff,
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
mod backoff;
mod event_scheduler;
mod notifer;
mod utils;
mod broadcast_queue;
mod mock_transport;

// SWIM Protocol Implementation for GOSSIPOD

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

pub struct Gossipod<M = DefaultMetadata>
where
    M: NodeMetadata + Send + Sync,
{
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


pub(crate) struct InnerGossipod<M>
where
    M: NodeMetadata + Send + Sync,
{
    /// The local node metadata
    metadata: M,

    /// Configuration settings for the Gossipod
    config: GossipodConfig,

    /// Map of all known members and their current state.
    /// Each node maintains information about all other nodes.
    members: Membership<M>,

    /// Communication layer for sending and receiving messages
    transport: Arc<dyn NodeTransport>,

    /// Current state of the Gossipod,
    state: RwLock<GossipodState>,

    /// Channel sender for initiating shutdown
    shutdown: broadcast::Sender<()>,

    /// Monotonically increasing sequence number for events
    sequence_number: AtomicU64,

    /// Incarnation number, used to detect and handle node restarts
    incarnation: AtomicU64,

    /// Manager for handling and creating of events
    event_scheduler: EventScheduler,

    notifier: Arc<Notifier>,

    /// Network service for handling communication
    pub(crate) net_svc: NetSvc,

    // broadcasts represents outbound messages to peers
    // when it's not set, it defaults to DefaultBroadcastQueue
    pub(crate) broadcasts: Arc<dyn BroadcastQueue>,
}

impl<M> Clone for Gossipod<M>
where
    M: NodeMetadata + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Gossipod {
    /// Creates a new instance of Gossipod with the default metadata.
    pub async fn new(config: GossipodConfig) -> Result<Self> {
        let queue = DefaultBroadcastQueue::new(config.initial_cluster_size);
        Gossipod::with_custom(config, DefaultMetadata::default(), Arc::new(queue)).await
    }
}

impl<M> Gossipod<M>
where
    M: NodeMetadata + Send + Sync,
{
    /// Creates a new instance of Gossipod with the provided metadata.
    pub async fn with_custom(config: GossipodConfig, metadata: M, broadcasts: Arc<dyn BroadcastQueue> ) -> Result<Self> {
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

        let shared_transport = Arc::new(transport);

        let net_svc = NetSvc::new(shared_transport.clone());
        
        let swim = Self {
            inner: Arc::new(InnerGossipod {
                config,
                net_svc,
                metadata,
                members: Membership::new(),
                state: RwLock::new(GossipodState::Idle),
                shutdown: shutdown_tx.clone(),
                transport: shared_transport,
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(0),
                notifier: Arc::new(Notifier::new()),
                event_scheduler: EventScheduler::new(),
                broadcasts,
            })
        };

        Self::launch_listeners(
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
        info!("> [GOSSIPOD] Server Started with `{}`", self.inner.config.name);
        let shutdown_rx = self.inner.shutdown.subscribe();

        self.inner.transport.bind_tcp_listener().await?;
        self.inner.transport.bind_udp_socket().await?;

        self.log_ip_binding_info().await?;
        self.set_state(GossipodState::Running).await?;
        self.set_local_node_liveness().await?;

        let tcp_handle = Self::launch_tcp_listener(
            self.inner.transport.clone(),
            BackOff::new(),
            self.inner.shutdown.subscribe(),
        );
        let udp_handle = Self::launch_udp_listener(
            self.inner.transport.clone(),
            BackOff::new(),
            self.inner.shutdown.subscribe(),
        );
        let probe_handle = self.launch_probe_scheduler(self.inner.shutdown.subscribe());
        let gossip_handle = self.launch_gossip_scheduler(self.inner.shutdown.subscribe());

        let event_scheduler_handler = self.launch_event_scheduler(self.inner.shutdown.subscribe());
    
        let shutdown_reason = self.handle_shutdown_signal(
            tcp_handle, 
            udp_handle, 
            probe_handle, 
            gossip_handle,
            event_scheduler_handler,
            shutdown_rx,
        ).await?;

        if shutdown_reason != ShutdownReason::Termination {
            self.inner.shutdown.send(()).
                map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }

        self.set_state(GossipodState::Stopped).await?;
        // self.leave().await?;

        info!("> [GOSSIPOD] Gracefully shut down due to {:?}", shutdown_reason);
    
        Ok(())
    }

    // handle shutdown signal
    async fn handle_shutdown_signal(
        &self,
        tcp_handle: tokio::task::JoinHandle<Result<()>>,
        udp_handle: tokio::task::JoinHandle<Result<()>>,
        probe_handle: tokio::task::JoinHandle<Result<()>>,
        gossip_handle: tokio::task::JoinHandle<Result<()>>,
        event_scheduler_handler: tokio::task::JoinHandle<Result<()>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<ShutdownReason> {
        tokio::select! {
            _ = tcp_handle => Ok(ShutdownReason::TcpFailure),
            _ = udp_handle => Ok(ShutdownReason::UdpFailure),
            _ = probe_handle => Ok(ShutdownReason::SchedulerFailure),
            _ = gossip_handle => Ok(ShutdownReason::SchedulerFailure),
            _ = event_scheduler_handler => Ok(ShutdownReason::SchedulerFailure),
            _ = shutdown_rx.recv() => {
                info!("> [RECV] Initiating graceful shutdown..");
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
                    "> [GOSSIPOD] Binding to all network interfaces: {}:{} (Private IP: {}:{})",
                    ip_addr.to_string(),
                    self.inner.config.port,
                    private_ip_addr.to_string(),
                    self.inner.config.port
                );
            } else {
                info!(
                    "> [GOSSIPOD] Binding to specific IP: {}:{}",
                    ip_addr.to_string(),
                    self.inner.config.port
                );
            }
        }
        Ok(())
    }

    async fn launch_listeners(mut listener: EventListener<M>) {
        tokio::spawn(async move {
            listener.run_listeners().await 
        });
    }

    fn launch_tcp_listener(
        transport: Arc<dyn NodeTransport>,
        backoff: BackOff,
        mut shutdown_rx: broadcast::Receiver<()>
    ) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                if backoff.is_circuit_open() {
                    warn!("Circuit breaker is opened due to previous consecutive failures");
                    continue
                }
                tokio::select! {
                    listener_result = transport.tcp_stream_listener() => {
                        match listener_result {
                            Ok(_) =>  backoff.record_success(),
                            Err(e) => {
                                let (failures, circuit_opened) = backoff.record_failure();
                                error!("TCP listener error: {}. Consecutive failures: {}", e, failures);
                                
                                if circuit_opened {
                                    warn!("Circuit breaker opened due to consecutive failures");
                                    return Err(anyhow!("TCP listener failed {} times consecutively", failures));
                                } else {
                                    let delay = backoff.calculate_delay();
                                    warn!("TCP listener restarting in {:?}", delay);
                                    
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
                    }
                    _ = shutdown_rx.recv() => {
                        warn!("[RECV] Shutdown signal received, stopping TCP listener");
                        return Ok(());
                    }
                }
            }
        })
    }
    
    fn launch_udp_listener(
        transport: Arc<dyn NodeTransport>,
        backoff: BackOff,
        mut shutdown_rx: broadcast::Receiver<()>
    ) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            loop {
                if backoff.is_circuit_open() {
                    warn!("Circuit breaker is opened due to previous consecutive failures");
                    continue
                }

                tokio::select! {
                    listener_result = transport.udp_socket_listener() => {
                        match listener_result {
                            Ok(_) =>  backoff.record_success(),
                            Err(e) => {
                                let (failures, circuit_opened) = backoff.record_failure();
                                error!("UDP listener error: {}. Consecutive failures: {}", e, failures);
                                
                                if circuit_opened {
                                    error!("Circuit breaker opened due to consecutive failures");
                                    return Err(anyhow!("UDP listener failed {} times consecutively", failures));
                                } else {
                                    let delay = backoff.calculate_delay();
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
                    }
                    _ = shutdown_rx.recv() => {
                        warn!("[RECV] Shutdown signal received, stopping UDP listener");
                        return Ok(());
                    }
                }
            }
        })
    }

    fn launch_probe_scheduler(&self, mut shutdown_rx: broadcast::Receiver<()>) -> tokio::task::JoinHandle<Result<()>> {
        let gossipod = Arc::new(self.clone());
        let backoff = Arc::new(BackOff::new());
        
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
            info!("Starting adaptive prober");

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal, stopping scheduler");
                        break;
                    }
                    _ = async {
                        if !gossipod.is_running().await {
                            info!("Gossipod is no longer running, stopping prober");
                            return;
                        }

                        if backoff.is_circuit_open() {
                            warn!("Circuit is open. Waiting before next probe attempt.");
                            time::sleep(backoff.reset_timeout).await;
                            return;
                        }

                        match gossipod.probe().await {
                            Ok(_) => {
                                debug!(
                                    "{}", 
                                    pretty_debug("Membership:", &gossipod.inner.members.get_all_nodes().unwrap()),
                                );

                                debug!("Probe completed successfully");
                                backoff.record_success();
                            }
                            Err(e) => {
                                error!("Probe error: {}", e);
                                let (failures, circuit_opened) = backoff.record_failure();
                                if circuit_opened {
                                    warn!("Circuit breaker opened after {} consecutive failures", failures);
                                }
                            }
                        }

                        let backoff_delay = backoff.calculate_delay();
                        let size_based_delay = if let Ok(member_count) = gossipod.inner.members.len() {
                            gossipod.inner.config.calculate_interval(
                                gossipod.inner.config.base_probing_interval,
                                member_count,
                            )
                        } else {
                            backoff_delay
                        };

                        let final_delay = std::cmp::max(backoff_delay, size_based_delay);
                        if final_delay != backoff_delay {
                            info!("Adjusting probe interval to {:?} based on cluster size", final_delay);
                        }
                        debug!("Waiting for {:?} before next probe", final_delay);
                        time::sleep(final_delay).await;
                    } => {}
                }
            }

            gossipod.stop().await?;
            Ok(())
        })
    }

    pub(crate) fn launch_event_scheduler(&self, mut shutdown_rx: broadcast::Receiver<()>) -> tokio::task::JoinHandle<Result<()>> {
        let gossipod = self.clone();
        
        tokio::spawn(async move {
            loop {
                let sleep_duration = gossipod.inner.event_scheduler.time_to_next_event().await.unwrap_or(Duration::from_millis(500));

                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {
                        let cluster_size = gossipod.inner.members.len().unwrap_or(gossipod.inner.config.initial_cluster_size);
                        // Calculate event processing limit to balance performance and resource usage:
                        // 1. Slow down processing if message volume is high
                        // 2. Events are created for most pings/ping-req so it's important to the drain channel quickly to reduce memory overhead
                        // 3. Avoid CPU spikes from processing too many messages at once
                        // 4. Scale with cluster size, but within reasonable bounds (100-1000)
                        // Note: This is a naive approach and may need tuning for optimal performance
                        let limit = std::cmp::min(
                            std::cmp::max(100, cluster_size), 
                            1000
                        );
                        let events = gossipod.inner.event_scheduler.next_events(limit).await;
                        for (event_type, event) in events {
                            match event_type {
                                EventType::Ack { sequence_number } => {
                                    info!("Ack with sequence number {} TIMED OUT", sequence_number);
                                    let _ = event.sender.try_send(event.state);
                                },
                                EventType::SuspectTimeout { node } => {
                                    info!("Moving Node {} to DEAD state", node);
                                    if let Ok(Some(node)) = gossipod.inner.members.get_node(&node) {
                                        if node.is_suspect() {
                                            if let Err(e)= gossipod.confirm_node_dead(&node).await {
                                                warn!("unable to confirm dead for node: {} because of: {}", node.name, e);
                                            }else {
                                                info!("Successfully Moved node {} to DEAD state", node.name);
                                            }
                                
                                        }
                                    };
                                },
                            }
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

    fn launch_gossip_scheduler(&self, mut shutdown_rx: broadcast::Receiver<()>) -> tokio::task::JoinHandle<Result<()>> {
        let gossipod = Arc::new(self.clone());
        let backoff = Arc::new(BackOff::new());
        
        tokio::spawn(async move {
            info!("Starting gossip scheduler");

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal, stopping gossip scheduler");
                        break;
                    }
                    _ = async {
                        if !gossipod.is_running().await {
                            info!("Gossipod is no longer running, stopping gossip scheduler");
                            return;
                        }

                        if backoff.is_circuit_open() {
                            warn!("Circuit is open. Waiting before next gossip attempt.");
                            time::sleep(backoff.reset_timeout).await;
                            return;
                        }

                        match gossipod.gossip().await {
                            Ok(_) => {
                                
                                if let Ok(len) = gossipod.inner.broadcasts.len() {
                                    debug!("Gossip completed successfully, messages left: {}", len);
                                }
                                backoff.record_success();
                            }
                            Err(e) => {
                                error!("Gossip error: {}", e);
                                let (failures, circuit_opened) = backoff.record_failure();
                                if circuit_opened {
                                    warn!("Circuit breaker opened after {} consecutive failures", failures);
                                }
                            }
                        }

                        let backoff_delay = backoff.calculate_delay();
                        let size_based_delay = if let Ok(member_count) = gossipod.inner.members.len() {
                            gossipod.inner.config.calculate_interval(
                                gossipod.inner.config.base_probing_interval,
                                member_count,
                            )
                        } else {
                            backoff_delay
                        };

                        let final_delay = std::cmp::max(backoff_delay, size_based_delay);
                        if final_delay != backoff_delay {
                            info!("Adjusting gossip interval to {:?} based on cluster size", final_delay);
                        }
                        debug!("Waiting for {:?} before next gossip", final_delay);
                        time::sleep(final_delay).await;
                    } => {}
                }
            }

            Ok(())
        })
    }
    
    /// Generate the next sequence number for message ordering.
    /// This method allows wrapping around to 0 when the maximum is reached.
    fn next_sequence_number(&self) -> u64 {
        self.inner.sequence_number.fetch_add(1, Ordering::SeqCst) + 1
    }

   /// Generate the next incarnation number for conflict resolution.
    fn next_incarnation(&self) -> u64 {
        self.inner.incarnation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Quickly advances the incarnation number using a random offset to avoid conflicts.
    /// The offset value is randomly chosen between 1 and 10.
    /// Returns the new incarnation number.
    fn advance_incarnation_with_offset(&self) -> u64 {
        loop {
            let current = self.inner.incarnation.load(Ordering::SeqCst);
            let new_incarnation = current + 1 + thread_rng().gen_range(0..10);
            
            if self.inner.incarnation.compare_exchange(current, new_incarnation, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return new_incarnation;
            }
        }
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
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;

        self.inner.net_svc.message_target(target, local_addr, msg).await?;
        Ok(())
    }

    // checks if server is running and checks if your have receiver initialized for 
    // accepting app-specific message
    pub async fn is_ready(&self) -> bool {
        self.inner.notifier.is_initialized().await && self.is_running().await
    }

    /// Gossipod PROBE - Probes randomly selected nodes
    ///
    /// This function implements the probing mechanism of the gossip protocol:
    /// 1. Selects a random node to probe, excluding the local node and dead nodes.
    /// 2. Sends a Ping message to the selected node.
    /// 3. Waits for an ACK response or a timeout.
    /// 4. Handles the response: updates node state or marks the node as suspect if no response.
    ///
    /// The function uses a combination of direct UDP messaging and an event-based
    /// system to handle asynchronous responses. If the direct UDP message fails,
    /// it falls back to indirect pinging through other nodes.
    pub(crate) async fn probe(&self) -> Result<()> {
        debug!("> start probing");
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
        let target_node =  self.inner.members.next_probe_node(Some(|n: &Node<M>| {
            n.socket_addr()
                .map(|addr| !(n.is_alive() || n.is_suspect()) || addr == local_addr)
                .unwrap_or(false)
        }))?;

        if let Some(node) = target_node {
            debug!("> picked a random node of name: {}", node.name);
            let sequence_number = self.next_sequence_number();
            let local_addr = local_node.socket_addr()?;
            let ping_payload = PingPayload {
                sequence_number,
                piggybacked_updates: vec![], 
            };
            let ping = Message {
                msg_type: MessageType::Ping,
                sender: local_addr,
                payload: MessagePayload::Ping(ping_payload),
            };
            debug!("> send ping: {:?}", ping);
            let message = self.encode_message_with_piggybacked_updates(ping).await?;
            let target = node.socket_addr()?;

            self.send_probe_and_respond(message.to_vec(), target, sequence_number, node).await?;
        } else {
            debug!("No target node to select for probing");
        }
        Ok(())
    }

    /// Sends a probe to the target node and handles the response.
    /// 
    /// The function sends a UDP message to the target node, schedules an event to wait for an acknowledgment,
    /// and handles the response within the specified deadlines. If no acknowledgment is received before the probe
    /// deadline, the node is marked as suspect.
    async fn send_probe_and_respond(&self, message_bytes: Vec<u8>, target: SocketAddr, sequence_number: u64, node: Node<M>) -> Result<()> {
        let now = Instant::now();
        let cluster_size = self.inner.members.len().unwrap_or(self.inner.config.initial_cluster_size);
        let probe_deadline = now+self.inner.config.calculate_interval(
            self.inner.config.base_probing_interval,
            cluster_size,
        );

        let ack_deadline = now + self.inner.config.ack_timeout;

        self.inner.net_svc.transport.write_to_udp(target, &message_bytes).await?;

        let (mut rx, _) = self.inner.event_scheduler.schedule_event(
            EventType::Ack { sequence_number }, 
            ack_deadline,
        ).await?;

        tokio::select! {
            event_state = rx.recv() => {
                self.handle_ack_response(event_state, sequence_number, node, probe_deadline).await?;
            },
            _ = tokio::time::sleep_until(probe_deadline) => {
                warn!("Probing deadline reached for node {}, proceeding to the next cycle.", node.name);
                self.handle_suspect_node(node).await?;
            }
        }

        Ok(())
    }
    
    /// Handles the acknowledgment response for a probe sent to a node.
    /// 
    /// This function processes the event state received in response to a probe. Depending on the event state,
    async fn handle_ack_response(&self, event_state: Option<EventState>, sequence_number: u64, node: Node<M>, probe_deadline: Instant) -> Result<()> {
        match event_state {
            Some(EventState::Intercepted) => {
                debug!("> Received ACK for probe to node {}", node.name);
            },
            Some(EventState::ReachedDeadline) => {
                self.handle_ack_timeout(sequence_number, node, probe_deadline).await?;
            },
            Some(other_state) => {
                warn!("> Unexpected event state: {:?}", other_state);
            },
            None => {
                warn!("> Event channel closed unexpectedly for node {}", node.name);
            }
        }
        Ok(())
    }

    async fn handle_ack_timeout(
        &self,
        sequence_number: u64,
        node: Node<M>,
        probe_deadline: Instant,
    ) -> Result<()> {
        warn!("> Probe to node {} timed out without ACK, proceeding to PING-REQ", node.name);
        let local_addr = self.get_local_node().await?.socket_addr()?;
        self.send_indirect_pings(local_addr, sequence_number, node.socket_addr()?).await?;

        let now = Instant::now();
        let indirect_ack_deadline = now + self.inner.config.indirect_ack_timeout;
        let (mut rx, _)  = self.inner.event_scheduler.schedule_event(
            EventType::Ack{ sequence_number }, 
            indirect_ack_deadline,
        ).await?;

        tokio::select! {
            event_state = rx.recv() => {
                self.handle_indirect_ack_response(event_state, node).await?;
            },
            _ = tokio::time::sleep_until(probe_deadline) => {
                warn!("Probing deadline reached for node {}, proceeding to the next cycle.", node.name);
                self.handle_suspect_node(node).await?;
            }
        }

        Ok(())
    }

    async fn handle_indirect_ack_response(
        &self,
        event_state: Option<EventState>,
        node: Node<M>,
    ) -> Result<()> {
        match event_state {
            Some(EventState::Intercepted) => {
                debug!("> Received ACK for probe to node {}", node.name);
            },
            Some(EventState::ReachedDeadline) => {
                self.handle_suspect_node(node).await?;
            },
            Some(other_state) => {
                warn!("> Unexpected event state: {:?}", other_state);
            },
            None => {
                warn!("> Event channel closed unexpectedly for node {}", node.name);
            }
        }
        Ok(())
    }

    async fn handle_suspect_node(&self, node: Node<M>) -> Result<()> {
        self.suspect_node(node.incarnation(), &node.name).await?;
        
        let now = Instant::now();
        let cluster_size = self.inner.members.len().map_or(1, |n| n);
        let suspect_deadline = now + self.inner.config.suspicious_timeout(cluster_size);

        self.inner.event_scheduler.schedule_event(
            EventType::SuspectTimeout { node: node.name }, 
            suspect_deadline,
        ).await?;
    
        Ok(())
    }
    async fn send_indirect_pings(&self, local_addr: SocketAddr,sequence_number: u64, target: SocketAddr) -> Result<()> {
        debug!(">> send indirect pings");
        let known_nodes = self.inner.members.select_random_probe_nodes(INDIRECT_REQ, Some(|n: &Node<M>| {
            !n.is_alive() || n.socket_addr().map_or(true, |addr| addr == local_addr || addr == target)
        })).unwrap_or_default();
    
        if known_nodes.is_empty() {
            debug!("> no nodes available for indirect pings");
            return Ok(());
        }
    
        let ping_req_payload = PingReqPayload {
            sequence_number,
            target,
            // We'll add updates later if packet size is within mimium packet size
            piggybacked_updates: vec![],
        };
    
        let ping_req = Message {
            msg_type: MessageType::PingReq,
            sender: local_addr,
            payload: MessagePayload::PingReq(ping_req_payload),
        };
        
        for node in &known_nodes {
            let message_bytes = self.encode_message_with_piggybacked_updates(ping_req.clone()).await?;
    
            if let Err(e) = self.inner.net_svc.transport.write_to_udp(node.socket_addr()?, &message_bytes).await {
                warn!("Failed to send indirect ping to {} for target {}: {}", node.name, target, e);
            } else {
                debug!("Sent indirect ping to {} for target {}", node.name, target);
            }
        }
    
        debug!(
            "Sent indirect pings to {} nodes for target {}",
            known_nodes.len(), 
            target,
        );
        Ok(())
    }

    async fn encode_message_with_piggybacked_updates(&self, item: Message) -> Result<BytesMut> {
        let local_node = self.get_local_node().await?;
        let mut message_bytes = BytesMut::new();

        MessageCodec::encode_u8(item.msg_type as u8, &mut message_bytes)?;
        MessageCodec::encode_socket_addr(&local_node.socket_addr()?, &mut message_bytes)?;

        let mut payload_bytes = BytesMut::new();
        MessageCodec::encode_message_payload(&item.payload, &mut payload_bytes)?;

        // Reserve space for number of piggybacked updates
        let updates_count_index = payload_bytes.len();
        payload_bytes.put_u32(0);

        let mut updates_count = 0;
        let mut least_piggybacked_iter = self.inner.members.least_recently_piggybacked_iter();
        while let Some(node) = least_piggybacked_iter.next() {
            let remote_node = RemoteNode {
                name: node.name.clone(),
                address: node.socket_addr()?,
                metadata: node.metadata.to_bytes()?,
                state: node.state().clone(),
                incarnation: node.incarnation(),
            };
            
            let mut temp_bytes = BytesMut::new();
            MessageCodec::encode_remote_node(&remote_node, &mut temp_bytes)?;

            if message_bytes.len() + payload_bytes.len() + temp_bytes.len() + 4 <= MAX_UDP_PACKET_SIZE {
                payload_bytes.extend_from_slice(&temp_bytes);
                updates_count += 1;
            } else if updates_count == 0 && message_bytes.len() + payload_bytes.len() < MAX_UDP_PACKET_SIZE {
                // If no updates yet and this one doesn't fit, add it anyway if there's any space
                payload_bytes.extend_from_slice(&temp_bytes);
                updates_count += 1;
                break;
            } else {
                // This update doesn't fit, and we already have some updates anyway, so we're done
                break;
            }
        }

        // Update the number of piggybacked updates
        payload_bytes[updates_count_index..updates_count_index + 4].copy_from_slice(&(updates_count as u32).to_be_bytes());

        // Add payload length and payload to message_bytes
        MessageCodec::encode_bytes(&payload_bytes, &mut message_bytes)?;

        Ok(message_bytes)
    }

    /// Gossipod GOSSIP - Gossip with randomly selected nodes
    pub(crate) async fn gossip(&self) -> Result<()> {
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;

        let dead_node_gossip_window = self.inner.config.dead_node_gossip_window();
        let known_nodes = self.inner.members.select_random_gossip_nodes(
            BROADCAST_FANOUT,
            Some(|n: &Node<M>| {
                n.socket_addr().map_or(true, |addr| addr == local_addr)
                || (n.is_dead() && !n.is_within_dead_gossip_window(dead_node_gossip_window).unwrap_or(false))
            })
        ).unwrap_or_default();

        if known_nodes.is_empty() {
            debug!("No known nodes to gossip to , messages {:?}", self.inner.broadcasts.len()?);
            return Ok(());
        }

        let mut messages: Vec<(String , BytesMut)> = Vec::new();
        let mut current_byte_count = 0;

        while let Ok(Some((key,broadcast))) = self.inner.broadcasts.pop() {
            let message = Message {
                msg_type: MessageType::Broadcast,
                payload: MessagePayload::Broadcast(broadcast.clone()),
                sender: local_addr,
            };

            let mut codec = MessageCodec::new();
            let mut buffer: BytesMut = BytesMut::new();
            codec.encode(message, &mut buffer)?;

            if buffer.len() + current_byte_count > MAX_UDP_PACKET_SIZE {
                break
            }
            current_byte_count +=  buffer.len();
            messages.push((key, buffer));
        }

        if messages.is_empty() {
            debug!("No messages in the outbound queue to gossip");
            return Ok(());
        }

        debug!("Nodes to gossip: {:?}", known_nodes.len());
        for node in &known_nodes {
            for (key, broadcast) in &messages {
                match self.inner.transport.write_to_udp(node.socket_addr()?, broadcast).await {
                    Ok(_) => {
                        debug!("Gossiped message to {}", node.name);
                    },
                    Err(e) => {
                        warn!("Failed to gossip message to {}: {}", node.name, e);
                        self.inner.broadcasts.decrement_retransmit(key.to_string())?;
                    }
                }
            }
        }

        debug!("Gossiped {} messages to {} nodes", messages.len(), known_nodes.len());
        Ok(())
    }
    /// Gossipod LEAVE - Notifies nodes of a peer leaving 
    pub async fn leave(&self) -> Result<()> {
        let incarnation = self.next_incarnation();
        let local_node = self.get_local_node().await?;
        let broadcast = Broadcast::Leave {  
            member: local_node.name,
            incarnation,
        };

        debug!("Leave broadcast: {:?}", broadcast);
        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
        Ok(())

    }

    async fn handle_ping(&self, sender: SocketAddr, message_payload: MessagePayload) -> Result<()> {
        match message_payload {
            MessagePayload::Ping(payload) => {
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = this.send_ack(sender, payload.sequence_number).await {
                        error!("Failed to send ACK: {}", e);
                    }
                });

                if !payload.piggybacked_updates.is_empty() {
                    let this = self.clone();
                    let updates = payload.piggybacked_updates;
                    tokio::spawn(async move {
                        if let Err(e) = this.handle_piggybacked_updates(updates).await {
                            error!("Failed to handle piggybacked updates: {}", e);
                        }
                    });
                }
            },
            _ => {
                warn!("Received Non-Ping message in handle_ping from {}", sender);
                return Err(anyhow!("Unexpected message type in handle_ping"));
            }
        }
        Ok(())
    }
    async fn send_ack(&self, target: SocketAddr,  sequence_number: u64) -> Result<()> {
        // Send ACK response
        let ack_payload = AckPayload {
            sequence_number,
            piggybacked_updates: vec![], // We'll add updates later if space permits
        };

        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
    
        let ack = Message {
            msg_type: MessageType::Ack,
            sender: local_addr,
            payload: MessagePayload::Ack(ack_payload),
        };
        let message_bytes = self.encode_message_with_piggybacked_updates(ack.clone()).await?;
        if let Err(e) = self.inner.net_svc.transport.write_to_udp(target, &message_bytes).await {
            warn!("Failed to send ack to {}: {}", target, e);
        } else {
            debug!("Sent ACK response to {}", target);
        }
        Ok(())
    }
    async fn handle_ping_req(&self, sender: SocketAddr, message_payload: MessagePayload) -> Result<()> {
        match message_payload {
            MessagePayload::PingReq(payload) => {
                let this = self.clone();
                tokio::spawn( async move {
                    // do-update in the background
                    if payload.piggybacked_updates.len() > 0 {
                        _ = this.handle_piggybacked_updates(payload.piggybacked_updates).await;
                    }
                });

                let local_node = self.get_local_node().await?;
                let local_addr = local_node.socket_addr()?;
                
                // Prepare the Ping message
                let sequence_number = self.next_sequence_number();
                let ping_payload = PingPayload {
                    sequence_number,
                    piggybacked_updates: vec![], 
                };
                let ping = Message {
                    msg_type: MessageType::Ping,
                    sender: local_addr,
                    payload: MessagePayload::Ping(ping_payload),
                };
                debug!("> send ping: {:?}", ping);

                // Add prioritized updates to the message
                let message_bytes = self.encode_message_with_piggybacked_updates(ping).await?;
                let original_target = payload.target;
                let original_sequence_number = payload.sequence_number;

                // Set up the probing deadline and create an event for ACK tracking
                let now = Instant::now();
                let cluster_size = self.inner.members.len().unwrap_or(self.inner.config.initial_cluster_size);
                let probe_deadline = now + self.inner.config.calculate_interval(
                    self.inner.config.base_probing_interval,
                    cluster_size,
                );
    
                let ack_deadline = now + self.inner.config.ack_timeout;
                let (mut rx, _) = self.inner.event_scheduler.schedule_event(
                    EventType::Ack{ 
                        sequence_number  
                    },
                    ack_deadline,
                ).await?;

                // Send the UDP PING-REQ message.
                if let Err(e) = self.inner.net_svc.transport.write_to_udp(original_target, &message_bytes).await {
                    error!("[ERR] Failed to send UDP PING-REQ: {}", e);
                    return Ok(());
                }

                tokio::select! {
                    event_state = rx.recv() => {
                        match event_state {
                            Some(EventState::Intercepted) => {
                                debug!(
                                    ">> Received ACK for Indriect PING to target {}", 
                                    original_target,
                                );
                                self.send_ack(sender, original_sequence_number).await?;
                            },
                            Some(EventState::ReachedDeadline) => {
                                warn!(">> Indirect PING to target {} reached deadline", original_target);
                            },
                            Some(other_state) => {
                                warn!(">> Unexpected event state: {:?}", other_state);
                            },
                            None => {
                                warn!(">> Event channel closed unexpectedly for target {}", original_target);
                            }
                        }
                    },
                    _ = tokio::time::sleep_until(probe_deadline) => {
                        warn!(">> Indirect PING deadline reached for target {}", original_target);
                    }
                }
            },
            _ => {
                warn!("Received non-Ping-Req message in PING_REQ from {}", sender);
                return Err(anyhow!("Unexpected message type in PING_REQ"));
            }
        }
        Ok(())
    }

    /// Handles incoming ACK (Acknowledgement).
    ///
    /// This function is responsible for processing ACK messages, which are crucial for:
    /// confirming successful communication with other nodes, updating the future event before they reach deadline
    /// about received ACKs, and for processing any piggybacked updates that come with the ACK.
    ///
    async fn handle_ack(&self, message_payload: MessagePayload) -> Result<()> {
        match message_payload {
            MessagePayload::Ack(payload) => {
                let _ = self.inner.event_scheduler.try_intercept_event(&EventType::Ack{
                    sequence_number: payload.sequence_number
                }).await;
            
                if payload.piggybacked_updates.len() > 0 {
                    self.handle_piggybacked_updates(payload.piggybacked_updates).await?;
                }
            },
            _ => {}
        }
       Ok(())
    }
    
    async fn handle_app_msg(&self, message_payload: MessagePayload) -> Result<()> {
        match message_payload {
            MessagePayload::AppMsg(payload) => {
                if !self.inner.notifier.is_initialized().await {
                    warn!("New message but no receiver is set, ignoring app message.");
                    return Ok(());
                }

                self.inner.notifier.notify(payload.data).await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Processes incoming broadcast messages.
    ///
    /// This function is a critical component responsible for handling various types
    /// of broadcast messages that propagate information about cluster membership and 
    /// node states.
    pub(crate) async fn handle_broadcast(&self, message: Message) -> Result<()> {
        match message.payload {
            MessagePayload::Broadcast(broadcast) => {
                match broadcast {
                    Broadcast::Suspect { incarnation, member } => {
                        debug!("Received SUSPECT broadcast for member: {}", member);
                        self.suspect_node(incarnation, &member).await?;
                    },
                    Broadcast::Join { member } => {
                        debug!("Received JOIN broadcast for member: {}", member.name);
                        if let Err(e) = self.integrate_new_node(member).await {
                            warn!("unable to handle join node: {}", e.to_string())
                        }
                    },
                    Broadcast::Leave { incarnation, member } => {
                        debug!("Received LEAVE broadcast for member: {}", member);
                        if let Err(e) = self.process_node_departure(incarnation, &member).await {
                            warn!("unable to handle leave node request")
                        }
                    },
                    Broadcast::Confirm { incarnation, member } => {
                        debug!("Received CONFIRM broadcast for member: {}", member);
                        self.confirm_node(incarnation, &member).await?;
                    },
                    Broadcast::Alive { incarnation, member } => {
                        debug!("Received ALIVE broadcast for member: {}", member);
                        self.update_node_liveness(incarnation, &member).await?;
                    },
                }
                Ok(())
            },
            _ => Err(anyhow!("Expected Broadcast payload, got {:?}", message.payload)),
        }
    }

    pub(crate) async fn suspect_node(&self, incarnation: u64, member: &str) -> Result<()> {
        // If the suspect message is about ourselves, we need to refute it
        if member == self.inner.config.name() {
            return self.refute_suspicion(incarnation).await;
        }

        if let Ok(Some(node)) = self.inner.members.get_node(member) {
            let mut suspect_node = node.clone();
            suspect_node.update_state(NodeState::Suspect)?;
            suspect_node.set_incarnation(incarnation);

            match self.inner.members.merge(&suspect_node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Updated => {
                            debug!("Node {} is now SUSPECTED", member);
                            let broadcast = Broadcast::Suspect {
                                incarnation, 
                                member: suspect_node.name
                            };
                            self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                        },
                        MergeAction::Unchanged => {
                            debug!("Node {} was already SUSPECTED", member);
                        },
                        _ => {
                            warn!("Unexpected merge result when suspecting node {}", member);
                        }
                    }
                },
                Err(e) => {
                    info!("Unable to merge node assert_eq{}: {}", member, e);
                },
            }
        } else {
            warn!("Received suspect message for unknown node: {}", member);
        }

        Ok(())
    }

    async fn refute_suspicion(&self, incarnation: u64) -> Result<()> {
        let mut local_node = self.get_local_node().await?;
        let next_incarnation = std::cmp::max(incarnation + 1, self.advance_incarnation_with_offset());
        local_node.set_incarnation(next_incarnation);
        local_node.update_state(NodeState::Alive)?;
    
        info!(
            "Refuting suspicion with new incarnation: {}. Delta: {}",
            local_node.incarnation(),
            next_incarnation,
        );
        
        self.inner.members.merge(&local_node)?;
        let broadcast = Broadcast::Alive {
            incarnation: local_node.incarnation(),
            member: local_node.name.clone(),
        };
        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
    
        Ok(())
    }

    async fn broadcast_join(&self, node: Node<M>) -> Result<()> {
        let broadcast = Broadcast::Join {  
            member: RemoteNode { 
                name: node.name.clone(), 
                address: node.socket_addr()?, 
                metadata: node.metadata.to_bytes()?, 
                state: node.state().clone(), 
                incarnation: node.incarnation(),
            },
        };

        let key = format!("join:{}", broadcast.get_key()).to_string();
        self.inner.broadcasts.upsert(key, broadcast)?;

        Ok(())
    }

    async fn confirm_node_dead(&self, node: &Node<M>) -> Result<()> {
        let mut dead_node = node.clone();
        dead_node.update_state(NodeState::Dead)?;
        
        let broadcast = Broadcast::Confirm {
            incarnation: node.incarnation(),
            member: node.name.clone(),
        };

        match self.inner.members.merge(&dead_node) {
            Ok(merge_result) => {
                match merge_result.action {
                    MergeAction::Updated => {
                        if let Some(old_state) = merge_result.old_state {
                            debug!("Node {} state changed from {:?} to {:?}", dead_node.name, old_state, merge_result.new_state);
                        }
                    },
                    _ => {}
                }
            },
            Err(e) => {
                error!("Unable to merge node {}: {}", dead_node.name, e);
            },
        }
       
        if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
            dispatch_handler.notify_dead(&node).await?;
        }
        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)

    }

    async fn set_local_node_liveness(&self) -> Result<()> {
        let ip_addr = self.inner.config.addr();
        let name = self.inner.config.name();
        let port = self.inner.config.port();
        let incarnation = self.next_incarnation();
        let node = Node::with_state(
            NodeState::Alive, 
            ip_addr, 
            port, 
            name.clone(), 
            incarnation, 
            self.inner.metadata.clone(),
        );
       
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
                error!("Unable to merge node {}: {}", name, e);
            },
        }

        Ok(())
    }

    pub(crate) async fn integrate_new_node(&self, member: RemoteNode) -> Result<()> {
        debug!(">>>> Integrate new node");
        let _ = self.inner.event_scheduler.try_intercept_event(&EventType::SuspectTimeout{node: member.name.clone()}).await;

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

        debug!(">> Processing join for node: {:?}", new_node);

        // Attempt to merge the new node into our membership list
        match self.inner.members.merge(&new_node) {
            Ok(merge_result) => {
                match merge_result.action {
                    MergeAction::Added => {
                        info!("Added new node {} to the cluster", member.name);
                        self.broadcast_join(new_node).await?;
                    },
                    MergeAction::Updated => {
                        info!("Updated existing node {} in the cluster", member.name);
                        if let Some(old_state) = merge_result.old_state {
                            if old_state != merge_result.new_state {
                                debug!(
                                    "Node {} state changed from {:?} to {:?}",
                                    member.name, old_state, merge_result.new_state
                                );

                                if let Some(old_state)= merge_result.old_state {
                                    if old_state != NodeState::Alive {
                                        let broadcast = Broadcast::Suspect {
                                            incarnation: new_node.incarnation(), 
                                            member: new_node.name
                                        };
                                        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                                    }
                                }
                            }
                        }
                    },
                    MergeAction::Unchanged => {
                        debug!(">> No changes for node {} in the cluster", member.name);
                    },
                    _ => {}
                }
            },
            Err(e) => {
                warn!("Failed to merge node {} into cluster: {}", member.name, e);
                return Err(e.into());
            }
        }

        debug!(">> {}", pretty_debug("Membership:", &self.inner.members.get_all_nodes()?));

        Ok(())
    }

    async fn process_node_departure(&self, incarnation: u64, member: &str) -> Result<()> {
        debug!(">> Leave incarnation: {}", incarnation);

        if let Ok(Some(node)) = self.inner.members.get_node(member) {
            let mut leave_node = node.clone();
            leave_node.update_state(NodeState::Leaving)?;
            leave_node.set_incarnation(incarnation);

            match self.inner.members.merge(&leave_node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Removed => {
                            info!("State changed({:?}) for node {} in the cluster", merge_result.action , node.name);
                            let broadcast = Broadcast::Leave {  
                                incarnation,
                                member: member.to_string(),
                            };
                            self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
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

    async fn confirm_node(&self, incarnation: u64, member: &str) -> Result<()> {
        let local_node = self.get_local_node().await?;

        // If it's about us, we need to refute
        if member == local_node.name {
            return self.refute_node(incarnation).await;
        }

        if let Ok(Some(node)) = self.inner.members.get_node(member) {
            let mut confim_node = node.clone();
            confim_node.update_state(NodeState::Dead)?;
            confim_node.set_incarnation(incarnation);

            match self.inner.members.merge(&confim_node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Updated => {
                            info!("Node {} confirmed dead. Updating state.", node.name);
                            let broadcast = Broadcast::Confirm {  
                                incarnation: confim_node.incarnation(), 
                                member: member.to_string(),
                            };
                            self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
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

    /// Refutes a node's death by updating its incarnation and broadcasting an Alive message.
    async fn refute_node(&self, incarnation: u64) -> Result<()> {
        let local_node = self.get_local_node().await?;
        let new_incarnation = std::cmp::max(incarnation + 1, self.next_incarnation());
        
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

        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
        info!(
            "Refuted death confirmation by broadcasting Alive message with new incarnation {}", 
            new_incarnation,
        );
        
        Ok(())
    }

    // Update Node liveness is responsible for handling ALIVE node when an ALIVE Broadcast received
    // it uses the central membership merge funciton to resolve conflicting data and return merge result accordingly
    pub(crate) async fn update_node_liveness(&self, incarnation: u64, member: &str) -> Result<()> {
        debug!(">> {}", pretty_debug("Membership:", &self.inner.members.get_all_nodes()?));


        match self.inner.members.get_node(member)? {
            Some(node) => {
                let mut alive_node = node.clone();
                alive_node.update_state(NodeState::Alive)?;
                alive_node.set_incarnation(incarnation);

                match self.inner.members.merge(&alive_node) {
                    Ok(merge_result) => {
                        match merge_result.action {
                            MergeAction::Updated => {
                                info!("Node {} is alive with incarnation {}", member, incarnation);
                                
                                // Only disseminate if the node was previously suspected or dead
                                if let Some(old_state)= merge_result.old_state {
                                    if old_state != NodeState::Alive {
                                        let broadcast = Broadcast::Suspect {
                                            incarnation: alive_node.incarnation(), 
                                            member: alive_node.name
                                        };
                                        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
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

    /// Notifies nodes of a peer joining the network
    pub async fn join(&self, target: SocketAddr) -> Result<()> {
        debug!("Initiating join process with initial target: {}", target);

        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;
        let broadcast = Broadcast::Join {  
            member: RemoteNode { 
                name: local_node.name.clone(),
                address: local_addr,
                metadata: local_node.metadata.to_bytes()?, 
                state: local_node.state().clone(), 
                incarnation: local_node.incarnation(),
            },
        };

        self.inner.net_svc.broadcast(target, local_addr, broadcast.clone()).await
            .context("failed to send join broadcast to initial contact")?;

        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
        // debug!(
        //     "{}", 
        //     pretty_debug("Membership:", &self.inner.members.get_all_nodes()?),
        // );

        Ok(())
    }

    async fn handle_piggybacked_updates(&self, updates: Vec<RemoteNode>) -> Result<()> {
        debug!("Processing new piggybacked updates");
    
        let converted_nodes: Result<Vec<Node<M>>> = updates.into_iter()
            .map(|payload| {
                let metadata = M::from_bytes(&payload.metadata)
                    .context("failed to deserialize node metadata")?;
                let node = Node::with_state(
                    payload.state,
                    payload.address.ip(),
                    payload.address.port(),
                    payload.name,
                    payload.incarnation,
                    metadata
                );
    
                Ok(node)
            })
            .collect();
    
        let converted_nodes = converted_nodes?;
        for node in converted_nodes {
            match self.inner.members.merge(&node) {
                Ok(merge_result) => {
                    match merge_result.action {
                        MergeAction::Added => {
                            info!("New node {} added to the cluster", node.name);
                            let broadcast = Broadcast::Join {
                                member: RemoteNode {
                                    name: node.name.clone(),
                                    address: node.socket_addr()?,
                                    metadata: node.metadata.to_bytes()?,
                                    state: node.state().clone(),
                                    incarnation: node.incarnation(),
                                },
                            };
                            self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                        },
                        MergeAction::Updated => {
                            info!("State changed for node {} in the cluster", node.name);
                            if let Some(old_state) = merge_result.old_state {
                                match (old_state, node.state()) {
                                    (NodeState::Alive, NodeState::Suspect) | (NodeState::Alive, NodeState::Dead) => {
                                        let broadcast = Broadcast::Suspect {
                                            incarnation: node.incarnation(),
                                            member: node.name.clone(),
                                        };
                                        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                                    },
                                    (NodeState::Suspect, NodeState::Dead) => {
                                        let broadcast = Broadcast::Confirm {
                                            incarnation: node.incarnation(),
                                            member: node.name.clone(),
                                        };
                                        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                                    },
                                    (_, NodeState::Alive) => {
                                        let broadcast = Broadcast::Alive {
                                            incarnation: node.incarnation(),
                                            member: node.name.clone(),
                                        };
                                        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
                                    },
                                    _ => {
                                        debug!("No broadcast needed for state change: {:?} -> {:?}", old_state, node.state());
                                    }
                                }
                            }
                        },
                        MergeAction::Unchanged => {
                            debug!("No changes for node {} in the cluster", node.name);
                        },
                        MergeAction::Removed => {
                            info!("Node {} removed from the cluster", node.name);
                            let broadcast = Broadcast::Leave {
                                incarnation: node.incarnation(),
                                member: node.name.clone(),
                            };
                            self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
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
        self.inner.members.get_nodes(Some(|n: &Node<M>| !n.is_alive()))
    }
    
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.inner.state.write();
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
        matches!(*self.inner.state.read(), GossipodState::Running)
    }

    async fn set_state(&self, gossipod_state: GossipodState) -> Result<()> {
        let mut state = self.inner.state.write();
        *state = gossipod_state;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use crate::mock_transport::{MockTransport, create_mock_node};
    use crate::config::GossipodConfigBuilder;
    use crate::message::{Broadcast, MessagePayload};
    use std::time::Duration;


    #[tokio::test]
    async fn test_suspect_refutation() -> Result<()> {
        let config = GossipodConfigBuilder::new()
            .name("local_node".to_string())
            .port(8000)
            .build()
            .await?;

        let (mock_transport, _) = MockTransport::new(
            8000,
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            Duration::from_secs(1)
        );
        let mock_transport = Arc::new(mock_transport);

        let gossipod = Gossipod {
            inner: Arc::new(InnerGossipod {
                config: config.clone(),
                metadata: DefaultMetadata::default(),
                members: Membership::new(),
                transport: mock_transport.clone(),
                state: RwLock::new(GossipodState::Running),
                shutdown: broadcast::channel(1).0,
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(10),
                event_scheduler: EventScheduler::new(),
                notifier: Arc::new(Notifier::new()),
                net_svc: NetSvc::new(mock_transport.clone()),
                broadcasts: Arc::new(DefaultBroadcastQueue::new(1))
            }),
        };

        // Add local node to membership
        let local_node = create_mock_node("local_node", "127.0.0.1", 8000, NodeState::Alive);
        gossipod.inner.members.merge(&local_node)?;

        // Add other nodes to membership
        let other_nodes = vec![
            create_mock_node("node1", "127.0.0.1", 8001, NodeState::Alive),
            create_mock_node("node2", "127.0.0.1", 8002, NodeState::Alive),
        ];

        for node in other_nodes {
            gossipod.inner.members.merge(&node)?;
        }

        // Simulate receiving a SUSPECT broadcast about the local node
        let suspect_broadcast = Broadcast::Suspect {
            incarnation: 15,
            member: "local_node".to_string(),
        };

        let suspect_message = Message {
            msg_type: MessageType::Broadcast,
            sender: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            payload: MessagePayload::Broadcast(suspect_broadcast),
        };

        // Handle the SUSPECT broadcast
        gossipod.handle_broadcast(suspect_message).await?;
        
        gossipod.gossip().await?;

        // Check that an ALIVE broadcast was sent to refute the suspicion
        let udp_messages = mock_transport.get_udp_messages().await;
        assert!(!udp_messages.is_empty(), "No UDP messages were sent");

        let (_, message_bytes) = &udp_messages[0];
        let message = Transport::read_socket(message_bytes.to_vec()).await?;

        match message.payload {
            MessagePayload::Broadcast(Broadcast::Alive { incarnation, member }) => {
                assert_eq!(member, "local_node", "ALIVE broadcast was not for the local node");
                assert!(incarnation > 15, "New incarnation should be greater than the received incarnation");
            },
            _ => panic!("Expected ALIVE broadcast, got {:?}", message.payload),
        }

        // Check that the local node's state and incarnation were updated
        let updated_local_node = gossipod.inner.members.get_node("local_node")?.unwrap();
        assert!(updated_local_node.incarnation() > 15, "Local node incarnation should be greater than the received incarnation");
        assert_eq!(updated_local_node.state(), NodeState::Alive, "Local node state should still be ALIVE");

        Ok(())
    }

    #[tokio::test]
    async fn test_ping_req_process() -> Result<()> {
        let config = GossipodConfigBuilder::new()
            .name("local_node".to_string())
            .probing_interval(Duration::from_millis(1_000))
            .ack_timeout(Duration::from_millis(500))
            .indirect_ack_timeout(Duration::from_secs(1))
            .suspicious_timeout(Duration::from_secs(5))
            .port(8000)
            .build()
            .await?;

        let (mock_transport, _) = MockTransport::new(
            8000,
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            Duration::from_secs(1)
        );
        let mock_transport = Arc::new(mock_transport);
        let (shutdown_tx,_) = broadcast::channel(1);

        let gossipod = Gossipod {
            inner: Arc::new(InnerGossipod {
                config: config.clone(),
                metadata: DefaultMetadata::default(),
                members: Membership::new(),
                transport: mock_transport.clone(),
                state: RwLock::new(GossipodState::Running),
                shutdown: shutdown_tx.clone(),
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(10),
                event_scheduler: EventScheduler::new(),
                notifier: Arc::new(Notifier::new()),
                net_svc: NetSvc::new(mock_transport.clone()),
                broadcasts: Arc::new(DefaultBroadcastQueue::new(1))
            }),
        };

        // Add nodes to membership
        let local_node = create_mock_node("local_node", "127.0.0.1", 8000, NodeState::Alive);
        let target_node = create_mock_node("target_node", "127.0.0.1", 8001, NodeState::Alive);
        let intermediate_node = create_mock_node("intermediate_node", "127.0.0.1", 8002, NodeState::Alive);
        
        gossipod.inner.members.merge(&local_node)?;
        gossipod.inner.members.merge(&target_node)?;
        gossipod.inner.members.merge(&intermediate_node)?;

        // Simulate receiving a PING-REQ
        let ping_req_payload = PingReqPayload {
            sequence_number: 1,
            target: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            piggybacked_updates: vec![],
        };
        
        let ping_req_message = Message {
            msg_type: MessageType::PingReq,
            sender: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002),
            payload: MessagePayload::PingReq(ping_req_payload.clone()),
        };

        let gossipod_clone2 = gossipod.clone();
        tokio::spawn(async move {
            // Handle the PING-REQ
            let _= gossipod_clone2.handle_ping_req(ping_req_message.sender, ping_req_message.payload).await;
        });
        time::sleep(Duration::from_millis(200)).await;

        // Check that a PING was sent to the target node
        let udp_messages = mock_transport.get_udp_messages().await;
        assert!(!udp_messages.is_empty(), "No UDP messages were sent");

        let (addr, message_bytes) = &udp_messages[0];
        assert_eq!(*addr, ping_req_payload.target, "PING was not sent to the correct target");

        let message = Transport::read_socket(message_bytes.to_vec()).await?;
        assert_eq!(message.msg_type, MessageType::Ping, "Message sent was not a PING");

        // Simulate receiving an ACK from the target node
        let ack_payload = AckPayload {
            sequence_number: 1,
            piggybacked_updates: vec![],
        };
        
        let ack_message = Message {
            msg_type: MessageType::Ack,
            sender: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            payload: MessagePayload::Ack(ack_payload),
        };

        gossipod.handle_ack(ack_message.payload).await?;
        time::sleep(Duration::from_millis(100)).await;

        let udp_messages = mock_transport.get_udp_messages().await;
        assert!(udp_messages.len() >= 2, "ACK was not sent");


        // Check that an ACK was sent back to the intermediate node
        let udp_message = mock_transport.get_last_udp_message().await;
        let (addr, message_bytes) = &udp_message.expect( "ACK was not sent back to the intermediate node");
        
        assert_eq!(*addr, ping_req_message.sender, "ACK was not sent to the correct intermediate node");

        let message = Transport::read_socket(message_bytes.to_vec()).await?;
        assert_eq!(message.msg_type, MessageType::Ack, "Message sent was not an ACK");

        Ok(())
    }
}
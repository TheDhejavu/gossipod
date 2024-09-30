#![allow(unused_variables)]
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, Context as _, Result};
use codec::MessageCodec;
use futures::Stream as _;
use runtime::{ActorId, GossipodCommand, GossipodRuntime};
use gossipod_runtime::{RuntimeExt};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt};
use tokio::macros::support::poll_fn;
pub use broadcast_queue::{BroadcastQueue, DefaultBroadcastQueue};
pub use  dispatch_event_handler::DispatchEventHandler;
use rand::{thread_rng, Rng};
use config::{BROADCAST_FANOUT, INDIRECT_REQ, MAX_UDP_PACKET_SIZE};
use event_scheduler::{Event, EventState, EventStream, EventType};
use tracing::{info, warn, error, debug, instrument};
use members::MergeAction;
use message::{AckPayload, AppMsgPayload, Broadcast, MessagePayload, MessageType, PingPayload, PingReqPayload};
pub use node::{DefaultMetadata, NodeMetadata};
use std::pin::pin;
use tokio::sync::broadcast;
use parking_lot::RwLock;
use tokio::time::{self, Instant};
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use transport::{ListenerEvent, TcpConnectionListener};
use utils::pretty_debug;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use crate::{
    config::GossipodConfig,
    event_scheduler::EventScheduler,
    members::Membership,
    message::{Message, RemoteNode},
    backoff::BackOff,
    state::NodeState,
};
pub use transport::{DatagramTransport, Datagram, DefaultTransport};
pub use ip_addr::IpAddress;
pub use node::Node;

mod transport;
mod message;
mod ip_addr;
pub mod config;
mod state;
mod node;
mod members;
mod codec;
mod backoff;
mod event_scheduler;
mod utils;
mod broadcast_queue;
mod mock_transport;
mod runtime;
mod dispatch_event_handler;

// SWIM Protocol Implementation for GOSSIPOD

/// This module implements an asynchronous SWIM (Scalable Weakly-consistent
/// Infection-style Membership) protocol. The implementation is modularized
/// for clarity, maintainability, and potential code reuse:
///
/// * Gossipod: Core protocol logic handler that manages messaging requests from 
/// cluster nodes and executes protocol-specific actions.
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
    transport: Arc<dyn DatagramTransport>,

    /// Current state of the Gossipod,
    state: RwLock<GossipodState>,

    /// Channel sender for initiating shutdown
    shutdown: broadcast::Sender<()>,

    /// Monotonically increasing sequence number for events
    sequence_number: AtomicU64,

    /// Incarnation number, used to detect and handle node restarts
    incarnation: AtomicU64,

    /// Manager for handling and creating timestamped events
    event_scheduler: Arc<EventScheduler>,

    // broadcasts represents outbound messages to peers
    // when it's not set, it defaults to DefaultBroadcastQueue
    pub(crate) broadcasts: Arc<dyn BroadcastQueue>,

    // gossipod runtime for managing all thread execution
    runtime: Arc<GossipodRuntime>,

    // Dispatch event handler is used for dispatching events based on memebership
    // state changes
    pub(crate) dispatch_event_handler: Option<Arc<dyn DispatchEventHandler<M>>>
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
    /// Creates a new instance of [`Gossipod`] with the default metadata.
    pub async fn new(config: GossipodConfig) -> Result<Self> {
        let queue = DefaultBroadcastQueue::new(config.initial_cluster_size);
        let transport = DefaultTransport::new(config.ip_addr(), config.port()).await?;
        Gossipod::with_custom(
            config, 
            DefaultMetadata::default(), 
            Arc::new(queue), 
            Arc::new(transport),
            None,
        ).await
    }

    /// Creates a new instance of [`Gossipod`] with the default metadata and custom event handler.
    pub async fn with_event_handler(
        config: GossipodConfig,  
        dispatch_event_handler: Arc<dyn DispatchEventHandler<DefaultMetadata>>,
    ) -> Result<Self> {
        let queue = DefaultBroadcastQueue::new(config.initial_cluster_size);
        let transport = DefaultTransport::new(config.ip_addr(), config.port()).await?;
        Gossipod::with_custom(
            config, 
            DefaultMetadata::default(), 
            Arc::new(queue), 
            Arc::new(transport),
            Some(dispatch_event_handler),
        ).await
    }
}

impl<M> Gossipod<M>
where
    M: NodeMetadata + Send + Sync,
{
    /// Creates a new instance of [`Gossipod`] with the provided metadata.
    pub async fn with_custom(
        config: GossipodConfig, 
        metadata: M, 
        broadcasts: Arc<dyn BroadcastQueue>,
        transport:  Arc<dyn DatagramTransport>,
        dispatch_event_handler: Option<Arc<dyn DispatchEventHandler<M>>>,
     ) -> Result<Self> {
    
        let (shutdown_tx, _) = broadcast::channel(1);
        let addr = transport.local_addr().map_err(|e|anyhow!(e))?;

        let event_scheduler = Arc::new(EventScheduler::new());

        let runtime: Arc<GossipodRuntime> = GossipodRuntime::builder()
            .with_thread_pool_size(8)
            .with_metrics_enabled(true)
            .build::<ActorId, GossipodCommand>();

        let swim = Self {
            inner: Arc::new(InnerGossipod {
                config,
                metadata,
                members: Membership::new(),
                state: RwLock::new(GossipodState::Idle),
                shutdown: shutdown_tx.clone(),
                transport,
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(0),
                event_scheduler: event_scheduler,
                broadcasts,
                runtime,
                dispatch_event_handler,
            })
        };

        Ok(swim)
    }
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!(name = %self.inner.config.name, "[GOSSIPOD] Server Started");
        let shutdown_rx = self.inner.shutdown.subscribe();

        self.set_state(GossipodState::Running).await?;
        self.set_local_node_liveness().await?;

        self.launch_datagram_listener().await?;
        self.launch_tcp_listener().await?;
        
        self.launch_prober().await?;
        self.launch_gossiper().await?;

        self.launch_event_scheduler().await?;

        self.wait_till_completion(shutdown_rx).await?;
        Ok(())
    }

    async fn graceful_shutdown(&self) -> Result<()>{
        let _ = self.inner.runtime.destroy().await;
        self.set_state(GossipodState::Stopped).await?;
        self.inner.transport.shutdown().await.map_err(|e|anyhow!(e))?;
        self.leave().await?;

        info!("> [GOSSIPOD] Gracefully shut down completed.");
        Ok(())
    }

    // handle shutdown signal
    async fn wait_till_completion(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<ShutdownReason> {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("> [RECV] Initiating graceful shutdown..");
                self.graceful_shutdown().await?;
                Ok(ShutdownReason::Termination)
            }
        }
    }

    /// Launches the Gossipod prober.
    ///
    /// The prober is responsible for failure detection and information dissemination:
    ///
    /// Failure Detection (Probing):
    /// 1. Pick a node at random using round-robin.
    /// 2. Send a PING message to the node.
    /// 3. If received, node sends back an ACK message.
    /// 4. If failed, pick random nodes and send an INDIRECT PING-REQ for INDIRECT ACK.
    /// 5. If successful, do nothing.
    /// 6. If failed, mark the node as suspicious and disseminate the state change.
    /// 7. If suspicious node is indeed dead, it's removed from the membership list.
    /// 8. If not, the node must refute the suspicion and its alive state is reinstated.
    ///
    /// Information Dissemination:
    /// - Use gossiping to disseminate node state changes to randomly selected nodes.
    /// - Piggyback information on PING and PING-REQ messages.
    async fn launch_prober(&self) -> Result<()> {
        let interval: Duration = self.inner.config.base_probing_interval;
        let gossipod = self.clone();

        // Spawn a recurrent actor for probing
        self.runtime().spawn_recurrent_actor(
            ActorId::ProbeActor,
            interval,
            move || {
                let gossipod_clone = gossipod.clone();
                Box::pin(
                    async move {
                        if !gossipod_clone.is_running().await {
                            debug!("Gossipod is no longer running, stopping prober");
                            return;
                        }
                        match gossipod_clone.probe().await {
                            Ok(_) => {
                                debug!("Probe cycle completed successfully");
                                if let Ok(nodes) = gossipod_clone.inner.members.get_all_nodes() {
                                    debug!("{}", pretty_debug("Membership:", &nodes));
                                }
                            },
                            Err(e) => error!("Probe error: {}", e),
                        }
                    }
                )
            }
        ).await
    }

    async fn launch_gossiper(&self) -> Result<()> {
        let interval: Duration = self.inner.config.base_probing_interval;
        let gossipod = self.clone();
        
        self.runtime().spawn_recurrent_actor(
            ActorId::GossipActor,
            interval,
            move || {
                let gossipod_clone = gossipod.clone();
                Box::pin(
                    async move {
                        if !gossipod_clone.is_running().await {
                            info!("Gossipod is no longer running, stopping gossip scheduler");
                            return;
                        }
        
                        match gossipod_clone.gossip().await {
                            Ok(_) => debug!("Gossip cycle completed successfully"),
                            Err(e) => error!("Gossip error: {}", e),
                        }
                    }
                )
            }
        ).await
    }

    pub(crate) async fn launch_event_scheduler(&self) -> Result<()> {
        let gossipod = self.clone();
        let scheduler_event_stream = EventStream::new(self.inner.event_scheduler.clone());
       
        self.runtime().spawn_actor_on_stream(
            ActorId::EventSchedulerActor,
            scheduler_event_stream,
            move |command| {
                let gossipod = gossipod.clone();
                Box::pin(async move {
                    match command {
                        GossipodCommand::EventScheduler(event) => {
                            match event.get_state() {
                                EventState::ReachedDeadline => {
                                    match &event.event_type {
                                        EventType::Ack { sequence_number } => {
                                            debug!("Ack with sequence number {} timed out", sequence_number);
                                            let _ = event.sender.try_send(event.get_state());
                                        },
                                        EventType::SuspectTimeout { node } => {
                                            debug!("moving node {} to `DEAD` state", node);
                                            if let Ok(Some(node)) = gossipod.inner.members.get_node(&node) {
                                                if node.is_suspect() {
                                                    if let Err(e) = gossipod.confirm_node_dead(&node).await {
                                                        warn!("unable to confirm dead for node: {} because of: {}", node.name, e);
                                                    } else {
                                                        debug!("Successfully moved node {} to DEAD state", node.name);
                                                    }
                                                }
                                            };
                                        },
                                    }
                                }
                                _ => {{/* Event was intercepted or cancelled, so no action needed here. */}}
                            }
                            // Ensure to remove event.
                            gossipod.inner.event_scheduler.remove_event(&event).await;
                        }
                    }
                })
            }
        ).await
    }

    fn runtime(&self) -> Arc<GossipodRuntime> {
        return self.inner.runtime.clone();
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

    // Send user application-specific messages to target
    pub async fn send(&self, target: SocketAddr, msg: &[u8]) -> Result<()> {
        let local_node = self.get_local_node().await?;
        let local_addr = local_node.socket_addr()?;

        let message_payload = AppMsgPayload {data: msg.to_vec()};
        let message = Message {
            msg_type: MessageType::AppMsg,
            payload: MessagePayload::AppMsg(message_payload),
            sender: local_addr,
        };

        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        let mut stream = TcpStream::connect(target).await?;
        stream.write_all( &buffer).await?;

        Ok(())
    }

    /// Gossipod PROBE - Probes randomly selected nodes
    ///
    /// Probe implements the probing mechanism of the gossip protocol:
    /// 1. Selects a random node to probe, excluding the local node and dead nodes.
    /// 2. Sends a Ping message to the selected node.
    /// 3. Waits for an ACK response or a timeout.
    /// 4. Handles the response: updates node state or marks the node as suspect if no response.
    ///
    /// It a combination of direct UDP messaging and an event-based
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

            self.send_probe_and_respond(message, target, sequence_number, node).await?;
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
    /// 
    async fn send_probe_and_respond(&self, message_bytes: BytesMut, target: SocketAddr, sequence_number: u64, node: Node<M>) -> Result<()> {
        let now = Instant::now();
        let cluster_size = self.inner.members.len().unwrap_or(self.inner.config.initial_cluster_size);
        let probe_deadline = now+self.inner.config.calculate_interval(
            self.inner.config.base_probing_interval,
            cluster_size,
        );

        let ack_deadline = now + self.inner.config.ack_timeout;

        self.inner.transport.send_to(target, &message_bytes).await.map_err(|e|anyhow!(e))?;

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
                debug!("Received ACK for probe to node {}", node.name);
            },
            Some(EventState::ReachedDeadline) => {
                self.handle_ack_timeout(sequence_number, node, probe_deadline).await?;
            },
            Some(other_state) => {
                warn!("Unexpected event state: {:?}", other_state);
            },
            None => {
                warn!("Event channel closed unexpectedly for node {}", node.name);
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
        warn!("Probe to node {} timed out without ACK, proceeding to PING-REQ", node.name);
        let local_addr = self.get_local_node().await?.socket_addr()?;
        self.send_indirect_pings(local_addr, sequence_number, node.socket_addr()?).await?;

        let now = Instant::now();
        let indirect_ack_deadline = now + self.inner.config.indirect_ack_timeout;
        let (mut rx, _)  = self.inner.event_scheduler.schedule_event(
            EventType::Ack{ sequence_number }, 
            indirect_ack_deadline,
        ).await?;

        // Wait for Resonse before suspecting Node of Failure if we do not get a response
        // before events reaches deadline. 
        tokio::select! {
            event_state = rx.recv() => {
                match event_state {
                    Some(EventState::Intercepted) => {
                        debug!("[ERR] Received ACK for probe to node {}", node.name);
                    },
                    Some(EventState::ReachedDeadline) => {
                        self.handle_suspect_node(node).await?;
                    },
                    Some(other_state) => {
                        warn!("Unexpected event state: {:?}", other_state);
                    },
                    None => {
                        warn!("ERR] Event channel closed unexpectedly for node {}", node.name);
                    }
                }
            },
            _ = tokio::time::sleep_until(probe_deadline) => {
                warn!("Probing deadline reached for node {}, proceeding to the next cycle.", node.name);
                self.handle_suspect_node(node).await?;
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
        debug!("[PING] Send indirect pings");
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
    
            if let Err(e) = self.inner.transport.send_to(node.socket_addr()?, &message_bytes).await {
                debug!("[ERR] Failed to send indirect ping to {} for target {}: {}", node.name, target, e);
            } else {
                debug!("[INFO] Sent indirect ping to {} for target {}", node.name, target);
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
                match self.inner.transport.send_to(node.socket_addr()?, &broadcast).await {
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
                self.runtime().execute_now(async move {
                    if let Err(e) = this.send_ack(sender, payload.sequence_number).await {
                        error!("Failed to send ACK: {}", e);
                    }
                }).await?;

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
                warn!("Received NON PING message from {}", sender);
                return Err(anyhow!("Unexpected message type"));
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
        if let Err(e) = self.inner.transport.send_to(target, &message_bytes).await {
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
                debug!("Send ping: {:?}", ping);

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

                // Send the PING-REQ message.
                if let Err(e) = self.inner.transport.send_to(original_target, &message_bytes).await {
                    error!("[ERR] Failed to send UDP PING-REQ: {}", e);
                    return Ok(());
                }

                tokio::select! {
                    event_state = rx.recv() => {
                        match event_state {
                            Some(EventState::Intercepted) => {
                                debug!(
                                    "Received ACK for Indriect PING to target {}", 
                                    original_target,
                                );
                                self.send_ack(sender, original_sequence_number).await?;
                            },
                            Some(EventState::ReachedDeadline) => {
                                warn!("Indirect PING to target {} reached deadline", original_target);
                            },
                            Some(other_state) => {
                                warn!("Unexpected event state: {:?}", other_state);
                            },
                            None => {
                                warn!("Event channel closed unexpectedly for target {}", original_target);
                            }
                        }
                    },
                    _ = tokio::time::sleep_until(probe_deadline) => {
                        warn!("Indirect PING deadline reached for target {}", original_target);
                    }
                }
            },
            _ => {
                warn!("Received NON PING REQ message in PING_REQ from {}", sender);
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
                let _ = self.inner.event_scheduler.intercept_event(&EventType::Ack{
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
    
    async fn handle_app_msg(&self, from: SocketAddr, message_payload: MessagePayload) -> Result<()> {
        match message_payload {
            MessagePayload::AppMsg(payload) => {
                if self.inner.dispatch_event_handler.is_none() {
                    warn!("[ERR] No event event handler, ignoring app message.");
                    return Ok(());
                }

                if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
                    dispatch_handler.notify_message(from, payload.data).await.map_err(|e| anyhow!("unable to notify message {}", e))?;
                }
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
            dispatch_handler.notify_dead(&node).await.map_err(|e| anyhow!("unable to dispatch dead node notification {}", e))?;
        }
        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)

    }

    async fn set_local_node_liveness(&self) -> Result<()> {
        let ip_addr = self.inner.config.ip_addr();
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
        debug!("Integrate new node");
        let _ = self.inner.event_scheduler.intercept_event(&EventType::SuspectTimeout{node: member.name.clone()}).await;

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
                        self.broadcast_join(new_node.clone()).await?;

                        if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
                            dispatch_handler.notify_join(&new_node).await.map_err(|e| anyhow!("unable to dispatch join notification {}",e))?;
                        }
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

    async fn process_node_departure(&self, incarnation: u64, member: &str) -> Result<()> {
        debug!("Leave incarnation: {}", incarnation);

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
                            if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
                                dispatch_handler.notify_leave(&leave_node).await.map_err(|e| anyhow!("unable to dispatch leave notification: {}",e))?;
                            }
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
        let local_node = self.get_local_node().await.map_err(|e| anyhow!("unable to get local node {}",e))?;

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
                            if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
                                dispatch_handler.notify_dead(&confim_node).await.map_err(|e| anyhow!("unable to dispatch dead node notification: {}",e))?;
                            }
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

        self.broadcast(target, local_addr, broadcast.clone()).await
            .context("Failed to send join broadcast to initial contact")?;

        self.inner.broadcasts.upsert(broadcast.get_key(), broadcast)?;
        Ok(())
    }

    async fn broadcast(&self,  target: SocketAddr, sender: SocketAddr, broadcast: Broadcast) -> Result<()> {
        let message = Message {
            msg_type: MessageType::Broadcast,
            payload: MessagePayload::Broadcast(broadcast.clone()),
            sender,
        };

        info!("Sending broadcast: {}", broadcast.type_str());
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        self.inner.transport.send_to(target,&buffer).await.map_err(|e|anyhow!(e))
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
                            if let Some(dispatch_handler) = &self.inner.dispatch_event_handler {
                                dispatch_handler.notify_join(&node).await.map_err(|e| anyhow!("unable to dispatch join notification: {}",e))?;
                            }
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

    pub(crate) async fn launch_datagram_listener(&self) -> Result<()> {
        info!("Starting Datagram Listener...");
        let mut shutdown_rx = self.inner.shutdown.subscribe();
        let datagram_transport = self.inner.transport.clone();
        let gossipod = self.clone();
    
        tokio::spawn(async move {
            let mut datagram_rx = datagram_transport.incoming();
            loop {
                tokio::select! {
                    result = datagram_rx.recv() => {
                        match result {
                            Ok(datagram) => {
                                if let Err(e) = Self::handle_datagram(&gossipod, datagram).await {
                                    error!("Error handling datagram: {}", e);
                                }
                            },
                            Err(e) => {
                                error!("Error receiving datagram: {}", e);
                                // TODO: Implement backoff + circuit breaker here if errors persist
                            }
                        }
                    },
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal, stopping datagram listener");
                        break;
                    }
                }
            }
            
            info!("Datagram listener stopped");
        });
    
        Ok(())
    }
    
    async fn handle_datagram(gossipod: &Gossipod<M>, datagram: Datagram) -> Result<()> {
        debug!("Received UDP datagram from {}", datagram.remote_addr);
        
        let message = Message::from_vec(&datagram.data)
            .context("Failed to decode message")?;
    
        match message.msg_type {
            MessageType::Ping => gossipod.handle_ping(message.sender, message.payload).await?,
            MessageType::PingReq => gossipod.handle_ping_req(message.sender, message.payload).await?,
            MessageType::Ack => gossipod.handle_ack(message.payload).await?,
            MessageType::Broadcast => gossipod.handle_broadcast(message).await?,
            _ => {
                warn!("Unexpected message type {} for UDP from {}", message.msg_type, datagram.remote_addr);
            }
        }
    
        Ok(())
    }

    async fn launch_tcp_listener(&self) -> Result<()> {
        if self.inner.config.disable_tcp {
            return Ok(());
        }
        let addr = self.inner.transport.local_addr().map_err(|e|anyhow!(e))?;
        let tcp_listener = TcpConnectionListener::bind(addr).await?;
        let mut shutdown_rx = self.inner.shutdown.subscribe();
        let gossipod = self.clone();
    
        tokio::task::spawn(async move {
            let mut listener = pin!(tcp_listener);
            loop {
                tokio::select! {
                    event = poll_fn(|cx| listener.as_mut().poll(cx)) => {
                        match event {
                            ListenerEvent::Incoming { stream, remote_addr } => {
                                debug!("Accepted TCP connection from {}", remote_addr);
                                tokio::spawn(Self::handle_tcp_connection(gossipod.clone(), stream, remote_addr));
                            }
                            ListenerEvent::ListenerClosed { local_addr } => {
                                warn!("TCP listener closed on {}", local_addr);
                                break;
                            }
                            ListenerEvent::Error(err) => {
                                error!("TCP listener error: {}", err);
                                // TODO: Implement backoff + circuit breaker here if errors persist
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal, stopping TCP listener");
                        break;
                    }
                }
            }
            info!("TCP listener stopped");
        });
        Ok(())
    }
    
    async fn handle_tcp_connection(gossipod: Gossipod<M>, mut stream: TcpStream, remote_addr: SocketAddr) {
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    info!("TCP connection closed by {}", remote_addr);
                    break;
                }
                Ok(n) => {
                    if let Err(e) = Self::process_tcp_stream(&gossipod, &buffer[..n], &mut stream, remote_addr).await {
                        error!("Error processing TCP message from {}: {}", remote_addr, e);
                    }
                }
                Err(e) => {
                    error!("Error reading from TCP stream {}: {}", remote_addr, e);
                    break;
                }
            }
        }
    }

    /// Processes a stream over TCP
    async fn process_tcp_stream(gossipod: &Gossipod<M>, data: &[u8], _stream: &mut TcpStream, remote_addr: SocketAddr) -> Result<()> {
        let message = Message::from_vec(data)
                    .context("Failed to decode message")?;

        match message.msg_type {
            MessageType::AppMsg => gossipod.handle_app_msg(message.sender, message.payload).await,
            _ => {
                warn!("[ERR] Unexpected message type {} for TCP from {}", message.msg_type, remote_addr);
                Ok(())
            }
        }
    }
    
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.inner.state.write();
        match *state {
            GossipodState::Running => {
                self.inner.shutdown.send(()).map_err(|e| anyhow::anyhow!("unable to shutdown: {}",e))?;
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
    use crate::mock_transport::{MockDatagramTransport, create_mock_node};
    use crate::config::GossipodConfigBuilder;
    use crate::message::{Broadcast, MessagePayload};
    use std::time::Duration;

    async fn create_test_gossipod(name: &str, port: u16) -> Result<(Gossipod<DefaultMetadata>, Arc<MockDatagramTransport>)> {
        let config = GossipodConfigBuilder::new()
            .with_name(name.to_string())
            .with_port(port)
            .with_tcp_enabled(true)
            .build()
            .await?;

        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let mock_transport = Arc::new(MockDatagramTransport::new(local_addr));
        let gossipod = Gossipod::with_custom(
            config,
            DefaultMetadata::default(),
            Arc::new(DefaultBroadcastQueue::new(1)),
            mock_transport.clone(),
            None,
        ).await?;

        Ok((gossipod, mock_transport))
    }

    #[tokio::test]
    async fn test_suspect_refutation() -> Result<()> {
        let (gossipod, mock_transport) = create_test_gossipod("local_node", 8000).await?;

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
        let sent_datagrams = mock_transport.get_sent_datagrams().await;
        assert!(!sent_datagrams.is_empty(), "No datagrams were sent");

        let (_, message_bytes) = &sent_datagrams[0];
        let message = Message::from_vec(message_bytes)?;

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
        
        let (gossipod, mock_transport) = create_test_gossipod("local_node", 8000).await?;

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
        let sent_datagrams = mock_transport.get_sent_datagrams().await;
        assert!(!sent_datagrams.is_empty(), "No datagrams were sent");

        let (addr, message_bytes) = &sent_datagrams[0];
        assert_eq!(*addr, ping_req_payload.target, "PING was not sent to the correct target");

        let message = Message::from_vec(message_bytes)?;
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

        // Check that an ACK was sent back to the intermediate node
        let sent_datagrams = mock_transport.get_sent_datagrams().await;
        assert!(sent_datagrams.len() >= 1, "ACK was not sent");

        let (addr, message_bytes) = &sent_datagrams[sent_datagrams.len() - 1];
        assert_eq!(*addr, ping_req_message.sender, "ACK was not sent to the correct intermediate node");

        let message = Message::from_vec(message_bytes)?;
        assert_eq!(message.msg_type, MessageType::Ack, "Message sent was not an ACK");

        Ok(())
    }
}
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use config::{GossipodConfig, GossipodConfigBuilder, DEFAULT_IP_ADDR, DEFAULT_TRANSPORT_TIMEOUT, MAX_CONSECUTIVE_FAILURES};
use event::EventManager;
use ip_addr::IpAddress;
use listener::EventListener;
use members::MembershipList;
use message::MessageBroker;
use node::Node;
use retry_state::RetryState;
use state::NodeState;
use log::*;
use tokio::sync::{broadcast, RwLock};
use tokio::time;
use transport::Transport;
use std::time::Duration;

use anyhow::{anyhow,  Result};
mod transport;
mod message;
mod ip_addr;
mod config;
mod state;
mod node;
mod members;
mod listener;
mod codec;
mod retry_state;
mod event;
/// # SWIM Protocol Implementation
///
/// This module implements a simple asynchronous SWIM (Scalable Weakly-consistent
/// Infection-style Membership) protocol. The implementation is divided
/// into several components to foster modularity, shareability, and clarity:
///

struct Gossipod {
    inner: Arc<InnerGossipod>,
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

pub(crate) struct InnerGossipod  {
    // the node configuration
    config: GossipodConfig,
    // current members and their current state. every node maintains a map of information
    // about each nodes.
    members: MembershipList,
    transport: Transport,
    state: RwLock<GossipodState>,
    shutdown: broadcast::Sender<()>,
    sequence_number: AtomicU64,
    incarnation: AtomicU64,
    event_manager: EventManager,
    pub(crate) message_broker: MessageBroker,
}

impl Clone for Gossipod {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Gossipod {
    pub async fn new(config: GossipodConfig) -> Result<Self> {
        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .init();

        let (shutdown_tx, _) = broadcast::channel(1);
        let (transport, transport_channel) = Transport::new(
            config.port(), 
            config.addr(),  
            Duration::from_millis(DEFAULT_TRANSPORT_TIMEOUT),
        );

        let message_broker = MessageBroker::new(Box::new(transport.clone()));

        let swim = Self {
            inner: Arc::new(InnerGossipod {
                config,
                message_broker,
                members: MembershipList::new(),
                state: RwLock::new(GossipodState::Idle),
                shutdown: shutdown_tx.clone(),
                transport,
                sequence_number: AtomicU64::new(0),
                incarnation: AtomicU64::new(0),
                event_manager: EventManager::new(),
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
    // | gossipod  +-------> message_broker +-------> transport layer   |                      |
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
    // |                              |  gossipod  |                           | message_broker|                      
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

        self.join_local_node().await?;

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
                info!("[GOSSIPOD] Initiating graceful shutdown..");
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

    async fn spawn_listeners(mut listener: EventListener) {
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
                        warn!("Shutdown signal received, stopping TCP listener");
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
                    listener_result = transport.udp_packet_listener() => {
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
                                        warn!("Shutdown signal received during TCP listener restart delay");
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        warn!("Shutdown signal received, stopping UDP listener");
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

        // 1. FAILURE DETECTION USING PROBING
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

        // 1.INFORMATION DISSEMINATION THROUGH PUSH PULL (SYNC) & GOSSIPING
        // 1. Pick x nodes at random using round-robin 
        // 2. Send a PUSH PULL message to the nodes which includes the membership information of the source node
        // 3. If Received, Node send back an PUSH PULL response containing full state sync of the target node
        // 9. End
        // Perform dissemination action

        // Basically we have:
        // PROBING: For detecting failure
        // GOSSIPING: For disseminating node state change in one node targeting x randomly selected nodes
        // PUSH PULL (refer to hashicorp): For performing full state sync between one node and x randomly selected nodes
        tokio::spawn(async move {
            // Create a ticker for the probing interval
            let mut probe_interval = time::interval(gossipod.inner.config.probing_interval);

            // Create a ticker for the gossip interval
            let mut gossip_interval = time::interval(gossipod.inner.config.gossip_interval);

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
                        info!("Scheduler shutting down");
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
        self.inner.incarnation.fetch_add(1, Ordering::SeqCst)
    }

    /// Gossipod PROBE - Probes randomly selected nodes
    pub(crate) async fn probe(&self) -> Result<()> {
        debug!("PROBING");
        // unimplemented!()
        Ok(())
    }

    /// Gossipod GOSSIP - Gossip with randomly selected nodes for state changes
    pub(crate) async fn gossip(&self) -> Result<()> {
        debug!("GOSSIP");
        Ok(())
    }

    /// Gossipod JOIN - Notifies nodes of a peer joining 
    pub async fn join(&self) -> Result<()> {
        debug!("JOINING");
        // unimplemented!()
        println!("{:?}", self.inner.members.next_node(Some(|n: &Node| n.name == self.inner.config.name)));
        Ok(())
    }

    /// Gossipod LEAVE - Notifies nodes of a peer leaving 
    pub async fn leave(&mut self) {
        debug!("LEAVING");
        unimplemented!()
    }

    async fn join_local_node(&self)-> Result<()>  {
        let ip_addr = self.inner.config.addr();
        let name = self.inner.config.name();
        let port = self.inner.config.port();

        let node = Node::new(ip_addr, port, name)
            .with_state(NodeState::Alive);

        self.inner.members.add_node(node)?;

        let node2 = Node::new(ip_addr, port, "node_2".to_string()).with_state(NodeState::Alive);;
        let node3 = Node::new(ip_addr, port, "node_3".to_string()).with_state(NodeState::Alive);;
        let node4 = Node::new(ip_addr, port, "node_4".to_string()).with_state(NodeState::Dead);;
        let node5 = Node::new(ip_addr, port, "node_5".to_string()).with_state(NodeState::Alive);;
        self.inner.members.add_node(node2)?;
        self.inner.members.add_node(node3)?;
        self.inner.members.add_node(node4)?;
        self.inner.members.add_node(node5)?;

        Ok(())
    }

    pub async fn get_local_node(&self) -> Result<Node> {
        let local_node = self.inner.members.get_node(self.inner.config.name())?;
        if let Some(node) = local_node {
            return Ok(node)
        }

        Err(anyhow!("local node is not set"))
    }

    pub async fn members(&self)-> Result<Vec<Node>>  {
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
            GossipodState::Idle => Err(anyhow::anyhow!("Swim is not running")),
            GossipodState::Stopped => Ok(()),  // Already stopped, no-op
        }
    }
    pub async fn is_running(&self) -> bool {
        matches!(*self.inner.state.read().await, GossipodState::Running)
    }
    pub async fn set_state(&self, swim_state: GossipodState) -> Result<()> {
        let mut state = self.inner.state.write().await;
        *state = swim_state;
        Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let config = GossipodConfigBuilder::new()
        .name("node_1")
        .port(8080)
        .addr(Ipv4Addr::new(127, 0, 0, 1))
        .ping_timeout(Duration::from_millis(2000))
        .build()
        .await?;

    let gossipod = Gossipod::new(config).await?;

    let gossipod_clone1 = gossipod.clone();
    tokio::spawn(async move {
        if let Err(e) = gossipod_clone1.start().await {
            error!("[ERR] Error starting Gossipod: {:?}", e);
        }
    });

    while !gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("Members: {:?}", gossipod.members().await?);
    info!("[PROCESS] Gossipod is running");

    let gossipod_clone2 = gossipod.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for event");
        info!("Signal received, stopping Gossipod...");
        gossipod_clone2.stop().await.expect("Failed to stop Gossipod");
    });

    for _ in 0..1000 {
        if !gossipod.is_running().await {
            break;
        }
        gossipod.join().await?;
        time::sleep(Duration::from_secs(1)).await;
    }

    while gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
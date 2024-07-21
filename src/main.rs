use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use config::{SwimConfig, SwimConfigBuilder, DEFAULT_IP_ADDR, DEFAULT_TRANSPORT_TIMEOUT};
use ip_addr::IpAddress;
use listener::SwimListener;
use members::Members;
use message::{Message, MessageType, NetMessage};
use node::Node;
use state::NodeState;
use log::*;
use tokio::sync::{broadcast, RwLock};
use tokio::time;
use transport::Transport;
use std::time::Duration;

use anyhow::{anyhow, Result};
mod transport;
mod message;
mod ip_addr;
mod config;
mod state;
mod node;
mod members;
mod listener;

/// # SWIM Protocol Implementation
///
/// This module implements a simple asynchronous SWIM (Scalable Weakly-consistent
/// Infection-style Membership) protocol. The implementation is divided
/// into several components to foster modularity, shareability, and clarity:
///

struct Swim {
    inner: Arc<SwimInner>,
}

enum SwimState {
    Idle,
    Running,
    Stopped,
}

struct SwimInner  {
    // the node configuration
    config: SwimConfig,

    // current members and their current state. every node maintains a map of information
    // about each nodes.
    members: Members,

    transport: Transport,
    
    state: RwLock<SwimState>,
    
    shutdown: broadcast::Sender<()>,

    net_message: NetMessage,
}

impl Clone for Swim {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Swim {
    pub async fn new(config: SwimConfig) -> Result<Self> {
        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .init();

        let (shutdown_tx, _) = broadcast::channel(1);
        let (transport, transport_channel) = Transport::new(
            config.port(), 
            config.addr(),  
            Duration::from_millis(DEFAULT_TRANSPORT_TIMEOUT),
        );

        let net_message = NetMessage::new(Box::new(transport.clone()));

        let swim = Self {
            inner: Arc::new(SwimInner {
                config,
                net_message,
                members: Members::new(),
                state: RwLock::new(SwimState::Idle),
                shutdown: shutdown_tx,
                transport,
            })
        };

        Self::spawn_listeners(
            SwimListener::new(swim.clone(), transport_channel),
        ).await;

        Ok(swim)
    }
    // Swim Communication Flow:
    // +-------+       +------------+       +---------------------+
    // | SWIM  +-------> NET_MESSAGE +-------> TRANSPORT LAYER    |
    // +-------+       +------------+       |      (TCP/UDP)      |
    //                                       +---------+-----------+
    //                                                 |
    //                                                 v
    //                                       +---------+-----------+
    //                                       |       LISTENER      |
    //                                       +---------+-----------+
    //                                                 |
    //                         +-----------------------+------------------------+
    //                         |                                                    |
    //                         v                                                    v                                
    //                     +---+---+                                           +---+-----------+                           
    //                     |  SWIM  |                                           | NET_MESSAGE  |                      
    //                     +--------+                                           +--------------+                       

    pub async fn start(&self) -> Result<()> {
        info!("[SWIM] Server Started with `{}`", self.inner.config.name);
        let mut shutdown_rx = self.inner.shutdown.subscribe();

        // Bind TCP & UDP to IP and Port configuration
        self.inner.transport.bind_tcp_listener().await?;
        self.inner.transport.bind_udp_socket().await?;

        // Start TCP & UDP listeners
        let tcp_handler = {
            let this = self.clone();
            tokio::spawn(async move { 
                this.inner.transport.tcp_stream_listener().await 
            })
        };
        
        let udp_handler = {
            let this = self.clone();
            tokio::spawn(async move { 
                this.inner.transport.udp_packet_listener().await 
            })
        };

        // Note: When the Local Host (127.0.0.1 or 0.0.0.0) is provisioned, it is automatically bound to the system's private IP.
        // If a Private IP address is bound, the local host (127.0.0.1) becomes irrelevant.
        let private_ip_addr = IpAddress::find_system_ip()?;
        for ip_addr in &self.inner.config.ip_addrs {
            if ip_addr.to_string() == DEFAULT_IP_ADDR || ip_addr.to_string() == "0.0.0.0" {
                info!(
                    " [SWIM] Binding to all network interfaces: {}:{} (Private IP: {}:{})",
                    ip_addr.to_string(),
                    self.inner.config.port,
                    private_ip_addr.to_string(),
                    self.inner.config.port
                );
            } else {
                info!(
                    "[SWIM] Binding to specific IP: {}:{}",
                    ip_addr.to_string(),
                    self.inner.config.port
                );
            }
        }

        // set swim state 
        self.set_state(SwimState::Running).await?;

        // join local node to membership list
        self.join_local_node().await?;
       
        // Here, Both the TCP and UDP listener tasks are spawned and run concurrently in the background.
        // The downside of this is that, the process is spawned and if an error occurs in one of the task, 
        // it shutdowns the whole protocol. 
        // TODO? add a retry strategy to re-initialize the TCP/UDP background listeners
        tokio::select! {
            tcp_result = tcp_handler => {
                tcp_result.map_err(|e| anyhow!("TCP task errored-out: {}", e))??;
            }
            udp_result = udp_handler => {
                udp_result.map_err(|e| anyhow!("UDP task errored-out: {}", e))??;
            }
            _ = shutdown_rx.recv() => {
                self.set_state(SwimState::Stopped).await?;
                info!("[SWIM] Gracefully shutting down...");
            }
        }
        Ok(())
    }

    async fn spawn_listeners(mut listener: SwimListener) {
        tokio::spawn(async move {
            listener.run_listeners().await 
        });
    }
    // Handles each TCP request stream...
    async fn handle_tcp_stream(&mut self, addr: SocketAddr, data: Vec<u8>) -> Result<()> {
        let message = match String::from_utf8(data) {
            Ok(msg) => msg,
            Err(e) => {
                error!("[ERR] Failed to decode message from {}: {:?}", addr, e);
                return Err(anyhow!(format!("failed to decode message from {}: {:?}", addr, e)));
            }
        };

        // Log the received message and the sender address
        info!("[RECV] Received message from {}: {}", addr, message);
        match Message::from_json(&message) {
            Ok(parsed_message) => {

                info!("Received message from {}: {:?}", addr, parsed_message);
                // Handle the UDP Message Types
                match parsed_message.msg_type {
                    MessageType::Ping => {
                        info!("[MSG] Handling `PING` message with ID: {}", parsed_message.id);
                        // self.inner.net_message.send_ping(sender, self);
                    }
                    MessageType::PingReq => {
                        info!("MSG] Handling `PING_REQ` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Ack => {
                        info!("MSG] Handling `ACK` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Leave => {
                        info!("MSG] Handling `LEAVE` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Join => {
                        info!("MSG] Handling `JOIN]` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Update => {
                        info!("MSG] Handling `UPDATE` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Fail => {
                        info!("MSG] Handling `FAIL` message with ID: {}", parsed_message.id);
                    }
                    MessageType::AppMsg => {
                        info!("MSG] Handling `APP_MSG` message with ID: {}", parsed_message.id);
                    }
                }
            }
            Err(e) => {
                error!("[ERR] Failed to parse message from {}: {:?}", addr, e);
                return Err(anyhow!(format!("Failed to parse message from {}: {:?}", addr, e)));
            }
        };
        Ok(())
    }
    pub async fn send_message(&mut self) -> Result<()> {
        let members = self.members().await?;
        for node in &members {
            self.inner.net_message.send_ping(self.socket_addr().unwrap(), node.socket_addr().unwrap()).await?;
        }
        Ok(())
    }
    // handle UDP message from packet
    async fn handle_udp_packet(&mut self, addr: SocketAddr, data: Vec<u8>) -> Result<()>{
        let message = match String::from_utf8(data) {
            Ok(msg) => msg,
            Err(e) => {
                error!("[ERR] Failed to decode message from {}: {:?}", addr, e);
                return Err(anyhow!(format!("failed to decode message from {}: {:?}", addr, e)));
            }
        };

        // Log the received message and the sender address
        info!("[RECV] Received message from {}: {}", addr, message);
        match Message::from_json(&message) {
            Ok(parsed_message) => {

                info!("Received message from {}: {:?}", addr, parsed_message);
                // Handle the UDP Message Types
                match parsed_message.msg_type {
                    MessageType::Ping => {
                        info!("[MSG] Handling `PING` message with ID: {}", parsed_message.id);
                    }
                    MessageType::PingReq => {
                        info!("MSG] Handling `PING_REQ` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Ack => {
                        info!("MSG] Handling `ACK` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Leave => {
                        info!("MSG] Handling `LEAVE` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Join => {
                        info!("MSG] Handling `JOIN]` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Update => {
                        info!("MSG] Handling `UPDATE` message with ID: {}", parsed_message.id);
                    }
                    MessageType::Fail => {
                        info!("MSG] Handling `FAIL` message with ID: {}", parsed_message.id);
                    }
                    MessageType::AppMsg => {
                        info!("MSG] Handling `APP_MSG` message with ID: {}", parsed_message.id);
                    }
                }
            }
            Err(e) => {
                error!("[ERR] Failed to parse message from {}: {:?}", addr, e);
                return Err(anyhow!(format!("Failed to parse message from {}: {:?}", addr, e)));
            }
        };
        Ok(())
    }

    pub async fn join(&self) -> Result<()> {
        panic!("unimplement join")
    }

    async fn join_local_node(&self)-> Result<()>  {
        let ip_addr = self.inner.config.addr();
        let name = self.inner.config.name();
        let port = self.inner.config.port();

        let node = Node::new(ip_addr, port)
            .with_name(name)
            .with_state(NodeState::Alive);

        self.inner.members.add_node(node)?;

        Ok(())
    }

    pub(crate) fn socket_addr(&self) -> Result<SocketAddr> {
        let full_address = format!("{}:{}", self.inner.config.addr(), self.inner.config.port());
        full_address.parse::<SocketAddr>().map_err(|e| anyhow!(e.to_string()))
    }

    pub async fn members(&self)-> Result<Vec<Node>>  {
        self.inner.members.get_all_nodes()
    }
    
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.inner.state.write().await;
        match *state {
            SwimState::Running => {
                self.inner.shutdown.send(()).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                *state = SwimState::Stopped;
                Ok(())
            }
            SwimState::Idle => Err(anyhow::anyhow!("Swim is not running")),
            SwimState::Stopped => Ok(()),  // Already stopped, no-op
        }
    }
    pub async fn is_running(&self) -> bool {
        matches!(*self.inner.state.read().await, SwimState::Running)
    }
    pub async fn set_state(&self, swim_state: SwimState) -> Result<()> {
        let mut state = self.inner.state.write().await;
        *state = swim_state;
        Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let config = SwimConfigBuilder::new()
        .name("node_1")
        .port(8080)
        .addr(Ipv4Addr::new(127, 0, 0, 1))
        .ping_timeout(Duration::from_millis(2000))
        .build()
        .await?;
    
    let mut swim = Swim::new(config).await?;

    // Spawn a task to run the Swim instance
    let swim_clone = swim.clone();
    tokio::spawn(async move {
        if let Err(e) = swim_clone.start().await {
            error!("[ERR] Error starting Swim: {:?}", e);
        }
    });

    // wait for Swim to start
    while !swim.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("Members: {:?}", swim.members().await?);

    info!("[PROCESS] Swim is running");

    for _ in 0..10 {
        swim.send_message().await?;
        time::sleep(Duration::from_secs(1)).await;
    }

    // Stop Swim
    swim.stop().await?;

    info!("[PROCESS] Swim has been stopped");

    Ok(())
}
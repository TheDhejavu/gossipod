use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use async_trait::async_trait;
use gossipod::{DispatchEventHandler, Node, NodeMetadata};
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, Gossipod};
use log::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self};
use tokio::time;
use clap::Parser;

const NODE_NAME: &str = "NODE_2";
const BIND_PORT: u16 = 7947;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = NODE_NAME)]
    name: String,

    #[arg(long, default_value_t = BIND_PORT)]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    #[arg(long)]
    join_addr: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    key: String,
    value: u64,
}

struct SwimNode {
    gossipod: Arc<Gossipod>,
    receiver: mpsc::Receiver<Vec<u8>>,
    config: gossipod::config::GossipodConfig,
}

struct EventHandler{
    sender: mpsc::Sender<Vec<u8>>,
}

impl EventHandler {
    fn new(sender: mpsc::Sender<Vec<u8>>) -> Self{
        Self { sender }
    }
}

#[async_trait]
impl<M: NodeMetadata> DispatchEventHandler<M> for EventHandler {
    async fn notify_dead(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} detected as dead", node.name);
        Ok(())
    }

    async fn notify_leave(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} is leaving the cluster", node.name);
        Ok(())
    }

    async fn notify_join(&self, node: &Node<M>) -> Result<(), Box<dyn Error + Send + Sync>>  {
        info!("Node {} has joined the cluster", node.name);
        Ok(())
    }

    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Received message from {}: {:?}", from, message);
        self.sender.send(message).await?;
        Ok(())
    }
}


impl SwimNode {
    async fn new(args: &Args) -> Result<Self> {
        let config = GossipodConfigBuilder::new()
            .name(&args.name)
            .port(args.port)
            .addr(args.ip.parse::<Ipv4Addr>().expect("Invalid IP address"))
            .probing_interval(Duration::from_secs(5))
            .ack_timeout(Duration::from_millis(3_000))
            .indirect_ack_timeout(Duration::from_secs(1))
            .suspicious_timeout(Duration::from_secs(5))
            .network_type(NetworkType::Local)
            .build()
            .await?;

        let (sender, receiver) = mpsc::channel(1000);
        let dispatch_event_handler = EventHandler::new(sender);
    
        let gossipod = Gossipod::with_event_handler(config.clone(), Arc::new(dispatch_event_handler) )
            .await
            .context("Failed to initialize Gossipod with custom metadata")?;
    
           
        Ok(SwimNode {
            gossipod: gossipod.into(),
            receiver,
            config,
        })
    }

    async fn start(&self) -> Result<()> {
        let gossipod_clone = self.gossipod.clone();
        tokio::spawn(async move {
            if let Err(e) = gossipod_clone.start().await {
                error!("[ERR] Error starting Gossipod: {:?}", e);
            }
        });

        while !self.gossipod.is_running().await {
            time::sleep(Duration::from_millis(100)).await;
        }

        let local_node = self.gossipod.get_local_node().await?;
        info!("Local node: {}:{}", local_node.ip_addr, local_node.port);

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_incoming_message(msg).await?;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Signal received, stopping Gossipod...");
                    self.gossipod.stop().await?;
                    return Ok(());
                }
            }
        }
    }

    async fn handle_incoming_message(&self, data: Vec<u8>) -> Result<()> {
        let msg: Message = bincode::deserialize(&data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize msg from bytes: {}", e))?;

        info!("Received: key={} value={}", msg.key, msg.value);

        if msg.key == "ping" {
            self.send_pong_to_all(msg.value).await?;
        }

        Ok(())
    }

    async fn send_pong_to_all(&self, value: u64) -> Result<()> {
        let msg = Message {
            key: "pong".to_string(),
            value: value + 1,
        };

        for node in self.gossipod.members().await? {
            if node.name == self.config.name() {
                continue; // skip self
            }
            info!("Sending to {}: key={} value={}", node.name, msg.key, msg.value);
            self.gossipod.send(node.socket_addr()?, &bincode::serialize(&msg)?).await?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::Builder::new()
    .filter_level(::log::LevelFilter::Info) 
    .filter_level(::log::LevelFilter::Debug)
    .init();

    let mut node = SwimNode::new(&args).await?;
    node.start().await?;

    if let Some(join_addr) = args.join_addr {
        match join_addr.parse::<SocketAddr>() {
            Ok(addr) => {
                info!("Attempting to join {}", addr);
                if let Err(e) = node.gossipod.join(addr).await {
                    error!("Failed to join {}: {:?}", addr, e);
                } else {
                    info!("Successfully joined {}", addr);
                }
            },
            Err(e) => error!("Invalid join address {}: {:?}", join_addr, e),
        }
    } else {
        info!("No join address specified. Running as a standalone node.");
    }

    node.run().await?;

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
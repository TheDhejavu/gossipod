use std::error::Error;
use std::net::SocketAddr;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};

use async_trait::async_trait;
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, DispatchEventHandler, Gossipod, Node, NodeMetadata};
use tracing::{info, error};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self};
use tokio::time;
use clap::Parser;
use tracing_subscriber::EnvFilter;

const NODE_NAME: &str = "NODE_1";
const BIND_PORT: u16 = 7948;
const TICK_INTERVAL: Duration = Duration::from_secs(3);

struct SwimNode {
    gossipod: Arc<Gossipod>,
    receiver: mpsc::Receiver<Vec<u8>>,
    config: gossipod::config::GossipodConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    key: String,
    value: u64,
}


struct EventHandler{
    sender: mpsc::Sender<Vec<u8>>,
}

impl EventHandler {
    fn new(sender: mpsc::Sender<Vec<u8>>) -> Self{
        Self { sender }
    }
}

type DispatchError = Box<dyn Error + Send + Sync>;

#[async_trait]
impl<M: NodeMetadata> DispatchEventHandler<M> for EventHandler {
    async fn notify_dead(&self, node: &Node<M>) -> Result<(), DispatchError> {
        info!("Node {} detected as dead", node.name);
        Ok(())
    }

    async fn notify_leave(&self, node: &Node<M>) -> Result<(), DispatchError>  {
        info!("Node {} is leaving the cluster", node.name);
        Ok(())
    }

    async fn notify_join(&self, node: &Node<M>) ->Result<(), DispatchError> {
        info!("Node {} has joined the cluster", node.name);
        Ok(())
    }

    async fn notify_message(&self, from: SocketAddr, message: Vec<u8>) -> Result<(),DispatchError>  {
        info!("Received message from {}: {:?}", from, message);
        self.sender.send(message).await?;
        Ok(())
    }
}

impl SwimNode {
    async fn new(args: &Args) -> Result<Self> {
        let config = GossipodConfigBuilder::new()
            .with_name(&args.name)
            .with_port(args.port)
            .with_addr(args.ip.parse::<Ipv4Addr>().expect("Invalid IP address"))
            .with_probing_interval(Duration::from_secs(5))
            .with_ack_timeout(Duration::from_millis(500))
            .with_indirect_ack_timeout(Duration::from_secs(1))
            .with_suspicious_timeout(Duration::from_secs(5))
            .with_network_type(NetworkType::Local)
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
        let mut ticker = time::interval(TICK_INTERVAL);
        let mut counter: u64 = 0;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.send_ping_to_all(&mut counter).await;
                }
                Some(data) = self.receiver.recv() => {
                    self.handle_incoming_message(data, &mut counter).await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Signal received, stopping Gossipod...");
                    self.gossipod.stop().await?;
                    return Ok(());
                }
            }
        }
    }

    async fn send_ping_to_all(&self, counter: &mut u64) {
        let msg = Message {
            key: "ping".to_string(),
            value: *counter,
        };

        for node in self.gossipod.members().await.unwrap_or_default() {
            if node.name == self.config.name() {
                continue; // skip self
            }
            let target = node.socket_addr().unwrap();
            info!("Sending to {}: key={} value={} target={}", node.name, msg.key, msg.value, target);
            if let Err(e) = self.gossipod.send(target, &bincode::serialize(&msg).unwrap()).await {
                error!("Failed to send message to {}: {}", node.name, e);
            }
        }
    }

    async fn handle_incoming_message(&self, data: Vec<u8>, counter: &mut u64) {
        let msg: Message = match bincode::deserialize(&data) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to deserialize message: {}", e);
                return;
            }
        };

        info!("Received: key={} value={}", msg.key, msg.value);
        *counter = msg.value + 1;
    }
}

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


fn setup_tracing() {
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_ansi(true)
        .with_level(true);

    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("debug"));

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_tracing();

    let mut node = SwimNode::new(&args).await?;
    node.start().await?;

    node.run().await?;

    info!("Node stopped. Goodbye!");
    Ok(())
}
use std::{net::{Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use anyhow::{Context as _, Result};
use gossipod::{config::{GossipodConfigBuilder, NetworkType}, DefaultBroadcastQueue, DefaultTransport, Gossipod, NodeMetadata};
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use serde::{Deserialize, Serialize};
use tokio::time;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "node")]
    name: String,

    #[arg(long, default_value_t = 9090)]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    #[arg(long)]
    join_addr: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct Metadata {
    region: String,
    datacenter: String,
}

impl NodeMetadata for Metadata {}
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

    let config = GossipodConfigBuilder::new()
        .with_name(&args.name)
        .with_port(args.port)
        .with_addr(args.ip.parse::<Ipv4Addr>().expect("Invalid IP address"))
        .with_probing_interval(Duration::from_secs(1))
        .with_ack_timeout(Duration::from_millis(500))
        .with_indirect_ack_timeout(Duration::from_secs(1))
        .with_suspicious_timeout(Duration::from_secs(5))
        .with_network_type(NetworkType::LAN)
        .build()
        .await?;

    info!("Initializing Gossipod with custom metadata");
    
    // Use Custom Metadata
    let metadata = Metadata { 
        region: "aws-west-1".to_string(),
        datacenter: "dc1".to_string(),
    };

    // Use Default broadcast Queue
    let broadcast_queue =  Arc::new(DefaultBroadcastQueue::new(1));
    let transport = Arc::new(DefaultTransport::new(config.ip_addr(), config.port()).await?);
    let gossipod = Gossipod::with_custom(config, metadata, broadcast_queue, transport, None)
        .await
        .context("Failed to initialize Gossipod with custom metadata")?;

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

    if let Some(join_addr) = args.join_addr {
        match join_addr.parse::<SocketAddr>() {
            Ok(addr) => {
                info!("Attempting to join {}", addr);
                if let Err(e) = gossipod.join(addr).await {
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

    while gossipod.is_running().await {
        time::sleep(Duration::from_secs(1)).await;
    }

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
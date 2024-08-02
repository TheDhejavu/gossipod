use std::{net::{Ipv4Addr, SocketAddr}, time::Duration};
use anyhow::{Context as _, Result};
use gossipod::{config::GossipodConfigBuilder, Gossipod, NodeMetadata};
use log::*;
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = GossipodConfigBuilder::new()
        .name(&args.name)
        .port(args.port)
        .addr(args.ip.parse::<Ipv4Addr>().expect("Invalid IP address"))
        .ping_timeout(Duration::from_millis(2000))
        .build()
        .await?;

    info!("Initializing Gossipod with custom metadata");
    let metadata = Metadata { 
        region: "aws-west-1".to_string(),
        datacenter: "dc1".to_string(),
    };

    let gossipod = Gossipod::with_metadata(config, metadata)
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

    // Initialize the channel and get a receiver
    let mut receiver = gossipod.with_receiver(100).await;

    // Listen to app specific messages sent from peers
    let gossipod_clone = gossipod.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    println!("Received app message: {:?}", msg);
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // Modify the channel every 60 seconds
                    // receiver = gossipod_clone.with_receiver(200).await;
                    // println!("App message channel modified with new buffer size");
                }
            }
        }
    });

    while gossipod.is_running().await {
        time::sleep(Duration::from_secs(1)).await;
    }

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
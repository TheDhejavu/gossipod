# Gossipod
A Simple Asynchronous Swim Protocol written in Rust: [SWIM Protocol Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)

## Proposed Key Features

- Rust implementation for memory safety and performance
- Asynchronous architecture using Tokio for efficient I/O operations
- Simple API for easy integration into existing projects
- Codec-based message serialization and deserialization for efficient network communication
- Configurable failure detection parameters
- Support for both TCP and UDP protocols 
- Basic encryption of data packets for secure communication (planned)
- Extensible design allowing for future custom behaviors (planned)

## TODO List

- [ ] Complete basic SWIM protocol implementation
- [ ] Add TCP and UDP support
- [ ] Implement basic encryption for data packets
- [ ] Optimize performance (compression, faster serialization)
- [ ] Write tests and documentation

### Usage Sample

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = GossipodConfigBuilder::new()
        .name("node_1")
        .port(8080)
        .addr(Ipv4Addr::new(127, 0, 0, 1))
        .ping_timeout(Duration::from_millis(2000))
        .build()
        .await?;
    
    let mut gossipod = Gossipod::new(config).await?;

    // Spawn a task to run the Gossipod instance
    let gossipod_clone1 = gossipod.clone();
    tokio::spawn(async move {
        if let Err(e) = gossipod_clone1.start().await {
            error!("[ERR] Error starting gossipod: {:?}", e);
        }
    });

    // wait for Gossipod to start
    while !gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("Members: {:?}", gossipod.members().await?);
    info!("[PROCESS] Gossipod is running");

    // Listen for Ctrl+C or SIGINT
    let gossipod_clone2 = gossipod.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for event");
        info!("Signal received, stopping Gossipod...");
        gossipod_clone2.stop().await.expect("Failed to stop Gossipod");
    });

    for _ in 0..10 {
        if !gossipod.is_running().await {
            break;
        }
        gossipod.send_message().await?;
        time::sleep(Duration::from_secs(1)).await;
    }

    // Await until Gossipod is stopped either by signal or loop completion
    while gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
```
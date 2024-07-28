# Gossipod
A Simple Asynchronous Swim Protocol written in Rust: [SWIM Protocol Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)

### TODO List

#### 1. Protocol
   - [ ] Implement full SWIM protocol
       - [ ] Add failure detection mechanism
       - [ ] Implement dissemination component
       - [ ] Add support for suspicion mechanism to reduce false positives
   - [ ] Implement extensions to basic SWIM
       - [ ] Add support for lifeguard protocol for improved accuracy
       - [ ] Implement adaptive probe intervals

#### 2. Network
   - [ ] Implement TCP support
   - [ ] Implement UDP support

#### 3. Security
   - [ ] Implement encryption of data packets

#### 4. Performance
   - [ ] Add compression for data packets
   - [ ] Use codec for faster serialization/deserialization
   - [ ] Benchmark performance improvements

#### 5. Testing
   - [ ] Create unit and integration tests
   - [ ] Write basic usage documentation


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

    // Spawn a task to run the Swim instance
    let swim_clone1 = gossipod.clone();
    tokio::spawn(async move {
        if let Err(e) = swim_clone1.start().await {
            error!("[ERR] Error starting Swim: {:?}", e);
        }
    });

    // wait for Gossipod to start
    while !gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("Members: {:?}", gossipod.members().await?);
    info!("[PROCESS] Swim is running");

    // Listen for Ctrl+C or SIGINT
    let gossipod_clone2 = gossipod.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for event");
        info!("Signal received, stopping Swim...");
        gossipod_clone2.stop().await.expect("Failed to stop Swim");
    });

    for _ in 0..10 {
        if !gossipod.is_running().await {
            break;
        }
        gossipod.send_message().await?;
        time::sleep(Duration::from_secs(1)).await;
    }

    // Await until Swim is stopped either by signal or loop completion
    while gossipod.is_running().await {
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("[PROCESS] Gossipod has been stopped");
    Ok(())
}
```
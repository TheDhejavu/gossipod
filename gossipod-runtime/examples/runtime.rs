use std::time::Duration;
use gossipod_runtime::{ActorCommand, Runtime, RuntimeExt};
use tokio::time::sleep_until;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum ExActorId {
    RandomActor(String),
    PeriodicActor(String),
}

#[derive(Debug)]
enum ExCommand {
    Probe(String),
    Gossip(String),
}

impl ActorCommand for ExCommand {}

type ExRuntime = Runtime<ExActorId, ExCommand>;

#[tokio::main]
async fn main() {
    let runtime = ExRuntime::builder()
        .with_thread_pool_size(8)
        .with_metrics_enabled(true)
        .build::<ExActorId, ExCommand>();

    // Spawn a regular actor
    runtime.spawn_actor(
        ExActorId::RandomActor("actor1".to_string()),
        vec![async { ExCommand::Probe("node1".to_string()) }],
        |command| {
            Box::pin(async move {
                match command {
                    ExCommand::Probe(node) => println!("Probing node: {}", node),
                    ExCommand::Gossip(msg) => println!("Gossiping: {}", msg),
                }
            })
        }
    ).await.unwrap();

    // Spawn a recurrent actor
    runtime.spawn_recurrent_actor(
        ExActorId::PeriodicActor("actor2".to_string()),
        Duration::from_secs(1),
        || {
            Box::pin(async move {
                println!("Performing periodic task");
            })
        }
    ).await.unwrap();

    runtime.execute_now(async move {
        println!("Executing a task in the thread pool");
    }).await.unwrap();

    // Send a command to the regular actor
    // runtime.try_send_command(&ExActorId::RandomActor("actor1".to_string()), ExCommand::Gossip("Hello".to_string())).await.unwrap();

    sleep_until(tokio::time::Instant::now() + Duration::from_secs(10)).await;
    runtime.destroy().await.unwrap();

    println!("== Gracefully destroyed runtime ===");
}
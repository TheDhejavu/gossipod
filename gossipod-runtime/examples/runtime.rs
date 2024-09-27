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

    runtime.spawn_actor(ExActorId::RandomActor("actor1".to_string()), |command| {
        Box::pin(async move {
            match command {
               Some(cmd) => {
                    match  cmd {
                        ExCommand::Probe(node) => println!("Probing node: {}", node),
                        ExCommand::Gossip(msg) => println!("Gossiping: {}", msg),
                        _ => println!("Unhandled command: {:?}", cmd),
                    };
               }
               _ => println!("command is none."),
            }
        })
    }).await.unwrap();

    // Spawn a recurrent SWIM protocol actor for periodic tasks
    runtime.spawn_recurrent_actor(
        ExActorId::PeriodicActor("actor2".to_string()),
        Duration::from_secs(1),
        |command| {
            Box::pin(async move {
                match command {
                    Some(cmd) => {
                        match  cmd {
                            ExCommand::Probe(node) => println!("Probing node: {}", node),
                            ExCommand::Gossip(msg) => println!("Gossiping: {}", msg),
                            _ => println!("Unhandled command: {:?}", cmd),
                        };
                    }
                    _ => println!("_"),
                 }
            })
        }
    ).await.unwrap();

    runtime.execute_now(|| async {
        println!("Executing a task in the thread pool");
    }).await.unwrap();

    sleep_until(tokio::time::Instant::now() + Duration::from_millis(10_000)).await;
    runtime.destroy().await.unwrap();

    println!("==  Greacefully destroyed runtime === ");
}

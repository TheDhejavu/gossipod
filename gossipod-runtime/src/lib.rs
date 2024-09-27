//! # [`Runtime`] Module
//!
//! The `Runtime` module provides a robust and flexible framework for managing
//! concurrent and asynchronous tasks within your application. It offers a
//! comprehensive set of features for task scheduling, event communication,
//! and lifecycle management.

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use metrics::METRICS;
use pin_project::pin_project;
use tracing::error;
use std::pin::pin;
use std::time::Instant;
use rayon::ThreadPool;
use std::hash::Hash;
use futures::Future;
use std::sync::Arc;
use futures::future::{poll_fn, BoxFuture};
use crossbeam::queue::SegQueue;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, Duration};
use std::fmt::Debug;
use futures::stream::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
mod metrics;

// [`ActorCommand`] ths command is specific to actors, different actors
// can have different commands that does specific things and sometimes we 
// want to have seperation of concerns.
pub trait ActorCommand: Send + 'static {}

// [`RuntimeConfig`] runtime configuration
pub struct RuntimeConfig {
    thread_pool_size: usize,
    enable_metrics: bool,
    shutdown_duration: Duration,
}

// [`Runtime`] main entry point. it handles storage of actors (recurrent and ordinary )
// manages the runtime thread pool queue for executing `now` tasks.
// TODO: - back pressure implementation.
//       - etc
pub struct Runtime<T, C>
where 
    T: Hash + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand
{
    // Represents actors in the runtime, 0. represents the sender channel for incoming
    // actor-specific command, while .1 represents shutdown channel for runtime actor.
    // TODO: introduce runtime-specific command for managing actors E.g PAUSE,IDLE, RESUME, SHUTDOWN, to 
    // manage the lifecycle of the actor thread, this will also be useful for shutting down IDLE thread after
    // a period of time using `last_active` 
    actors: DashMap<T, (mpsc::UnboundedSender<C>, broadcast::Sender<()>)>,

    // thread pool queue using lock-free SeqQueue, having an entry queue allows
    // a more fine-grained execution of tasks that's non-blocking, it accepts `Box::pin(future)`
    // because we want to allocate the future on a heap.
    // thread_pool_queue: Arc<SegQueue<BoxFuture<'static, ()>>>,

    // Rayon thread pool for executing tasks
    thread_pool: Arc<ThreadPool>,

    // runtime configuration
    config: RuntimeConfig,
}

// [`Actor`] actor goal in the runtime is to react and excute tasks on a predefined handler.
pub struct Actor<T, C>
where
    T: Hash + Debug + Eq  + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    id: T,
    
    command_handler: Box<dyn Fn(Option<C>) -> BoxFuture<'static, ()> + Send + Sync>,

    // shutdown signal for actor:
    shutdown: broadcast::Sender<()>,
}

#[pin_project]
pub struct RecurrentActor<T, C>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    // actor
    actor: Actor<T, C>,

    // Interval for recurrent actor.
    #[pin]
    interval: tokio::time::Interval,
}

impl<T, C> Stream for RecurrentActor<T, C>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if this.interval.poll_tick(cx).is_ready() {
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}

// [`RuntimeBuilder`] represents runtime configuration builder.
pub struct RuntimeBuilder {
    config: RuntimeConfig,
}

impl RuntimeBuilder {
    /// Creates a new [`RuntimeBuilder`] to configure and build a Runtime.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let runtime = Runtime::builder()
    ///     .with_thread_pool_size(8)
    ///     .with_metrics_enabled(true)
    ///     .build::<ActorId, Command>();
    /// ```
    fn new() -> Self {
        Self {
            config: RuntimeConfig {
                thread_pool_size: 4,
                enable_metrics: false,
                shutdown_duration: Duration::from_millis(10_000)
            },
        }
    }

    /// Enables or disables metrics collection.
    ///
    /// # Arguments
    /// * `enable_metrics` - Set to `true` to enable metrics, `false` to disable.
    pub fn with_metrics_enabled(mut self, enable_metrics: bool) -> Self {
        self.config.enable_metrics = enable_metrics;
        self
    }

    /// Sets the size of the thread pool.
    ///
    /// # Arguments
    /// * `thread_pool_size` - The number of threads in the pool.
    pub fn with_thread_pool_size(mut self, thread_pool_size: usize) -> Self {
        self.config.thread_pool_size = thread_pool_size;
        self
    }

    /// Sets the duration to wait during shutdown.
    ///
    /// # Arguments
    /// * `shutdown_duration` - The maximum time to wait for shutdown.
    pub fn with_shutdown_duration(mut self, shutdown_duration: Duration) -> Self {
        self.config.shutdown_duration = shutdown_duration;
        self
    }

    /// Builds the Runtime with the configured settings.
    ///
    /// # Returns
    /// An `Arc<Runtime<T, C>>` instance.
    ///
    /// # Type Parameters
    /// * `T` - The actor identifier type.
    /// * `C` - The actor command type.
    pub fn build<T, C>(self) -> Arc<Runtime<T, C>>
    where
        T: Hash + Eq + Clone + Send + Sync + 'static,
        C: ActorCommand,
    {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.config.thread_pool_size)
            .build()
            .expect("unable to build thread pool");

        let runtime = Arc::new(Runtime {
            actors: DashMap::new(),
            thread_pool: Arc::new(thread_pool),
            config: self.config,
        });

        runtime
    }
}

// [`RuntimeExt`] represents runtime trait
#[async_trait::async_trait]
pub trait RuntimeExt<T, C>: Send + Sync + 'static 
where
    T: Hash + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    /// Spawn a future to be executed in the runtime thread pool immediately.
    async fn execute_now<F, Fut>(&self, future: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    /// Send a command to an actor.
    async fn send_command(&self, actor_id: &T, command: C) -> Result<()>;

    /// Spawn an actor with its handler
    async fn spawn_actor<F>(&self, actor_id: T, command_handler: F) -> Result<()>
    where
        F: Fn(Option<C>) -> BoxFuture<'static, ()> + Send + Sync + 'static;

    /// Spawn a recurrent actor that repeatedly calls its handler based on internal state
    async fn spawn_recurrent_actor<F>(&self, actor_id: T, initial_interval: Duration, command_handler: F) -> Result<()>
    where
        F: Fn(Option<C>) -> BoxFuture<'static, ()> + Send + Sync + 'static;

    /// Destroy a spawned actor by its ID
    async fn destroy_actor(&self, actor_id: &T) -> Result<()>;

    /// Destroy the whole runtime and shut everything down
    async fn destroy(&self) -> Result<()>;
}


impl<T, C> Runtime<T, C>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    /// Manages the lifecycle and execution of a recurrent actor.
    ///
    /// It handles the main loop for a recurrent actor, which performs periodic tasks
    /// in addition to handling incoming commands.
    /// 
    /// # TODO:
    /// * Add a mechanism to dynamically adjust the recurring interval.
    ///
    async fn run_recurrent_actor(
        mut recurrent_actor: RecurrentActor<T, C>, 
        mut actor_receiver: mpsc::UnboundedReceiver<C>,
    ) {
        let mut shutdown_rx = recurrent_actor.actor.shutdown.subscribe();
        let mut pinned_actor = pin!(recurrent_actor);
        loop {
            tokio::select! {
                Some(command) = actor_receiver.recv() => {
                    let start = Instant::now();
                    let fut = (pinned_actor.actor.command_handler)(Some(command));
                    fut.await;
                    let duration = start.elapsed();
                    
                    METRICS.actor_command_throughput.increment(1);
                    METRICS.actor_command_latency.record(duration.as_secs_f64());
                }
                _ = poll_fn(|cx| pinned_actor.as_mut().poll_next(cx)) => {
                    println!("==== Execute Recurring task =====");
                    let fut = (pinned_actor.actor.command_handler)(None);
                    fut.await;
                },
                _ = shutdown_rx.recv() => {
                    METRICS.total_actors.decrement(1.0);
                    break;
                }
            }
        }
    }
    /// Manages the lifecycle and execution of a standard actor.
    ///
    /// It handles the main loop for a standard actor, processing incoming commands
    /// until a shutdown signal is received.
    async fn run_actor(actor: Actor<T, C>,  mut actor_receiver: mpsc::UnboundedReceiver<C>) {
        let mut shutdown_rx = actor.shutdown.subscribe();
        loop {
            tokio::select! {
                Some(command) = actor_receiver.recv() => {
                    let start = Instant::now();
                    let fut = (actor.command_handler)(Some(command));
                    fut.await;
                    let duration = start.elapsed();
                    
                    METRICS.actor_command_throughput.increment(1);
                    METRICS.actor_command_latency.record(duration.as_secs_f64());
                }
                _ = shutdown_rx.recv() => {
                    METRICS.total_actors.decrement(1.0);
                    break;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl<T, C> RuntimeExt<T, C> for Runtime<T, C>
where
    T: Hash + Eq + Debug + Clone + Send + Sync + 'static,
    C: ActorCommand,
{
    /// Spawn a future to be executed immediately in the runtime's thread pool.
    ///
    /// # Arguments
    ///
    /// * `future` - A future to be executed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.execute_now(|| async {
    ///     println!("Executing a task in the thread pool");
    /// }).await.unwrap();
    /// ```
    async fn execute_now<F, Fut>(&self, future: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::current();
        self.thread_pool.spawn(move || {
            runtime.block_on(future());
        });
        Ok(())
    }

    /// Sends a command to a specific actor.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The ID of the actor to receive the command.
    /// * `command` - The command to send.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.send_command(&ActorId("probing".to_string()), Command::Probe("node1".to_string())).await.unwrap();
    /// ```
    async fn send_command(&self, actor_id: &T, command: C) -> Result<()> {   
        let result = if let Some(actor) = self.actors.get(actor_id) {
            actor.0.send(command).map_err(|_| anyhow!("Failed to send command to actor."))
        } else {
            Err(anyhow!("Actor not found".to_string()))
        };

        if result.is_err() {
            METRICS.error_rate.increment(1);
            return result;
        }

        Ok(())
    }

    /// Spawns a new actor with the given ID and command handler.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The ID for the new actor.
    /// * `command_handler` - A function that handles commands for this actor.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.spawn_actor(ActorId("probing".to_string()), |command| {
    ///     Box::pin(async move {
    ///         // Handle command
    ///     })
    /// }).await.unwrap();
    /// ```
    async fn spawn_actor<F>(&self, actor_id: T, command_handler: F) -> Result<()>
     where
        F: Fn(Option<C>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        METRICS.total_actors.increment(1.0);
        // create sender and receiver channel for actor.
        let (sender, receiver)  = mpsc::unbounded_channel();
        let (shutdown_tx, _) = broadcast::channel(1);

        let actor = Actor {
            id: actor_id.clone(),
            shutdown: shutdown_tx.clone(),
            command_handler: Box::new(command_handler),
        };

        // Insert actor into actors.
        self.actors.insert(actor_id.clone(),(sender, shutdown_tx));
    
        // Spawn new actor
        tokio::spawn(async move {
            Self::run_actor(actor, receiver).await;
        });
        
        Ok(())
    }

    /// Spawns a new recurrent actor that executes periodically.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The ID for the new actor.
    /// * `initial_interval` - The interval at which the actor should execute.
    /// * `command_handler` - A function that handles commands for this actor.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.spawn_recurrent_actor(
    ///     ActorId("gossip".to_string()),
    ///     Duration::from_secs(1),
    ///     |command| {
    ///         Box::pin(async move {
    ///             // Handle command or perform periodic task
    ///         })
    ///     }
    /// ).await.unwrap();
    /// ```
    async fn spawn_recurrent_actor<F>(&self, actor_id: T, initial_interval: Duration, command_handler: F) -> Result<()>
     where
        F: Fn(Option<C>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        // Create sender and receiver channel for actor.
        let (sender, receiver)  = mpsc::unbounded_channel();
        let (shutdown_tx, _) = broadcast::channel(1);

        let actor = Actor {
            id: actor_id.clone(),
            shutdown: shutdown_tx.clone(),
            command_handler: Box::new(command_handler),
        };
        
        let recurrent_actor: RecurrentActor<T, C> = RecurrentActor {
            actor,
            interval: interval(initial_interval),
        };

        // Insert actor into actors.
        self.actors.insert(actor_id.clone(),(sender, shutdown_tx));

        // Spawn new actor
        tokio::spawn(async move {
            Self::run_recurrent_actor(recurrent_actor, receiver).await;
        });
        
        Ok(())
    }

    /// Destroys a spawned actor by its ID.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The ID of the actor to destroy.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.destroy_actor(ActorId("gossip".to_string()).await.unwrap();
    /// ```
    async fn destroy_actor(&self, actor_id: &T) -> Result<()> {
        if let Some((_, (_, shutdown_sender))) = self.actors.remove(actor_id) {
            shutdown_sender.send(()).map_err(|e| anyhow!("unable to send shutdown signal: {}", e))?;
        }
        Ok(())
    }

    /// Destroys the entire runtime, shutting down all actors.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runtime.destroy().await.unwrap();
    /// ```
    async fn destroy(&self) -> Result<()> {
        // 1. Collect all actor IDs first
        let actor_ids: Vec<T> = self.actors.iter().map(|entry| entry.key().clone()).collect();

        // 2. Send shutdown signals to all actors
        for actor_id in actor_ids {
            if let Err(e) = self.destroy_actor(&actor_id).await {
                error!("Error destroying actor {:?}: {}", actor_id, e);
            }
        }

        // 4. Wait for all actors to shut down
        let start_time = std::time::Instant::now();
        while !self.actors.is_empty() {
            if start_time.elapsed() > self.config.shutdown_duration {
                error!("Shutdown timeout reached. Some actors did not shut down gracefully.");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
 
}

#[cfg(test)]
mod tests {
    use parking_lot::RwLock;

    use super::*;
    use crate::RuntimeExt;

    #[derive(Clone, Hash, Eq, PartialEq, Debug)]
    struct MockActorId(String);

    #[derive(Debug)]
    struct MockCommand;

    impl crate::ActorCommand for MockCommand {}

    type MockRuntime = Runtime<MockActorId, MockCommand>;

    #[tokio::test]
    async fn test_execute_now() {
        let runtime = MockRuntime::builder()
        .with_thread_pool_size(1)
        .with_metrics_enabled(true)
        .build::<MockActorId, MockCommand>();
        
        let task_executed = Arc::new(RwLock::new(false));
        let task_executed_clone = task_executed.clone();

        runtime.execute_now(move || async move {
            let mut executed = task_executed_clone.write();
            *executed = true;
        }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(*task_executed.read());
    }
}
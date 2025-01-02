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
use tracing::{error, info};
use tokio::task::JoinSet;
use std::pin::pin;
use rayon::ThreadPool;
use std::hash::Hash;
use futures::Future;
use std::sync::Arc;
use futures::future::{poll_fn, BoxFuture};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, Duration};
use std::fmt::Debug;
use futures::stream::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
mod metrics;

// ===== REFACTOR ========= 
// 1. Recurrent Actor does one thing, it excutes its handler periodically without any command, remove the command from this type of actors
// 2. Poll Actors does one thing, they executes a join set of futures thats polls , response received will execute 
//    the join set actor handler. Their goal is to poll something till whenever 
// 3. Execute Now executes any given futures in the moment
// 4. Consumer Actors, they can be short-lived or run forever, the listen to commands and rexecutes their command handler,if excution time has 
// passed it shuts down, if its set to run forever it stays alive. good for producer consumer cases, this one makes uses of 
// raynnon thread pool

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
    T: Hash + Eq + Debug + Clone + Send + Sync + 'static,
    C: ActorCommand
{
    // Represents actors in the runtime, 0. represents the sender channel for incoming
    // actor-specific command, while .1 represents shutdown channel for runtime actor.
    // TODO: introduce runtime-specific command for managing actors E.g PAUSE,IDLE, RESUME, SHUTDOWN, to 
    // manage the lifecycle of the actor thread, this will also be useful for shutting down IDLE thread after
    // a period of time using `last_active` 
    actors: DashMap<T, (Option<mpsc::UnboundedSender<C>>, broadcast::Sender<()>)>,

    // keep tracks of all poll-based actors.
    // actors doesn't share raynon thread pool, they run in their own seperate tokio thread
    // they mostly react to `Poll::Ready` events, this mostly is applicable to channel receivers or 
    // any form of future that is poll-based, infact you can turn almost any event to a poll if you
    // wrap it around `Future::Stream` or implements a `Future` that is guaranteed to resolve.
    // actors: DashMap<T, Actor<T, C>>,

    // Rayon thread pool for executing tasks.
    thread_pool: Arc<ThreadPool>,

    // runtime configuration
    config: RuntimeConfig,
}

// [`Actor`] actor goal in the runtime is to react and excute tasks on a predefined handler.
pub struct Actor<T, C>
where
    T: Hash + Debug + Eq  + Clone + Send + Sync + 'static,
{
    id: T,

    // handler for actor.
    handler: Box<dyn Fn(C) -> BoxFuture<'static, ()> + Send + Sync>,
}

#[pin_project]
pub struct RecurrentActor<T>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
{
    id: T,

    // handler for actor.
    handler: Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,

    // Interval for recurrent actor.
    #[pin]
    interval: tokio::time::Interval,
}

pub struct ConsumerActor<T, C>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
    C: ActorCommand
{
    id: T,

    // handler for actor.
    handler: Box<dyn Fn(C) -> BoxFuture<'static, ()> + Send + Sync>,
}

impl<T> Stream for RecurrentActor<T>
where
    T: Hash + Debug + Eq + Clone + Send + Sync + 'static,
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
        T: Hash + Eq + Debug + Clone + Send + Sync + 'static,
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
    T: Hash + Eq + Debug + Clone + Send + Sync + 'static,
    C: ActorCommand
{
    /// Spawn a future to be executed in the runtime thread pool immediately.
    async fn execute_now<F>(&self, future: F) -> Result<()>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Try to Send a command to an actor (consumer actors) in order to perform some actions.
    async fn try_send_command(&self, actor_id: &T, command: C) -> Result<()>;

    /// Spawn an actor with its handler
    async fn spawn_actor<F, H>(&self, actor_id: T, futures: Vec<F>, handler: H) -> Result<()>
    where
        F: Future<Output = C> + Send + 'static,
        H: Fn(C) -> BoxFuture<'static, ()> + Send + Sync + 'static;

    /// Spawn a recurrent actor that repeatedly calls its handler based on internal state
    async fn spawn_recurrent_actor<F>(&self, actor_id: T, initial_interval: Duration, command_handler: F) -> Result<()>
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;

    /// Spawn an actor that processes items from a stream
    async fn spawn_actor_on_stream<S, H>(&self, actor_id: T, stream: S, handler: H) -> Result<()>
    where
        S: Stream<Item = C> + Send + 'static,
        H: Fn(C) -> BoxFuture<'static, ()> + Send + Sync + 'static;

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
        mut recurrent_actor: RecurrentActor<T>, 
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let mut pinned_actor = pin!(recurrent_actor);
        loop {
            tokio::select! {
                _ = poll_fn(|cx| pinned_actor.as_mut().poll_next(cx)) => {
                    println!("==== Execute Recurring task =====");
                    let fut = (pinned_actor.handler)();
                    fut.await;
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
        METRICS.total_actors.decrement(1.0);
        info!("Actor {:?} shut down", pinned_actor.id);
    }
    /// Manages the lifecycle and execution of a standard actor.
    ///
    /// It handles the main loop for a standard actor, processing incoming commands
    /// until a shutdown signal is received.
    async fn run_actor(
        actor: Actor<T, C>,
        futures: Vec<BoxFuture<'static, C>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let mut join_set = JoinSet::new();
    
        // 1. Spawn initial futures
        for future in futures {
            join_set.spawn(future);
        }
    
        loop {
            tokio::select! {
                Some(result) = join_set.join_next() => {
                    match result {
                        Ok(value) => {
                            // 2. Execute handler on result value.
                            let fut = (actor.handler)(value);
                            fut.await;
                        },
                        Err(e) => error!("Actor task failed: {:?}", e)
                    }
                    if join_set.is_empty() {
                        break;
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Actor {:?} received shutdown signal", actor.id);
                    break;
                }
            }
        }
    
        // 3. Clean up future join set
        join_set.abort_all();
        METRICS.total_actors.decrement(1.0);
        info!("Actor {:?} shut down", actor.id);
    }

    async fn run_stream_actor<S>(
        actor: Actor<T, C>,
        mut stream: S,
        shutdown_tx: broadcast::Sender<()>,
    ) where
        S: Stream<Item = C> + Send + 'static,
    {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut event_stream = pin!(stream);
        loop {
            tokio::select! {
                Some(item) = poll_fn(|cx| event_stream.as_mut().poll_next(cx)) => {
                    let fut = (actor.handler)(item);
                    fut.await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Actor {:?} received shutdown signal", actor.id);
                    break;
                }
                else => break,
            }
        }
    
        METRICS.total_actors.decrement(1.0);
        info!("Actor {:?} shut down", actor.id);
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
    async fn execute_now<F>(&self, future: F) -> Result<()>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = tokio::runtime::Handle::current();
        self.thread_pool.spawn(move || {
            rt.block_on(future);
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
    /// runtime.try_send_command(&ActorId("probing".to_string()), Command::Probe("node1".to_string())).await.unwrap();
    /// ```
    async fn try_send_command(&self, actor_id: &T, command: C) -> Result<()> {   
        let result = if let Some(actor) = self.actors.get(actor_id) {
            if let Some(sender) = &actor.0 {
                sender.send(command).map_err(|_| anyhow!("Failed to send command to actor."))?;
            }
            Err(anyhow!("Consumer Actor not found".to_string()))
        } else {
            Err(anyhow!("Consumer Actor not found".to_string()))
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
    async fn spawn_actor<F, H>(&self, actor_id: T, futures: Vec<F>, handler: H) -> Result<()>
    where
        F: Future<Output = C> + Send + 'static,
        H: Fn(C) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        METRICS.total_actors.increment(1.0);

        // let (sender, _) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let actor = Actor {
            id: actor_id.clone(),
            handler: Box::new(handler),
        };

        // Insert actor into actors.
        self.actors.insert(actor_id.clone(),(None, shutdown_tx));

        let boxed_futures: Vec<BoxFuture<'static, C>> = futures
            .into_iter()
            .map(|f| Box::pin(f) as BoxFuture<'static, C>)
            .collect();
        
        tokio::spawn(async move {
            Self::run_actor(actor, boxed_futures, shutdown_rx).await;
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
    async fn spawn_recurrent_actor<F>(&self, actor_id: T, initial_interval: Duration, handler: F) -> Result<()>
     where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        // Create sender and receiver channel for actor.
        // let (sender, receiver)  = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let recurrent_actor = RecurrentActor {
            id: actor_id.clone(),
            handler: Box::new(handler),
            interval: interval(initial_interval),
        };

        // Insert actor into actors.
        self.actors.insert(actor_id.clone(),(None, shutdown_tx));

        // Spawn new actor
        tokio::spawn(async move {
            Self::run_recurrent_actor(recurrent_actor, shutdown_rx).await;
        });
        
        Ok(())
    }

    async fn spawn_actor_on_stream<S, H>(&self, actor_id: T, stream: S, handler: H) -> Result<()>
    where
        S: Stream<Item = C> + Send + 'static,
        H: Fn(C) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        METRICS.total_actors.increment(1.0);

        let (shutdown_tx, _) = broadcast::channel(1);
        let actor = Actor {
            id: actor_id.clone(),
            handler: Box::new(handler),
        };

        // Insert actor into actors.
        self.actors.insert(actor_id.clone(), (None, shutdown_tx.clone()));

        tokio::spawn(async move {
            Self::run_stream_actor(actor, stream, shutdown_tx).await;
        });

        if let Err(e) = self.destroy_actor(&actor_id).await {
            error!("Error destroying actor {:?}: {}", actor_id, e);
        }

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

        runtime.execute_now(async move {
            let mut executed = task_executed_clone.write();
            *executed = true;
        }).await.unwrap();

        tokio::spawn(async move {});

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(*task_executed.read());
    }
}
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::borrow::Cow;
use log::{info, warn};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::time::sleep;
use anyhow::{anyhow, Result};

use crate::message::MessagePayload;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum EventType {
    Ping,
    PingReq,
    NoAck,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventState {
    Pending,
    Completed,
    TimedOut,
}

#[derive(Debug)]
pub(crate) enum EventUpdate<'a> {
    StateChange(Cow<'a, EventState>),
    Message(Cow<'a, EventState>, Cow<'a, MessagePayload>),
}

#[derive(Debug)]
pub(crate) struct Event {
    event_type: EventType,
    sequence_number: u64,
    state: EventState,
    sender: mpsc::Sender<EventUpdate<'static>>,
}

impl Event {
    /// Creates a new Event and returns it along with a receiver for updates.
    fn new(event_type: EventType, sequence_number: u64) -> (Self, mpsc::Receiver<EventUpdate<'static>>) {
        let (sender, receiver) = mpsc::channel(10);
        (
            Self {
                event_type,
                sequence_number,
                state: EventState::Pending,
                sender,
            },
            receiver,
        )
    }

    /// Updates the state of the event and sends an update through the channel.
    async fn update_state(&mut self, new_state: EventState, payload: Option<MessagePayload>) -> Result<()> {
        self.state = new_state.clone();
        let update = match payload {
            Some(payload) => EventUpdate::Message(Cow::Owned(new_state), Cow::Owned(payload)),
            None => EventUpdate::StateChange(Cow::Owned(new_state)),
        };
        self.sender.send(update).await.map_err(|e| anyhow!("failed to send event update: {}", e))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EventManager {
    events: Arc<Mutex<HashMap<(EventType, u64), Event>>>,
    cleanup_interval: Duration,
    shutdown_tx: broadcast::Sender<()>,
    is_shutting_down: Arc<AtomicBool>,
}

impl EventManager {
    /// Creates a new EventManager.
    pub(crate) fn new(cleanup_interval: Duration) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        let manager = Self {
            events: Arc::new(Mutex::new(HashMap::new())),
            cleanup_interval,
            shutdown_tx,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
        };
        manager.start_cleanup_task();
        manager
    }

    /// Creates a new event and returns a receiver for updates on that event.
    pub(crate) async fn create_event(
        &self,
        event_type: EventType,
        sequence_number: u64,
        timeout: Duration,
    ) -> Result<mpsc::Receiver<EventUpdate<'static>>> {
        if self.is_shutting_down().await {
            return Err(anyhow!("EventManager is shutting down, cannot create new events"));
        }

        let (event, receiver) = Event::new(event_type.clone(), sequence_number);
        let event_key = (event_type, sequence_number);

        let mut events = self.events.lock().await;
        events.insert(event_key.clone(), event);

        if let Err(e) = self.spawn_timeout_task(event_key, timeout) {
            warn!("unable to spawn timeout task: {}", e.to_string());
        }

        Ok(receiver)
    }

    /// Retrieves the current state of an event.
    pub(crate) async fn get_event_state(&self, event_seq_num: u64, event_type: EventType) -> Result<EventState> {
        let events = self.events.lock().await;
        events.get(&(event_type, event_seq_num))
            .map(|event| event.state.clone())
            .ok_or_else(|| anyhow!("event not found"))
    }

    /// Handles an incoming event, updating its state if it matches a pending event.
    pub async fn handle_event(&self, sequence_number: u64, event_type: &EventType, payload: &MessagePayload) -> Result<()> {
        let mut events = self.events.lock().await;
        if let Some(e) = events.get_mut(&(event_type.clone(), sequence_number)) {
            if e.state == EventState::Pending {
                return e.update_state(EventState::Completed, Some(payload.clone())).await;
            }
        }
        Err(anyhow!("unable to get event, probably a case of an expired or already completed event"))
    }

    /// Removes all non-pending events from the event map.
    pub(crate) async fn cleanup(&self) {
        let mut events = self.events.lock().await;
        events.retain(|_, event| event.state == EventState::Pending);
    }

    /// Spawns a task that will time out an event after a specified duration.
    fn spawn_timeout_task(&self, event_key: (EventType, u64), timeout: Duration) -> Result<()> {
        let events_clone = self.events.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = sleep(timeout) => {
                    let mut events = events_clone.lock().await;
                    if let Some(event) = events.get_mut(&event_key) {
                        if event.state == EventState::Pending {
                            if let Err(e) = event.update_state(EventState::TimedOut, None).await {
                                warn!("unable to update state to timeout due to: {}", e.to_string())
                            }
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Timeout task shutting down");
                }
            }
        });
        Ok(())
    }

    /// Starts the cleanup task.
    fn start_cleanup_task(&self) {
        let interval = self.cleanup_interval;
        let this = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(interval) => {
                        this.cleanup().await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Cleanup task shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Initiates shutdown of the EventManager.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Initiating EventManager shutdown");
        self.is_shutting_down.store(true, Ordering::SeqCst);

        // Timeout all events in order to release all processes waiting for its completion
        let mut events = self.events.lock().await;
        for (_, event) in events.iter_mut() {
            if event.state == EventState::Pending {
                if let Err(e) = event.update_state(EventState::TimedOut, None).await {
                    warn!("failed to update event state during shutdown: {}", e);
                }
            }
        }
        events.clear();

        info!("EventManager shutdown complete");
        Ok(())
    }

    /// Checks if the EventManager is currently shutting down.
    async fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use crate::message::{NoAckPayload, PingPayload};

    #[tokio::test]
    async fn test_create_and_handle_event() {
        let manager = EventManager::new(Duration::from_secs(60));
        let sequence_number = 1;
        let mut rx = manager.create_event(EventType::NoAck, sequence_number, Duration::from_secs(5)).await.unwrap();

        let payload = MessagePayload::NoAck(NoAckPayload { sequence_number: 0 });
        manager.handle_event(sequence_number, &EventType::NoAck, &payload).await.expect("handle event failed");

        if let Ok(Some(EventUpdate::Message(state, _))) = timeout(Duration::from_secs(1), rx.recv()).await {
            assert_eq!(*state, EventState::Completed);
        } else {
            panic!("expected completed event within 1 second but none was received");
        }
    }

    #[tokio::test]
    async fn test_event_timeout() {
        let manager = EventManager::new(Duration::from_secs(60));
        let sequence_number = 1;
        let mut rx = manager.create_event(EventType::Ping, sequence_number, Duration::from_millis(10)).await.unwrap();

        if let Ok(Some(EventUpdate::StateChange(state))) = timeout(Duration::from_millis(20), rx.recv()).await {
            assert_eq!(*state, EventState::TimedOut);
        } else {
            panic!("expected timeout event within 20 milliseconds but none was received");
        }
    }

    #[tokio::test]
    async fn test_cleanup() {
        let manager = EventManager::new(Duration::from_millis(100));
        let sequence_number = 1;
        manager.create_event(EventType::Ping, sequence_number, Duration::from_millis(50)).await.unwrap();

        // Wait for the cleanup task to run
        sleep(Duration::from_millis(200)).await;

        let state = manager.get_event_state(sequence_number, EventType::Ping).await;
        assert!(state.is_err(), "Event should have been cleaned up");
    }

    #[tokio::test]
    async fn test_shutdown() {
        let manager = EventManager::new(Duration::from_secs(60));
        let sequence_number = 1;
        let _ = manager.create_event(EventType::Ping, sequence_number, Duration::from_secs(5)).await.unwrap();

        manager.shutdown().await.expect("Shutdown failed");

        // Try to create a new event after shutdown
        let result = manager.create_event(EventType::Ping, 2, Duration::from_secs(5)).await;
        assert!(result.is_err(), "Should not be able to create event after shutdown");

        // Check that all events are cleared
        let events = manager.events.lock().await;
        assert!(events.is_empty(), "All events should be cleared after shutdown");
    }
}
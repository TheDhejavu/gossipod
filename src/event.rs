use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;
use anyhow::{Result, anyhow};

use crate::message::MessagePayload;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventType {
    Ping,
    PingReq,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventState {
    Pending,
    Completed,
    TimedOut,
}

// New enum to wrap EventState and MessagePayload
#[derive(Debug, Clone)]
pub(crate) enum EventUpdate {
    StateChange(EventState),
    Message(EventState, MessagePayload),
}

#[derive(Debug, Clone)]
pub(crate) struct Event {
    event_type: EventType,
    sequence_number: u64,
    state: EventState,
    sender: mpsc::Sender<EventUpdate>,
}

impl Event {
    pub(crate) fn new(
        event_type: EventType,
        sequence_number: u64,
    ) -> (Self, mpsc::Receiver<EventUpdate>) {
        let (sender, receiver) = mpsc::channel(10); // Adjust buffer size as needed
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

    async fn update_state(&mut self, new_state: EventState, payload: Option<MessagePayload>) -> Result<()> {
        self.state = new_state.clone();
        match payload {
            Some(payload) => self.sender.send(EventUpdate::Message(new_state, payload)).await?,
            None => self.sender.send(EventUpdate::StateChange(new_state)).await?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EventManager {
    events: Arc<Mutex<HashMap<u64, Event>>>,
}

impl EventManager {
    pub(crate) fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    // create new event
    pub(crate) async fn create_event(
        &self,
        event_type: EventType,
        sequence_number: u64,
        timeout: Duration,
    ) -> Result<mpsc::Receiver<EventUpdate>> {
        let (event, receiver) = Event::new(event_type, sequence_number);
        let event_seq_num = event.sequence_number;

        let mut events = self.events.lock().await;
        events.insert(event_seq_num, event);

        let events_clone = self.events.clone();
        tokio::spawn(async move {
            sleep(timeout).await;
            let mut events = events_clone.lock().await;
            if let Some(event) = events.get_mut(&event_seq_num) {
                if event.state == EventState::Pending {
                    let _ = event.update_state(EventState::TimedOut, None).await;
                }
            }
        });

        Ok(receiver)
    }

    // get event state
    pub(crate) async fn get_event_state(&self, event_seq_num: u64) -> Result<EventState> {
        let events = self.events.lock().await;
        if let Some(event) = events.get(&event_seq_num) {
            Ok(event.state.clone())
        } else {
            Err(anyhow!("Event not found"))
        }
    }

    // handle events base-off sequence number and event type
    pub async fn handle_event(&self, sequence_number: u64, event_type: EventType, payload: MessagePayload) -> Result<()> {
        let mut events = self.events.lock().await;
        if let Some(event) = events.values_mut().find(|e| 
            e.sequence_number == sequence_number && e.event_type == event_type && e.state == EventState::Pending
        ) {
            event.update_state(EventState::Completed, Some(payload)).await?;
            Ok(())
        } else {
            Err(anyhow!("No matching pending event found for the response"))
        }
    }

    // clean up completed events
    pub(crate) async fn cleanup(&self) {
        let mut events = self.events.lock().await;
        events.retain(|_, event| event.state == EventState::Pending);
    }
}
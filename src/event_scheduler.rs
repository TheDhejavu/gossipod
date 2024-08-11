use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use anyhow::{Result, anyhow};
use tokio::time::Instant;
use std::cmp::Ordering;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum EventType {
    Ack { sequence_number: u64 },
    SuspectTimeout { node: String },
}

/// Represents the current state of a scheduled event in the EventScheduler.
///
/// The lifecycle of an event typically progresses as follows:
/// 1. An event is initially created in the `Pending` state.
/// 2. If the event reaches its scheduled time, it transitions to the `ReachedDeadline` state.
/// 3. If the event is handled before its scheduled time, it moves to the `Intercepted` state.
/// 4. At any point before timing out or being intercepted, an event can be explicitly `Cancelled`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventState {
    /// The initial state of a newly scheduled event.
    Pending,

    /// Indicates that the event has reached its scheduled time and has been picked up by the system.
    /// This state suggests that the associated action or check should now be performed.
    ReachedDeadline,

    /// Represents an event that was handled before its scheduled time due to external factors.
    /// This could occur when a condition is met earlier than expected.
    Intercepted,

    /// Indicates that the event was explicitly cancelled before it could time out or be intercepted.
    /// This might happen if the event becomes irrelevant or unnecessary before its scheduled time.
    Cancelled,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EventId(u64);

impl Ord for EventId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for EventId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub(crate) struct Event {
    pub(crate) state: EventState,
    pub(crate) sender: mpsc::Sender<EventState>,
}

pub(crate) struct EventScheduler {
    events: Arc<RwLock<BinaryHeap<(Instant, EventId)>>>,
    event_map: Arc<RwLock<HashMap<EventType, (EventId, Event)>>>,
    event_counter: std::sync::atomic::AtomicU64,
}

impl EventScheduler {
    // Create new [`EventScheduler`] Instance
    pub(crate) fn new() -> Self {
        EventScheduler {
            events: Arc::new(RwLock::new(BinaryHeap::new())),
            event_map: Arc::new(RwLock::new(HashMap::new())),
            event_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Schedules a new event with a specified type and deadline.
    /// Returns a receiver to monitor the event state.
    pub(crate) async fn schedule_event(
        &self,
        event_type: EventType,
        deadline: Instant,
    ) -> Result<(mpsc::Receiver<EventState>, mpsc::Sender<EventState>)> {
        let mut event_map = self.event_map.write().await;
        if event_map.contains_key(&event_type) {
            return Err(anyhow!("An event of this type already exists"));
        }

        let (sender, receiver) = mpsc::channel(10);
        let id = EventId(self.event_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst));

        let event = Event {
            state: EventState::Pending,
            sender: sender.clone(),
        };

        event_map.insert(event_type, (id.clone(), event));
        self.events.write().await.push((deadline, id));

        Ok((receiver, sender))
    }

    /// Retrieves the next event that has reached its deadline.
    /// Returns the event type and event details if any.
    pub(crate) async fn next_event(&self) -> Option<(EventType, Event)> {
        let now = Instant::now();
        let mut events = self.events.write().await;
        let mut event_map = self.event_map.write().await;
        
        while let Some((time, id)) = events.pop() {
            if time <= now {
                // Find and remove the event from the event_map
                let event_entry = event_map.iter()
                    .find(|(_, (event_id, _))| event_id == &id)
                    .map(|(event_type, _)| event_type.clone());
    
                if let Some(event_type) = event_entry {
                    if let Some((_, event)) = event_map.remove(&event_type) {
                        return Some((event_type,  Event { 
                            state: EventState::ReachedDeadline, 
                            sender: event.sender
                        }));
                    }
                }
            } else {
                // If the event is in the future, put it back and stop searching
                events.push((time, id));
                break;
            }
        }
        None
    }

    /// Calculates the duration until the next event is due.
    /// Returns the duration if an event is found, or None if there are no events.
    pub(crate) async fn time_to_next_event(&self) -> Option<Duration> {
        let now = Instant::now();
        let events = self.events.read().await;
        
        events.peek().map(|(time, _)| {
            if *time > now {
                *time - now
            } else {
                Duration::from_secs(0)
            }
        })
    }

    /// Retrieves a limited number of events that are due up to the current time.
    /// Returns a vector of event types and event details that reached it's deadline
    pub(crate) async fn next_events(&self, limit: usize) -> Vec<(EventType, Event)> {
        let now = Instant::now();
        let mut due_events = Vec::with_capacity(limit);
        let mut processed = 0;

        loop {
            // Acquire read lock to check if there's work to do
            let events = self.events.read().await;
            if events.is_empty() || events.peek().map_or(true, |(time, _)| *time > now) || processed >= limit {
                break;
            }
            drop(events);

            // Process one event at a time
            if let Some((event_type, event)) = self.process_next_event().await {
                due_events.push((event_type, event));
                processed += 1;
            } else {
                break;
            }
        }

        due_events
    }

    async fn process_next_event(&self) -> Option<(EventType, Event)> {
        let now = Instant::now();
        
        // Acquire write locks for a short duration
        let mut events = self.events.write().await;
        if let Some((time, id)) = events.pop() {
            if time <= now {
                drop(events);
                let mut event_map = self.event_map.write().await;
                let event_entry = event_map.iter()
                    .find(|(_, (event_id, _))| event_id == &id)
                    .map(|(event_type, _)| event_type.clone());

                if let Some(event_type) = event_entry {
                    if let Some((_, event)) = event_map.remove(&event_type) {
                        return Some((event_type,  Event { 
                            state: EventState::ReachedDeadline, 
                            sender: event.sender
                        }));
                    }
                }
            } else {
                // If the event is in the future, put it back
                events.push((time, id));
            }
        }
        None
    }
    
    /// Intercepts a specified event, changing its state to Intercepted.
    /// Removes the event from both the event map and the event heap.
    pub(crate) async fn try_intercept_event(&self, event_type: &EventType) -> Result<()> {
        let mut event_map = self.event_map.write().await;
        
        if let Some((id, event)) = event_map.remove(event_type) {
            event.sender.send(EventState::Intercepted).await?;
            // Remove the event from the events heap as well
            let mut events = self.events.write().await;
            events.retain(|&(_, ref e_id)| e_id != &id);
            Ok(())
        } else {
            Err(anyhow!("Event not found"))
        }
    }

    /// Cancels a specified event by intercepting it.
    pub(crate) async fn cancel_event(&self, event_type: &EventType) -> Result<()> {
        self.try_intercept_event(event_type).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_schedule_and_next_event() {
        let manager = EventScheduler::new();
        let now = Instant::now();
        let event_type = EventType::SuspectTimeout { node: "node".to_string() };
        
        manager.schedule_event(event_type.clone(), now + Duration::from_millis(100)).await.unwrap();
        
        sleep(Duration::from_millis(110)).await;
        
        let event = manager.next_event().await;
        assert!(event.is_some());
        let (returned_type, _) = event.unwrap();
        assert_eq!(returned_type, event_type);
    }

    #[tokio::test]
    async fn test_intercept_event() {
        let manager = EventScheduler::new();
        let now = Instant::now();
        let event_type = EventType::SuspectTimeout { node: "node".to_string() };
        
        let (mut receiver, _) = manager.schedule_event(event_type.clone(), now + Duration::from_secs(1)).await.unwrap();
        
        manager.try_intercept_event(&event_type).await.unwrap();
        
        let result = timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(EventState::Intercepted));
    }

    #[tokio::test]
    async fn test_duplicate_event_type() {
        let manager = EventScheduler::new();
        let now = Instant::now();
        let event_type = EventType::SuspectTimeout { node: "node".to_string() };
        
        manager.schedule_event(event_type.clone(), now).await.unwrap();
        let result = manager.schedule_event(event_type.clone(), now + Duration::from_secs(1)).await;
        
        assert!(result.is_err());
    }
}

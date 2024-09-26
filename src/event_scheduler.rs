use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, RwLock};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use anyhow::{Result, anyhow};
use tokio::time::{sleep_until, Instant};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::cmp::Ordering as CmpOrdering;
use futures::stream::Stream;
use crossbeam::queue::SegQueue;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum EventType {
    Ack { sequence_number: u64 },
    SuspectTimeout { node: String },
}

// EVENT-LIFECYCLE
/// [`EventState`] Represents the current state of a scheduled event in the EventScheduler.
///
/// The lifecycle of an event typically progresses as follows:
/// 1. An event is initially created in the `Pending` state.
/// 2. If the event reaches its scheduled time, it transitions to the `ReachedDeadline` state.
/// 3. If the event is handled before its scheduled time, it moves to the `Intercepted` state.
/// 4. At any point before timing out or being intercepted, an event can be explicitly `Cancelled`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventState {
    /// The initial state of a newly scheduled event.
    Pending = 0,

    /// Indicates that the event has reached its scheduled time and has been picked up by the system.
    /// This basically means that we are unable to check that something is true for a given case within a timeframe.
    ReachedDeadline = 1,

    /// Represents an event that was handled before its deadline time.
    /// This could occur when a condition is met earlier before it reaches its deadline.
    Intercepted = 2,

    /// Indicates that the event was explicitly cancelled before it could time out or be intercepted.
    /// This might happen if the event becomes irrelevant or unnecessary before its scheduled time.
    Cancelled = 3,
}

// ========= Refactor

#[derive(Debug)]
pub(crate) struct Event {
    id: u64,
    pub(crate) event_type: EventType,
    pub(crate) state: AtomicU8,
    pub(crate) sender: mpsc::Sender<EventState>,
}

impl Event {
    // Create new event.
    fn new(event_type: EventType, sender: mpsc::Sender<EventState>, id: u64) -> Self {
        Event {
            sender,
            event_type,
            state: AtomicU8::new(EventState::Pending as u8),
            id,
        }
    }

    // Get event state.
    pub(crate) fn get_state(&self) -> EventState {
        match self.state.load(Ordering::Relaxed) {
            0 => EventState::Pending,
            1 => EventState::ReachedDeadline,
            2 => EventState::Intercepted,
            3 => EventState::Cancelled,
            _ => unreachable!(),
        }
    }

    // Set event state.
    fn set_state(&self, new_state: EventState) -> bool {
        self.state.compare_exchange(
            EventState::Pending as u8,
            new_state as u8,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ).is_ok()
    }
}

#[derive(Debug)]
struct TimestampedEvent {
    deadline: Instant,
    event: Arc<Event>,
}

impl PartialEq for TimestampedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.event.id == other.event.id
    }
}

impl Eq for TimestampedEvent {}

impl PartialOrd for TimestampedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedEvent {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.deadline.cmp(&self.deadline).then_with(|| other.event.id.cmp(&self.event.id))
    }
}

pub(crate) struct EventScheduler {
    event_map: Arc<RwLock<HashMap<EventType, Arc<Event>>>>,
    incoming_events: Arc<SegQueue<TimestampedEvent>>,
    next_event_id: AtomicU64,
    new_event_notify: Arc<Notify>,
}

impl EventScheduler {
    pub(crate) fn new() -> Self {
        EventScheduler {
            incoming_events: Arc::new(SegQueue::new()),
            event_map: Arc::new(RwLock::new(HashMap::new())),
            next_event_id: AtomicU64::new(0),
            new_event_notify: Arc::new(Notify::new()),
        }
    }
    /// Schedules a new event with a specified type and deadline.
    /// Returns a sender and a receiver to monitor the event state.
    pub(crate) async fn schedule_event(
        &self,
        event_type: EventType,
        deadline: Instant,
    )-> Result<(mpsc::Receiver<EventState>, mpsc::Sender<EventState>)> {
        if self.event_map.read().await.contains_key(&event_type) {
            return Err(anyhow!("An event of this type already exists"));
        }
        
        // Create a sender and receiver channel.
        let (sender, receiver) = mpsc::channel(1);
        let id = self.next_event_id.fetch_add(1, Ordering::SeqCst);

        // Construct new event.
        let event =  Arc::new(Event::new(event_type.clone(), sender.clone(), id));

        // Push events to lock-free incoming_events(`SegQueue`).
        self.incoming_events.push(TimestampedEvent { deadline, event: event.clone() });

        // Keep track of event in a map for interception / cancelling purposes
        self.event_map.write().await.insert(event_type, event.clone());

        // Notify event stream of new event.
        self.new_event_notify.notify_one();

        Ok((receiver, sender))
    }

    /// Intercepts a specified event, changing its state to Intercepted.
    pub(crate) async fn intercept_event(&self, event_type: &EventType) -> Result<bool> {
        if let Some(event) = self.event_map.read().await.get(event_type) {
            event.sender.send(EventState::Intercepted).await?;
            Ok(event.set_state(EventState::Intercepted))
        } else {
            Ok(false)
        }
    }
    /// Cancels a specified event before it reaches deadline, this can be use for a different case
    /// where `intercept_event` is not ideal.
    pub(crate) async fn cancel_event(&self, event_type: &EventType) -> Result<bool> {
        if let Some(event) = self.event_map.read().await.get(event_type) {
            event.sender.send(EventState::Cancelled).await?;
            Ok(event.set_state(EventState::Cancelled))
        } else {
            Ok(false)
        }
    }

    // Remove event from event mapping.
    pub(crate) async fn remove_event(&self, event: &Event) -> Option<Arc<Event>> {
        let mut event_map = self.event_map.write().await;
        event_map.remove(&event.event_type)
    }
}

#[pin_project]
pub(crate) struct EventStream {
    scheduler: Arc<EventScheduler>,
    lobby: BinaryHeap<TimestampedEvent>,
}

impl EventStream {
    // Create new event stream.
    pub(crate) fn new(scheduler: Arc<EventScheduler>) -> Self {
        EventStream {
            scheduler,
            lobby: BinaryHeap::new(),
        }
    }
    // Move event stream from events(`SegQueue`) from incoming to lobby for processing
    fn move_incoming_events_to_lobby(&mut self) -> bool {
        let mut moved_events = false;
        let now = Instant::now();
        while let Some(event) = self.scheduler.incoming_events.pop() {
            // events by default are FIFO, here we move any event that we see to the lobby for processing
            let current_deadline = event.deadline;
            self.lobby.push(event);
            moved_events = true;

            // if the current event that we picked has not reached deadline yet, we stop 
            // we only want to priotize moving event that have reached deadline to the lobby.
            if now < current_deadline {
                break;
            }
        }
        moved_events
    }
    // Peek into the lobby and picks the deadline for the next event.
    fn deadline_of_next_event(&self) -> Option<Instant> {
        self.lobby.peek().map(|event| event.deadline)
    }
}

impl Stream for EventStream {
    type Item = Arc<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let moved_events = self.move_incoming_events_to_lobby();
        // Process events from the lobby(`BinaryHeap`).
        while let Some(TimestampedEvent { deadline, event }) = self.lobby.pop() {
            // Double-check the deadline in case time has passed since moving events
            let now = Instant::now();
            if deadline <= now {
                // The event has reached its deadline
                if event.set_state(EventState::ReachedDeadline) {
                    return Poll::Ready(Some(event));
                }
                
                // If set state failed then we are sure that event has probably been intercepted / cancelled.
                return Poll::Ready(Some(event));
            } else {
                // If the event is still in the future, put it back into the lobby and stop processing
                self.lobby.push(TimestampedEvent { deadline, event });
                break;
            }
        }


        let waker = cx.waker().clone();
        let notify = self.scheduler.new_event_notify.clone();
        if let Some(next_deadline) = self.deadline_of_next_event() {
            tokio::spawn(async move {
                tokio::select! {
                    _ = sleep_until(next_deadline) => {},
                    _ = notify.notified() => {},
                }
                waker.wake();
            });
            return Poll::Pending;
        }

        // Schedule a task that will tell the runtime to check back on me 
        // when i receive a new event notification.
        tokio::spawn(async move {
            notify.notified().await;
            waker.wake();
        });

        return Poll::Pending;
    }
}


#[cfg(test)]
mod tests {
    use std::pin::pin;
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_intercept_event() {
        let scheduler = EventScheduler::new();
        let now = Instant::now();
        let event_type = EventType::SuspectTimeout { node: "node".to_string() };
        
        let (mut receiver, _) = scheduler.schedule_event(event_type.clone(), now + Duration::from_secs(1)).await.unwrap();
        
        assert!(scheduler.intercept_event(&event_type).await.expect("should intercept"));
        
        let result = timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(EventState::Intercepted));
    }
    #[tokio::test]
    async fn test_event_stream() {
        let scheduler = Arc::new(EventScheduler::new());
        let event_stream = EventStream::new(scheduler.clone());
        let mut stream = pin!(event_stream);

        // Schedule multiple events
        let now = Instant::now();
        let event_type1 = EventType::SuspectTimeout { node: "node1".to_string() };
        let event_type2 = EventType::Ack { sequence_number: 1 };
        let event_type3 = EventType::SuspectTimeout { node: "node2".to_string() };

        scheduler.schedule_event(event_type1.clone(), now + Duration::from_millis(100)).await.unwrap();
        scheduler.schedule_event(event_type2.clone(), now + Duration::from_millis(200)).await.unwrap();
        scheduler.schedule_event(event_type3.clone(), now + Duration::from_millis(300)).await.unwrap();

        // Use a timeout to prevent infinite waiting
        let timeout_duration = Duration::from_secs(1);

        // Aggregate events
        let mut received_events = Vec::new();
        while let Ok(Some(event)) = timeout(timeout_duration, futures::StreamExt::next(&mut stream)).await {
            received_events.push(event.event_type.clone());
            if received_events.len() == 3 {
                break;
            }
        }

        // Assert received events
        assert_eq!(received_events.len(), 3, "Expected to receive 3 events");
        assert_eq!(received_events[0], event_type1);
        assert_eq!(received_events[1], event_type2);
        assert_eq!(received_events[2], event_type3);

        // Check event states
        for event_type in &[event_type1, event_type2, event_type3] {
            if let Some(event) = scheduler.event_map.read().await.get(event_type) {
                assert_eq!(event.get_state(), EventState::ReachedDeadline);
            } else {
                panic!("Event {:?} not found in the map", event_type);
            }
        }
    }

    #[tokio::test]
    async fn test_duplicate_event_type() {
        let scheduler = EventScheduler::new();
        let now = Instant::now();
        let event_type = EventType::SuspectTimeout { node: "node".to_string() };
        
        scheduler.schedule_event(event_type.clone(), now).await.unwrap();
        let result = scheduler.schedule_event(event_type.clone(), now + Duration::from_secs(1)).await;
        
        assert!(result.is_err());
    }
}

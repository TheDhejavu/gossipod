use std::sync::Arc;

use gossipod_runtime::{ActorCommand, Runtime};

use crate::event_scheduler::Event;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) enum ActorId {
    ProbeActor,
    GossipActor,
    EventSchedulerActor,
}

#[derive(Debug)]
pub(crate) enum GossipodCommand {
    EventScheduler(Arc<Event>),
}

impl ActorCommand for GossipodCommand {}

pub(crate) type GossipodRuntime = Runtime<ActorId, GossipodCommand>;
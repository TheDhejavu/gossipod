use gossipod_runtime::{ActorCommand, Runtime};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) enum ActorId {
    ProbeActor,
    GossipActor,
    EventSchedulerActor,
}

#[derive(Debug)]
pub(crate) enum GossipodCommand {
    Probe(String),
    Gossip(String),
}

impl ActorCommand for GossipodCommand {}

pub(crate) type GossipodRuntime = Runtime<ActorId, GossipodCommand>;
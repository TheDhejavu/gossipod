// SWIM Protocol Message and Message Type.
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio_util::{bytes::BytesMut, codec::Decoder};
use core::fmt;
use std::net::SocketAddr;

use crate::{codec::MessageCodec, NodeState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingPayload {
    pub sequence_number: u64,
    pub piggybacked_updates: Vec<RemoteNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingReqPayload {
    pub target: SocketAddr,
    pub sequence_number: u64,
    pub piggybacked_updates: Vec<RemoteNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SuspectPayload {
    pub target: SocketAddr,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AckPayload {
    pub sequence_number: u64,
    pub piggybacked_updates: Vec<RemoteNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NoAckPayload {
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinPayload {
    pub node: String,
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LeavePayload {
    pub node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FailedPayload {
    pub node: String,
    pub target: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncReqPayload {
    pub sender: SocketAddr,
    pub members: Vec<RemoteNode>,
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq, Deserialize)]
pub struct RemoteNode {
    pub name: String,
    pub address: SocketAddr,
    pub metadata: Vec<u8>,
    pub state: NodeState,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AppMsgPayload {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MessagePayload {
    Ping(PingPayload),
    PingReq(PingReqPayload),
    Ack(AckPayload),
    NoAck(NoAckPayload),
    SyncReq(SyncReqPayload),
    AppMsg(AppMsgPayload),
    Broadcast(Broadcast),
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq, Deserialize)]
pub enum Broadcast {
    Suspect { incarnation: u64, member: String },
    Join { member: RemoteNode  },
    Leave { incarnation: u64, member: String },
    Confirm { incarnation: u64, member: String },
    Alive { incarnation: u64, member: String },
}

impl Broadcast {
    /// Returns a string representation of the broadcast type
    pub(crate) fn type_str(&self) -> &'static str {
        match self {
            Broadcast::Suspect { .. } => "SUSPECT",
            Broadcast::Join { .. } => "JOIN",
            Broadcast::Leave { .. } => "LEAVE",
            Broadcast::Confirm { .. } => "CONFIRM",
            Broadcast::Alive { .. } => "ALIVE",
        }
    }

    pub(crate) fn priority(&self) -> u8 {
        match self {
            Broadcast::Confirm { .. } => 4, 
            Broadcast::Leave { .. } => 3,  
            Broadcast::Suspect { .. } => 2,
            Broadcast::Alive { .. } => 1,   
            Broadcast::Join { .. } => 0,  
        }
    }

    pub fn get_key(&self) -> String {
        match self {
            Broadcast::Suspect { member, .. } => member.clone(),
            Broadcast::Join { member } => member.name.clone(),
            Broadcast::Leave { member, .. } => member.clone(),
            Broadcast::Confirm { member, .. } => member.clone(),
            Broadcast::Alive { member, .. } => member.clone(),
        }
    }
}


impl MessagePayload {
    /// Serializes different types of payloads.
    pub(crate) fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        match self {
            MessagePayload::Ping(p) => bincode::serialize(p),
            MessagePayload::PingReq(p) => bincode::serialize(p),
            MessagePayload::Ack(p) => bincode::serialize(p),
            MessagePayload::NoAck(p) => bincode::serialize(p),
            MessagePayload::SyncReq(p) => bincode::serialize(p),
            MessagePayload::Broadcast(p) => bincode::serialize(p),
            MessagePayload::AppMsg(p) => bincode::serialize(p),
        }
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    pub(crate) msg_type: MessageType,
    pub(crate) payload: MessagePayload,
    pub(crate) sender: SocketAddr,
}

impl Message {
    pub(crate) fn from_vec(data: &[u8]) -> Result<Self> {
        let mut codec = MessageCodec::new();
        let mut bytes = BytesMut::from(data);
        codec.decode(&mut bytes)?
            .ok_or_else(|| anyhow!("Unable to decode message"))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Ping = 0,
    PingReq = 1,
    Ack = 2,
    NoAck = 3,
    SyncReq = 4,
    AppMsg = 5,
    Broadcast = 6,
}

impl MessageType {
    pub(crate) fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(MessageType::Ping),
            1 => Ok(MessageType::PingReq),
            2 => Ok(MessageType::Ack),
            3 => Ok(MessageType::NoAck),
            4 => Ok(MessageType::SyncReq),
            5 => Ok(MessageType::AppMsg),
            6 => Ok(MessageType::Broadcast),
            _ => Err(anyhow!("Invalid MessageType value: {}", value)),
        }
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageType::Ping => write!(f, "PING"),
            MessageType::PingReq => write!(f, "PING_REQ"),
            MessageType::Ack => write!(f, "ACK"),
            MessageType::NoAck => write!(f, "NO_ACK"),
            MessageType::SyncReq => write!(f, "SYNC_REQ"),
            MessageType::AppMsg => write!(f, "APP_MSG"),
            MessageType::Broadcast => write!(f, "BROADCAST"),
        }
    }
}
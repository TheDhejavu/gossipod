// SWIM Protocol Message and Message Type.
use anyhow::{anyhow, Result};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::{bytes::BytesMut, codec::{Decoder, Encoder}};
use uuid::Uuid;
use core::fmt;
use std::{net::SocketAddr, time::{SystemTime, UNIX_EPOCH}};

use crate::{codec::MessageCodec, node::{Node, NodeMetadata}, transport::{NodeTransport, Transport}, NodeState};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemoteNode {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Broadcast {
    Suspect { incarnation: u64, member: String },
    Join { member: RemoteNode  },
    Leave { incarnation: u64, member: String },
    Confirm { incarnation: u64, member: String },
    Alive { incarnation: u64, member: String },
}

impl Broadcast {
    /// Returns a string representation of the broadcast type
    fn type_str(&self) -> &'static str {
        match self {
            Broadcast::Suspect { .. } => "SUSPECT",
            Broadcast::Join { .. } => "JOIN",
            Broadcast::Leave { .. } => "LEAVE",
            Broadcast::Confirm { .. } => "CONFIRM",
            Broadcast::Alive { .. } => "ALIVE",
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
    pub(crate) id: Uuid,
    pub(crate) msg_type: MessageType,
    pub(crate) payload: MessagePayload,
    pub(crate) sender: SocketAddr,
    pub(crate) timestamp: u64,
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

pub(crate) struct NetSvc {
    transport: Box<dyn NodeTransport>,
}

impl NetSvc {
    /// Creates a new MessageBroker instance
    pub fn new(transport: Box<dyn NodeTransport>) -> Self {
        Self { transport }
    }

    /// Synchronizes state with a target node
    pub async fn sync_state<T: NodeMetadata>(&self, target: SocketAddr, sender: SocketAddr, members: &[Node<T>]) -> Result<Vec<RemoteNode>> {
        let mut stream = self.transport.dial_tcp(target).await?;
        info!("Initiating sync state with: {}", target);
    
        self.send_sync_request(&mut stream, sender, members).await?;
        self.receive_sync_response(&mut stream).await
    }

    /// Receives a sync response from a stream
    async fn receive_sync_response(&self, stream: &mut TcpStream) -> Result<Vec<RemoteNode>> {
        let message = Transport::read_stream(stream).await?;

        match message.payload {
            MessagePayload::SyncReq(payload) => Ok(payload.members),
            _ => Err(anyhow!("unexpected message payload")),
        }
    }

    /// Sends a sync request to a stream
    pub async fn send_sync_request<M: NodeMetadata>(&self, stream: &mut TcpStream, sender: SocketAddr, members: &[Node<M>]) -> Result<()> {
        let sync_payload = SyncReqPayload { 
            sender,
            members: members.iter()
                .map(|member| {
                    Ok(RemoteNode { 
                        name: member.name.clone(), 
                        address: member.socket_addr()?,
                        state: member.state(),
                        metadata: member.metadata.to_bytes()?,
                        incarnation: member.incarnation(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };

        let message = Message {
            id: Uuid::new_v4(),
            msg_type: MessageType::SyncReq,
            payload: MessagePayload::SyncReq(sync_payload),
            sender,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        };
        
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        self.transport.write_to_tcp(stream, &buffer).await
    }

    pub async fn broadcast(&self,  target: SocketAddr, sender: SocketAddr, broadcast: Broadcast) -> Result<()> {
        let message = Message {
            id: Uuid::new_v4(),
            msg_type: MessageType::Broadcast,
            payload: MessagePayload::Broadcast(broadcast.clone()),
            sender,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };

        info!("Sending broadcast: {}", broadcast.type_str());
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        self.transport.write_to_udp(target,&buffer).await
    }
}
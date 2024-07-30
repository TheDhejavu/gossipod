// SWIM Protocol Message and Message Type.
use anyhow::{anyhow, Context as _, Result};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_util::{bytes::BytesMut, codec::{Decoder as _, Encoder}};
use uuid::Uuid;
use core::fmt;
use std::{net::SocketAddr, time::Duration};

use crate::{codec::MessageCodec, config::DEFAULT_MESSAGE_BUFFER_SIZE, transport::NodeTransport, NodeState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingPayload {
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingReqPayload {
    pub target: SocketAddr,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AckPayload {
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
pub(crate) struct DeadPayload {
    pub node: String,
    pub detected_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncReqPayload {
    pub node: String,
    pub members: Vec<NodeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeEntry {
    pub name: String,
    pub address: SocketAddr,
    pub region: Option<String>,
    pub state: NodeState,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AnnouncePayload {
    pub node: String,
    pub address: SocketAddr,
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
    Join(JoinPayload),
    Leave(LeavePayload),
    Dead(DeadPayload),
    SyncReq(SyncReqPayload),
    AppMsg(AppMsgPayload),
}

// NOTES:
// PING and PING-REQ wil be used for failure detections and information
// dissemination , which means messages have to be piggybacked while we wait or we can 
// create a future event that will wait for ack message based on sequence number within a time
// frame. PIN or PING-REQ will happen via constant probing

// JOIN, when a node joins, it must specify a node, we will send a SYNC-REQ message to the target node
// hoping to get a response via SYNC-RESP containing the memebership entry of the target node, thereby'
// allowing us to perform a full-sync between the new node and the target node
// if successful , the target node can create a broadcast notifying the nodes of a JOIN that cab 
// be queued and bradcasted later to randomly selected nodes.


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
    Join = 3,
    Leave = 4,
    Dead = 5,
    SyncReq = 6,
    AppMsg = 7,
}

impl MessageType {
    pub(crate) fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(MessageType::Ping),
            1 => Ok(MessageType::PingReq),
            2 => Ok(MessageType::Ack),
            3 => Ok(MessageType::Join),
            4 => Ok(MessageType::Leave),
            5 => Ok(MessageType::Dead),
            6 => Ok(MessageType::SyncReq),
            7 => Ok(MessageType::AppMsg),
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
            MessageType::Join => write!(f, "JOIN"),
            MessageType::Leave => write!(f, "LEAVE"),
            MessageType::Dead => write!(f, "DEAD"),
            MessageType::SyncReq => write!(f, "SYNC_REQ"),
            MessageType::AppMsg => write!(f, "APP_MSG"),
        }
    }
}

pub(crate) struct MessageBroker {
    transport: Box<dyn NodeTransport>,
}

impl MessageBroker {
    pub fn new(transport: Box<dyn NodeTransport>) -> Self {
        Self { 
            transport,
        }
    }
    
    /// Sends a ping message to the target address
    pub async fn send_ping(&self, target: SocketAddr, sender: SocketAddr, sequence_number: u64, ping_timeout: Duration,  ack_timeout: Duration) -> Result<()> {
        let ping_payload = PingPayload { 
            sequence_number,
        };

        let message = Message {
            id: Uuid::new_v4(),
            msg_type: MessageType::Ping,
            payload: MessagePayload::Ping(ping_payload),
            sender,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        // Send the PING message with timeout
        timeout(ping_timeout, self.transport.write_to_udp(target, &buffer))
            .await
            .context("UDP write timed-out")?
            .context("Failed to send PING message")?;
     
        self.await_ping_ack(target, ack_timeout).await
    }

    /// wait for PING-ACK Message 
    async fn await_ping_ack(&self, target: SocketAddr, timeout_duration: Duration) -> Result<()> {
        let mut buf = [0u8; DEFAULT_MESSAGE_BUFFER_SIZE];
        // TODO: Forward packet to handle_incoming if:
        // 1. They are not from target address 
        // 2. Not an ACK message
        match timeout(timeout_duration, self.transport.recv_packet(&mut buf)).await {
            Ok(Ok((_, addr))) if addr == target => {
                let response = Message::from_vec(&buf)
                    .context("Failed to decode response message")?;
                match response.msg_type {
                    MessageType::Ack => Ok(()),
                    _ => Err(anyhow::anyhow!("Unexpected response type: {:?}", response.msg_type)),
                }
            }
            Ok(Ok((_, addr))) => Err(anyhow::anyhow!("Response from unexpected address: {}", addr)),
            Ok(Err(e)) => Err(e).context("Error receiving PING response"),
            Err(_) => Err(anyhow::anyhow!("PING timed out")),
        }
    }
}
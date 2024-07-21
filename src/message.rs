// Swim Protocol Message and Message Type.
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json as serdeJson;
use uuid::Uuid;
use std::net::SocketAddr;

use crate::{transport::{NodeTransport, Transport}, NodeState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingPayload {
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PingReqPayload {
    // target node socket address for PING_REQ
    pub target: SocketAddr,

    // sequence number
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AckPayload {
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinPayload {
    // name of the node that's joining
    pub node: String,

    // socket address of the node
    pub address: SocketAddr,

    // metadata of the node
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LeavePayload {
    // name of the node
    pub node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FailPayload {
    // name of the node that failed
    pub failed_node: String,

    // name of the node that detected the failure
    pub detected_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UpdatePayload {
    pub updates: Vec<MembershipUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MembershipUpdate {
    // name of the node that require update
    pub node: String,

    // state of the node
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
    Fail(FailPayload),
    Update(UpdatePayload),
    AppMsg(AppMsgPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    pub(crate) id: String,
    pub(crate) msg_type: MessageType,
    pub(crate) payload: MessagePayload,
    pub(crate) sender: SocketAddr,
    pub(crate) timestamp: u64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MessageType {
    Ping,
    PingReq,
    Ack,
    Join,
    Leave,
    Fail,
    Update,
    AppMsg,
}

impl Message {
    pub(crate) fn from_json(json: &str) -> Result<Message>{
        serdeJson::from_str(json).map_err(|e| anyhow!(e.to_string()))
    }
}


pub(crate) struct NetMessage {
    transport: Box<dyn NodeTransport>
}

impl NetMessage {
    pub fn new(transport: Box<dyn NodeTransport>) -> Self {
        Self { transport }
    }
    /// Sends a ping message to the target address
    pub async fn send_ping(&self, sender: SocketAddr, target: SocketAddr) -> Result<()> {
        let ping_payload = PingPayload { sequence_number: 1 };
        let message = Message {
            id: Uuid::new_v4().to_string(),
            msg_type: MessageType::Ping,
            payload: MessagePayload::Ping(ping_payload),
            sender,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let serialized = serde_json::to_string(&message)?;
        let mut stream = self.transport.dial_tcp(target).await?;
        self.transport.write_to_tcp(&mut stream, serialized.as_bytes()).await?;
     
        Ok(())
    }
}
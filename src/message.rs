// Swim Protocol Message and Message Type.
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio_util::{bytes::{Buf as _, BufMut, BytesMut}, codec::{Decoder, Encoder}};
use uuid::Uuid;
use std::net::SocketAddr;

use crate::{codec::Codec, transport::NodeTransport, NodeState};

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
#[repr(u8)]
pub enum MessageType {
    Ping = 0,
    PingReq = 1,
    Ack = 2,
    Join = 3,
    Leave = 4,
    Fail = 5,
    Update = 6,
    AppMsg = 7,
}

impl MessageType {
    pub(crate)  fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(MessageType::Ping),
            1 => Ok(MessageType::PingReq),
            2 => Ok(MessageType::Ack),
            3 => Ok(MessageType::Join),
            4 => Ok(MessageType::Leave),
            5 => Ok(MessageType::Fail),
            6 => Ok(MessageType::Update),
            7 => Ok(MessageType::AppMsg),
            _ => Err(anyhow!("Invalid MessageType value: {}", value)),
        }
    }
}

pub(crate) struct MessageCodec;

impl MessageCodec {
    pub(crate) fn new() -> Self {
        MessageCodec
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    /// Encodes a `Message` into a `BytesMut` buffer for transmission.
    /// encoding format ensures that independent data has it's length set as prefix
    /// for accurate decoding and reconstruction of data.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Encode Message Type
        dst.put_u8(item.msg_type as u8);

        // Encode ID with length prefix
        let id_bytes = bincode::serialize(&item.id)?;
        dst.put_u32(id_bytes.len() as u32);
        dst.extend_from_slice(&id_bytes);

        // Encode Socket Address
        Codec::encode_socket_addr(&item.sender, dst);

        // Encode Timestamp with length prefix
        let timestamp_bytes = bincode::serialize(&item.timestamp)?;
        dst.put_u32(timestamp_bytes.len() as u32);
        dst.extend_from_slice(&timestamp_bytes);

        // Encode Payload with length prefix
        let payload_bytes = Codec::serialize_payload(&item.payload)?;
        dst.put_u32(payload_bytes.len() as u32); 
        dst.extend_from_slice(&payload_bytes);

        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Ensure we have at least one byte for the message type
        if src.is_empty() {
            return Ok(None);
        }
        
        let message_type = MessageType::from_u8(src.get_u8())?;
        let id: String = Codec::read_length_prefixed(src)?;
        let sender = Codec::decode_socket_addr(src)?;
        let timestamp: u64 = Codec::read_length_prefixed(src)?;

        let payload_len = Codec::read_bytes(src, 4)?.get_u32() as usize;
        let payload_bytes = Codec::read_bytes(src, payload_len)?;
       
        let payload: MessagePayload = match message_type {
            MessageType::Ping => MessagePayload::Ping(bincode::deserialize(&payload_bytes)?),
            MessageType::PingReq => MessagePayload::PingReq(bincode::deserialize(&payload_bytes)?),
            MessageType::Ack => MessagePayload::Ack(bincode::deserialize(&payload_bytes)?),
            MessageType::Join => MessagePayload::Join(bincode::deserialize(&payload_bytes)?),
            MessageType::Leave => MessagePayload::Leave(bincode::deserialize(&payload_bytes)?),
            MessageType::Fail => MessagePayload::Fail(bincode::deserialize(&payload_bytes)?),
            MessageType::Update => MessagePayload::Update(bincode::deserialize(&payload_bytes)?),
            MessageType::AppMsg => MessagePayload::AppMsg(bincode::deserialize(&payload_bytes)?),
        };

        Ok(Some(Message {
            id,
            msg_type: message_type,
            payload,
            sender,
            timestamp,
        }))
    }
}
pub(crate) struct MessageBroker {
    transport: Box<dyn NodeTransport>
}

impl MessageBroker {
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
        
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(message, &mut buffer)?;

        let mut stream = self.transport.dial_tcp(target).await?;
        self.transport.write_to_tcp(&mut stream, &buffer).await?;
     
        Ok(())
    }
}
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use tokio_util::{bytes::{Buf as _, BufMut as _, BytesMut}, codec::{Decoder, Encoder}};
use uuid::Uuid;

use crate::message::{Message, MessagePayload, MessageType};

pub(crate) struct MessageCodec;

impl MessageCodec {
    pub(crate) fn new() -> Self {
        MessageCodec
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    /// Encodes a `Message` into a `BytesMut` buffer for transmission.
    /// this encoding format ensures that independent data has it's length set as prefix
    /// for accurate decoding and reconstruction of data.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Encode Message Type
        dst.put_u8(item.msg_type as u8);

        // Encode ID with length prefix
        let id_bytes = bincode::serialize(&item.id)?;
        dst.put_u32(id_bytes.len() as u32);
        dst.extend_from_slice(&id_bytes);

        // Encode Socket Address
        Self::encode_socket_addr(&item.sender, dst);

        // Encode Timestamp with length prefix
        let timestamp_bytes = bincode::serialize(&item.timestamp)?;
        dst.put_u32(timestamp_bytes.len() as u32);
        dst.extend_from_slice(&timestamp_bytes);

        // Encode Payload with length prefix
        let payload_bytes = Self::serialize_payload(&item.payload)?;
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
        let id: Uuid = Self::read_length_prefixed(src)?;
        let sender = Self::decode_socket_addr(src)?;
        let timestamp: u64 = Self::read_length_prefixed(src)?;

        let payload_len = Self::read_bytes(src, 4)?.get_u32() as usize;
        let payload_bytes = Self::read_bytes(src, payload_len)?;
       
        let payload: MessagePayload = match message_type {
            MessageType::Ping => MessagePayload::Ping(bincode::deserialize(&payload_bytes)?),
            MessageType::PingReq => MessagePayload::PingReq(bincode::deserialize(&payload_bytes)?),
            MessageType::Ack => MessagePayload::Ack(bincode::deserialize(&payload_bytes)?),
            MessageType::Join => MessagePayload::Join(bincode::deserialize(&payload_bytes)?),
            MessageType::Leave => MessagePayload::Leave(bincode::deserialize(&payload_bytes)?),
            MessageType::Dead => MessagePayload::Dead(bincode::deserialize(&payload_bytes)?),
            MessageType::SyncReq => MessagePayload::SyncReq(bincode::deserialize(&payload_bytes)?),
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

impl MessageCodec {
    /// read a fixed number of bytes
    pub(crate) fn read_bytes(src: &mut BytesMut, size: usize) -> Result<BytesMut> {
        if src.remaining() < size {
            return Err(anyhow!("Buffer underflow: not enough data"));
        }
        Ok(src.split_to(size))
    }

    /// Read the length of the data, stored as a u32 (4 bytes).
    /// If successful, proceed to read the actual data based on this length.
    pub(crate) fn read_length_prefixed<T: DeserializeOwned>(src: &mut BytesMut) -> Result<T> {
        let len = MessageCodec::read_bytes(src, 4)?.get_u32() as usize;
        let data = MessageCodec::read_bytes(src, len)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// serialize different types of payloads
    pub(crate) fn serialize_payload(payload: &MessagePayload) -> Result<Vec<u8>, bincode::Error> {
        match payload {
            MessagePayload::Ping(p) => bincode::serialize(p),
            MessagePayload::PingReq(p) => bincode::serialize(p),
            MessagePayload::Ack(p) => bincode::serialize(p),
            MessagePayload::Join(p) => bincode::serialize(p),
            MessagePayload::Leave(p) => bincode::serialize(p),
            MessagePayload::Dead(p) => bincode::serialize(p),
            MessagePayload::SyncReq(p) => bincode::serialize(p),
            MessagePayload::AppMsg(p) => bincode::serialize(p),
        }
    }

    /// encode a SocketAddr into BytesMut
    pub(crate) fn encode_socket_addr(addr: &SocketAddr, dst: &mut BytesMut) {
        match addr {
            SocketAddr::V4(addr_v4) => {
                dst.put_u8(4);  // IPv4 identifier
                dst.extend_from_slice(&addr_v4.ip().octets());
                dst.put_u16(addr_v4.port());
            },
            SocketAddr::V6(addr_v6) => {
                dst.put_u8(6);  // IPv6 identifier
                dst.extend_from_slice(&addr_v6.ip().octets());
                dst.put_u16(addr_v6.port());
            },
        }
    }

    /// decode a SocketAddr from BytesMut
    pub(crate) fn decode_socket_addr(src: &mut BytesMut) -> Result<SocketAddr, anyhow::Error> {
        let ip_type = src.get_u8();
        let ip_addr = match ip_type {
            4 => {
                let bytes = src.split_to(4);
                IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]))
            },
            6 => {
                let bytes = src.split_to(16);
                IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(&bytes[..])?))
            },
            _ => return Err(anyhow!("Invalid IP type")),
        };
        let port = src.get_u16();
        Ok(SocketAddr::new(ip_addr, port))
    }
}


#[cfg(test)]
mod tests {
    use crate::message::PingPayload;

    use super::*;
    use serde::{Serialize, Deserialize};
    use tokio_util::bytes::BytesMut;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestPayload {
        data: String,
    }

    #[test]
    fn test_read_bytes() {
        let mut src = BytesMut::from(&b"Hello"[..]);
        assert_eq!(MessageCodec::read_bytes(&mut src, 5).unwrap(), b"Hello"[..]);
        // should error-out (buffer over flow)
        assert!(MessageCodec::read_bytes(&mut src, 1).is_err());
    }

    #[test]
    fn test_read_length_prefixed() {
        let payload = TestPayload { data: "World".to_string() };
        let mut buffer = BytesMut::new();
        let payload_bytes = bincode::serialize(&payload).unwrap();
        buffer.put_u32(payload_bytes.len() as u32);
        buffer.extend_from_slice(&payload_bytes);

        let decoded_payload: TestPayload = MessageCodec::read_length_prefixed(&mut buffer).unwrap();
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn test_encode_and_decode_socket_addr_ipv4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let mut dst = BytesMut::new();
        MessageCodec::encode_socket_addr(&addr, &mut dst);

        let mut src = dst.clone();
        let decoded_addr = MessageCodec::decode_socket_addr(&mut src).unwrap();
        assert_eq!(decoded_addr, addr);
    }

    #[test]
    fn test_encode_and_decode_socket_addr_ipv6() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 8080);
        let mut dst = BytesMut::new();
        // encode
        MessageCodec::encode_socket_addr(&addr, &mut dst);

        // decode
        let mut src = dst.clone();
        let decoded_addr = MessageCodec::decode_socket_addr(&mut src).unwrap();
       
        // assert equality 
        assert_eq!(decoded_addr, addr);
    }

    #[test]
    fn test_serialize_payload() {
        let payload = MessagePayload::Ping(PingPayload{ sequence_number: 1 });
        let serialized = MessageCodec::serialize_payload(&payload).unwrap();
        let expected_bytes = bincode::serialize(&PingPayload{ sequence_number: 1 }).unwrap();
        assert_eq!(serialized, expected_bytes);
    }
}

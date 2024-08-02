use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use tokio_util::{bytes::{Buf, BufMut, BytesMut}, codec::{Decoder, Encoder}};
use uuid::Uuid;

use crate::message::{Message, MessagePayload, MessageType};

pub(crate) struct MessageCodec;

impl MessageCodec {
    /// Creates a new MessageCodec instance.
    pub(crate) fn new() -> Self {
        MessageCodec
    }

    /// Reads a fixed number of bytes from the source BytesMut.
    pub(crate) fn read_bytes(src: &mut BytesMut, size: usize) -> Result<BytesMut> {
        if src.remaining() < size {
            return Err(anyhow!("buffer underflow: not enough data"));
        }
        Ok(src.split_to(size))
    }

    /// Reads length-prefixed data from the source BytesMut and deserializes it.
    pub(crate) fn read_length_prefixed<T: DeserializeOwned>(src: &mut BytesMut) -> Result<T> {
        let len = Self::read_bytes(src, 4)?.get_u32() as usize;
        let data = Self::read_bytes(src, len)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// Encodes a SocketAddr into BytesMut.
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

    /// Decodes a SocketAddr from BytesMut.
    pub(crate) fn decode_socket_addr(src: &mut BytesMut) -> Result<SocketAddr> {
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
            _ => return Err(anyhow!("invalid IP type")),
        };
        let port = src.get_u16();
        Ok(SocketAddr::new(ip_addr, port))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    /// Encodes a `Message` into a `BytesMut` buffer for transmission.
    ///
    /// This encoding format ensures that independent data has its length set as prefix
    /// for accurate decoding and reconstruction of data.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_u8(item.msg_type as u8);
        
        let id_bytes = bincode::serialize(&item.id)?;
        dst.put_u32(id_bytes.len() as u32);
        dst.extend_from_slice(&id_bytes);

        Self::encode_socket_addr(&item.sender, dst);

        let timestamp_bytes = bincode::serialize(&item.timestamp)?;
        dst.put_u32(timestamp_bytes.len() as u32);
        dst.extend_from_slice(&timestamp_bytes);

        let payload_bytes = item.payload.serialize()?;
        dst.put_u32(payload_bytes.len() as u32);
        dst.extend_from_slice(&payload_bytes);

        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = anyhow::Error;

    /// Decodes a `Message` from a `BytesMut` buffer.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
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
            MessageType::NoAck => MessagePayload::NoAck(bincode::deserialize(&payload_bytes)?),
            MessageType::SyncReq => MessagePayload::SyncReq(bincode::deserialize(&payload_bytes)?),
            MessageType::Broadcast => MessagePayload::Broadcast(bincode::deserialize(&payload_bytes)?),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{NoAckPayload, PingPayload};
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestPayload {
        data: String,
    }

    #[test]
    fn test_read_bytes() {
        let mut src = BytesMut::from(&b"Hello"[..]);
        assert_eq!(MessageCodec::read_bytes(&mut src, 5).unwrap(), b"Hello"[..]);
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
    fn test_encode_and_decode_socket_addr() {
        let addrs = vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 8080),
        ];

        for addr in addrs {
            let mut dst = BytesMut::new();
            MessageCodec::encode_socket_addr(&addr, &mut dst);
            let decoded_addr = MessageCodec::decode_socket_addr(&mut dst).unwrap();
            assert_eq!(decoded_addr, addr);
        }
    }

    #[test]
    fn test_serialize_payload() {
        let payload = MessagePayload::NoAck(NoAckPayload{ sequence_number: 1 });
        let serialized = payload.serialize().unwrap();
        let expected_bytes = bincode::serialize(&NoAckPayload{ sequence_number: 1 }).unwrap();
        assert_eq!(serialized, expected_bytes);
    }
}
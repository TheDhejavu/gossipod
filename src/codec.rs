use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use anyhow::{anyhow, bail, Context as _, Result};
use serde::de::DeserializeOwned;
use tokio_util::{bytes::{Buf, BufMut, BytesMut}, codec::{Decoder, Encoder}};

use crate::{message::{AckPayload, AppMsgPayload, Broadcast, Message, MessagePayload, MessageType, NoAckPayload, PingPayload, PingReqPayload, RemoteNode, SyncReqPayload}, state::NodeState};

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
    pub(crate) fn encode_socket_addr(addr: &SocketAddr, dst: &mut BytesMut) -> Result<()> {
        match addr {
            SocketAddr::V4(addr_v4) => {
                dst.put_u8(4);  // IPv4 identifier
                dst.extend_from_slice(&addr_v4.ip().octets());
                dst.put_u16(addr_v4.port());
                return Ok(())
            },
            SocketAddr::V6(addr_v6) => {
                dst.put_u8(6);  // IPv6 identifier
                dst.extend_from_slice(&addr_v6.ip().octets());
                dst.put_u16(addr_v6.port());
                return Ok(())
            },
            _ => Err(bail!("address does not match v4 or v6"))
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


    pub(crate) fn encode_u8(value: u8, dst: &mut BytesMut) -> Result<()> {
        dst.put_u8(value);
        Ok(())
    }

    pub(crate) fn decode_u8(src: &mut BytesMut) -> Result<u8> {
        if src.remaining() < 1 {
            return Err(anyhow!("Buffer underflow when decoding u8"));
        }
        Ok(src.get_u8())
    }

    pub(crate) fn encode_u64(value: u64, dst: &mut BytesMut) -> Result<()> {
        dst.put_u64(value);
        Ok(())
    }

    pub(crate) fn decode_u64(src: &mut BytesMut) -> Result<u64> {
        if src.remaining() < 8 {
            return Err(anyhow!("Buffer underflow when decoding u64"));
        }
        Ok(src.get_u64())
    }

    pub(crate) fn encode_bytes(bytes: &[u8], dst: &mut BytesMut) -> Result<()> {
        dst.put_u32(bytes.len() as u32);
        dst.extend_from_slice(bytes);
        Ok(())
    }

    fn decode_bytes(src: &mut BytesMut) -> Result<Vec<u8>> {
        let len = Self::read_bytes(src, 4)?.get_u32() as usize;
        let data = Self::read_bytes(src, len)?;
        Ok(data.to_vec())
    }

    pub(crate) fn encode_string(s: &str, dst: &mut BytesMut) -> Result<()> {
        Self::encode_bytes(s.as_bytes(), dst)
    }

    pub(crate) fn decode_string(src: &mut BytesMut) -> Result<String> {
        let bytes = Self::decode_bytes(src)?;
        String::from_utf8(bytes).map_err(|e| anyhow!("Invalid UTF-8 sequence: {}", e))
    }

    pub(crate) fn encode_remote_node(node: &RemoteNode, dst: &mut BytesMut) -> Result<()> {
        Self::encode_string(&node.name, dst)?;
        Self::encode_socket_addr(&node.address, dst)?;
        Self::encode_bytes(&node.metadata, dst)?;
        Self::encode_u8(node.state as u8, dst)?;
        Self::encode_u64(node.incarnation, dst)?;
        Ok(())
    }

    pub(crate) fn decode_remote_node(src: &mut BytesMut) -> Result<RemoteNode> {
        let name = Self::decode_string(src)?;
        let address = Self::decode_socket_addr(src)?;
        let metadata = Self::decode_bytes(src)?;
        let state = NodeState::from_u8(Self::decode_u8(src)?)?;
        let incarnation = Self::decode_u64(src)?;
        Ok(RemoteNode { name, address, metadata, state, incarnation })
    }

    pub(crate) fn encode_vec<T, F>(vec: &[T], encode_item: F, dst: &mut BytesMut) -> Result<()>
    where
        F: Fn(&T, &mut BytesMut) -> Result<()>,
    {
        dst.put_u32(vec.len() as u32);
        for item in vec {
            encode_item(item, dst)?;
        }
        Ok(())
    }

    pub(crate) fn decode_vec<T, F>(decode_item: F, src: &mut BytesMut) -> Result<Vec<T>>
    where
        F: Fn(&mut BytesMut) -> Result<T>,
    {
        let len = src.get_u32() as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(decode_item(src)?);
        }
        Ok(vec)
    }

    pub(crate) fn encode_ping_payload(payload: &PingPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_u64(payload.sequence_number, dst)?;
        if payload.piggybacked_updates.len() > 0 {
            Self::encode_vec(&payload.piggybacked_updates, Self::encode_remote_node, dst)?;
        }
        Ok(())
    }

    pub(crate) fn decode_ping_payload(src: &mut BytesMut) -> Result<PingPayload> {
        let sequence_number = Self::decode_u64(src)?;
        let piggybacked_updates = Self::decode_vec(Self::decode_remote_node, src)?;
        Ok(PingPayload { sequence_number, piggybacked_updates })
    }

    pub(crate) fn encode_ping_req_payload(payload: &PingReqPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_socket_addr(&payload.target, dst)?;
        Self::encode_u64(payload.sequence_number, dst)?;
        if payload.piggybacked_updates.len() > 0 {
            Self::encode_vec(&payload.piggybacked_updates, Self::encode_remote_node, dst)?;
        }
        Ok(())
    }

    pub(crate) fn decode_ping_req_payload(src: &mut BytesMut) -> Result<PingReqPayload> {
        let target = Self::decode_socket_addr(src)?;
        let sequence_number = Self::decode_u64(src)?;
        let piggybacked_updates = Self::decode_vec(Self::decode_remote_node, src)?;
        Ok(PingReqPayload { target, sequence_number, piggybacked_updates })
    }

    pub(crate) fn encode_ack_payload(payload: &AckPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_u64(payload.sequence_number, dst)?;
        if payload.piggybacked_updates.len() > 0 {
            Self::encode_vec(&payload.piggybacked_updates, Self::encode_remote_node, dst)?;
        }
        Ok(())
    }

    pub(crate) fn decode_ack_payload(src: &mut BytesMut) -> Result<AckPayload> {
        let sequence_number = Self::decode_u64(src)?;
        let piggybacked_updates = Self::decode_vec(Self::decode_remote_node, src)?;
        Ok(AckPayload { sequence_number, piggybacked_updates })
    }

    pub(crate) fn encode_no_ack_payload(payload: &NoAckPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_u64(payload.sequence_number, dst)
    }

    pub(crate) fn decode_no_ack_payload(src: &mut BytesMut) -> Result<NoAckPayload> {
        let sequence_number = Self::decode_u64(src)?;
        Ok(NoAckPayload { sequence_number })
    }

    pub(crate) fn encode_sync_req_payload(payload: &SyncReqPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_socket_addr(&payload.sender, dst)?;
        Self::encode_vec(&payload.members, Self::encode_remote_node, dst)?;
        Ok(())
    }

    pub(crate) fn decode_sync_req_payload(src: &mut BytesMut) -> Result<SyncReqPayload> {
        let sender = Self::decode_socket_addr(src)?;
        let members = Self::decode_vec(Self::decode_remote_node, src)?;
        Ok(SyncReqPayload { sender, members })
    }

    pub(crate) fn encode_app_msg_payload(payload: &AppMsgPayload, dst: &mut BytesMut) -> Result<()> {
        Self::encode_bytes(&payload.data, dst)
    }

    pub(crate) fn decode_app_msg_payload(src: &mut BytesMut) -> Result<AppMsgPayload> {
        let data = Self::decode_bytes(src)?;
        Ok(AppMsgPayload { data })
    }

    pub(crate) fn encode_broadcast(broadcast: &Broadcast, dst: &mut BytesMut) -> Result<()> {
        let mut temp_dst = BytesMut::new();
        match broadcast {
            Broadcast::Suspect { incarnation, member } => {
                temp_dst.put_u8(0);
                Self::encode_u64(*incarnation, &mut temp_dst)?;
                Self::encode_string(member, &mut temp_dst)?;
            },
            Broadcast::Join { member } => {
                temp_dst.put_u8(1);
                Self::encode_remote_node(member, &mut temp_dst)?;
            },
            Broadcast::Leave { incarnation, member } => {
                temp_dst.put_u8(2);
                Self::encode_u64(*incarnation, &mut temp_dst)?;
                Self::encode_string(member, &mut temp_dst)?;
            },
            Broadcast::Confirm { incarnation, member } => {
                temp_dst.put_u8(3);
                Self::encode_u64(*incarnation, &mut temp_dst)?;
                Self::encode_string(member, &mut temp_dst)?;
            },
            Broadcast::Alive { incarnation, member } => {
                temp_dst.put_u8(4);
                Self::encode_u64(*incarnation, &mut temp_dst)?;
                Self::encode_string(member, &mut temp_dst)?;
            },
        }
        dst.extend_from_slice(&temp_dst);
    
        Ok(())
    }


    pub(crate) fn decode_broadcast(src: &mut BytesMut) -> Result<Broadcast> {
        let broadcast_type = Self::decode_u8(src)?;
        match broadcast_type {
            0 => {
                let incarnation = Self::decode_u64(src)?;
                let member = Self::decode_string(src)?;
                Ok(Broadcast::Suspect { incarnation, member })
            },
            1 => {
                let member = Self::decode_remote_node(src)?;
                Ok(Broadcast::Join { member })
            },
            2 => {
                let incarnation = Self::decode_u64(src)?;
                let member = Self::decode_string(src)?;
                Ok(Broadcast::Leave { incarnation, member })
            },
            3 => {
                let incarnation = Self::decode_u64(src)?;
                let member = Self::decode_string(src)?;
                Ok(Broadcast::Confirm { incarnation, member })
            },
            4 => {
                let incarnation = Self::decode_u64(src)?;
                let member = Self::decode_string(src)?;
                Ok(Broadcast::Alive { incarnation, member })
            },
            _ => Err(anyhow!("Invalid broadcast type: {}", broadcast_type)),
        }
    }
    pub(crate) fn encode_message_payload(payload: &MessagePayload, dst: &mut BytesMut) -> Result<()> {
        match payload {
            MessagePayload::Ping(p) => Self::encode_ping_payload(p, dst),
            MessagePayload::PingReq(p) => Self::encode_ping_req_payload(p, dst),
            MessagePayload::Ack(p) => Self::encode_ack_payload(p, dst),
            MessagePayload::NoAck(p) => Self::encode_no_ack_payload(p, dst),
            MessagePayload::SyncReq(p) => Self::encode_sync_req_payload(p, dst),
            MessagePayload::AppMsg(p) => Self::encode_app_msg_payload(p, dst),
            MessagePayload::Broadcast(b) => Self::encode_broadcast(b, dst),
        }
    }

    fn decode_message_payload(msg_type: MessageType, src: &mut BytesMut) -> Result<MessagePayload> {
        let len = Self::read_bytes(src, 4)?.get_u32() as usize;
        let mut payload_data = Self::read_bytes(src, len)?;

        match msg_type {
            MessageType::Ping => Ok(MessagePayload::Ping(Self::decode_ping_payload(&mut payload_data)?)),
            MessageType::PingReq => Ok(MessagePayload::PingReq(Self::decode_ping_req_payload(&mut payload_data)?)),
            MessageType::Ack => Ok(MessagePayload::Ack(Self::decode_ack_payload(&mut payload_data)?)),
            MessageType::NoAck => Ok(MessagePayload::NoAck(Self::decode_no_ack_payload(&mut payload_data)?)),
            MessageType::SyncReq => Ok(MessagePayload::SyncReq(Self::decode_sync_req_payload(&mut payload_data)?)),
            MessageType::AppMsg => Ok(MessagePayload::AppMsg(Self::decode_app_msg_payload(src)?)),
            MessageType::Broadcast => Ok(MessagePayload::Broadcast(Self::decode_broadcast(&mut payload_data)?)),
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    /// Encodes a `Message` into a `BytesMut` buffer for transmission.
    ///
    /// This encoding format ensures that independent data has its length set as prefix
    /// for accurate decoding and reconstruction of data.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Self::encode_u8(item.msg_type as u8, dst)?;
        Self::encode_socket_addr(&item.sender, dst)?;

        let mut payload_bytes = BytesMut::new();
        Self::encode_message_payload(&item.payload, &mut payload_bytes)?;
        Self::encode_bytes(&payload_bytes, dst)?;

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
        
        let message_type = MessageType::from_u8(Self::decode_u8(src)?)?;
        let sender = Self::decode_socket_addr(src)?;
        let payload: MessagePayload =  Self::decode_message_payload(message_type, src)?;

        Ok(Some(Message {
            msg_type: message_type,
            payload,
            sender,
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
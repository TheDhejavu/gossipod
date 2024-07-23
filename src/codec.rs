use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use tokio_util::bytes::{Buf as _, BufMut as _, BytesMut};

use crate::message::MessagePayload;
/*
 *
 * ===== Codec =====
 *
 */
pub(crate) struct Codec;

impl Codec {
    // `read_bytes` read a fixed number of bytes
    pub(crate) fn read_bytes(src: &mut BytesMut, len: usize) -> Result<BytesMut> {
        if src.remaining() < len {
            return Err(anyhow!("Buffer underflow: not enough data"));
        }
        Ok(src.split_to(len))
    }

    // `read_length_prefixed` reads a length-prefixed field
    pub(crate) fn read_length_prefixed<T: DeserializeOwned>(src: &mut BytesMut) -> Result<T> {
        // First, read the length of the data, stored as a u32 (4 bytes).
        // If successful, proceed to read the actual data based on this length.
        let len = Codec::read_bytes(src, 4)?.get_u32() as usize;
        let data = Codec::read_bytes(src, len)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// `serialize_payload` serialize different types of payloads
    pub(crate) fn serialize_payload(payload: &MessagePayload) -> Result<Vec<u8>, bincode::Error> {
        match payload {
            MessagePayload::Ping(p) => bincode::serialize(p),
            MessagePayload::PingReq(p) => bincode::serialize(p),
            MessagePayload::Ack(p) => bincode::serialize(p),
            MessagePayload::Join(p) => bincode::serialize(p),
            MessagePayload::Leave(p) => bincode::serialize(p),
            MessagePayload::Fail(p) => bincode::serialize(p),
            MessagePayload::Update(p) => bincode::serialize(p),
            MessagePayload::AppMsg(p) => bincode::serialize(p),
        }
    }

    /// `encode_socket_addr` encode a SocketAddr into BytesMut
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

    /// `decode_socket_addr` decode a SocketAddr from BytesMut
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
        assert_eq!(Codec::read_bytes(&mut src, 5).unwrap(), b"Hello"[..]);
        // should error-out (buffer over flow)
        assert!(Codec::read_bytes(&mut src, 1).is_err());
    }

    #[test]
    fn test_read_length_prefixed() {
        let payload = TestPayload { data: "World".to_string() };
        let mut buffer = BytesMut::new();
        let payload_bytes = bincode::serialize(&payload).unwrap();
        buffer.put_u32(payload_bytes.len() as u32);
        buffer.extend_from_slice(&payload_bytes);

        let decoded_payload: TestPayload = Codec::read_length_prefixed(&mut buffer).unwrap();
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn test_encode_and_decode_socket_addr_ipv4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let mut dst = BytesMut::new();
        Codec::encode_socket_addr(&addr, &mut dst);

        let mut src = dst.clone();
        let decoded_addr = Codec::decode_socket_addr(&mut src).unwrap();
        assert_eq!(decoded_addr, addr);
    }

    #[test]
    fn test_encode_and_decode_socket_addr_ipv6() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 8080);
        let mut dst = BytesMut::new();
        // encode
        Codec::encode_socket_addr(&addr, &mut dst);

        // decode
        let mut src = dst.clone();
        let decoded_addr = Codec::decode_socket_addr(&mut src).unwrap();
       
        // assert equality 
        assert_eq!(decoded_addr, addr);
    }

    #[test]
    fn test_serialize_payload() {
        let payload = MessagePayload::Ping(PingPayload{ sequence_number: 1 });
        let serialized = Codec::serialize_payload(&payload).unwrap();
        let expected_bytes = bincode::serialize(&PingPayload{ sequence_number: 1 }).unwrap();
        assert_eq!(serialized, expected_bytes);
    }
}

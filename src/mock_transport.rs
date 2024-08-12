use std::{collections::VecDeque, net::IpAddr};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::error::Error;
use tokio::sync::{broadcast, Mutex};
use async_trait::async_trait;

use crate::state::NodeState;
use crate::{DefaultMetadata, Node};

use super::{DatagramTransport, Datagram};

pub struct MockDatagramTransport {
    incoming_queue: Arc<Mutex<VecDeque<Datagram>>>,
    outgoing_queue: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>,
    datagram_tx: broadcast::Sender<Datagram>,
    local_addr: SocketAddr,
}

impl MockDatagramTransport {
    pub fn new(local_addr: SocketAddr) -> Self {
        let (datagram_tx, _) = broadcast::channel(100);
        Self {
            incoming_queue: Arc::new(Mutex::new(VecDeque::new())),
            outgoing_queue: Arc::new(Mutex::new(VecDeque::new())),
            datagram_tx,
            local_addr,
        }
    }

    pub async fn inject_datagram(&self, datagram: Datagram) {
        self.incoming_queue.lock().await.push_back(datagram.clone());
        let _ = self.datagram_tx.send(datagram);
    }

    pub async fn get_sent_datagrams(&self) -> Vec<(SocketAddr, Vec<u8>)> {
        self.outgoing_queue.lock().await.drain(..).collect()
    }
}

#[async_trait]
impl DatagramTransport for MockDatagramTransport {
    fn incoming(&self) -> broadcast::Receiver<Datagram> {
        self.datagram_tx.subscribe()
    }

    async fn send_to(&self, target: SocketAddr,data: &[u8]) ->  Result<(), Box<dyn Error + Send + Sync>> {
        self.outgoing_queue.lock().await.push_back((target, data.to_vec()));
        Ok(())
    }

    fn local_addr(&self) -> Result<SocketAddr, Box<dyn Error + Send + Sync>>{
        Ok(self.local_addr)
    }

    async fn shutdown(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}


pub(crate) fn create_mock_node(name: &str, ip: &str, port: u16, state: NodeState) -> Node<DefaultMetadata> {
    Node::with_state(
        state,
        IpAddr::V4(ip.parse::<Ipv4Addr>().unwrap()),
        port,
        name.to_string(),
        0,
        DefaultMetadata::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_mock_datagram_transport() {
        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
        let mock_transport = MockDatagramTransport::new(local_addr);

        // Test incoming
        let mut rx = mock_transport.incoming();
        
        let test_datagram = Datagram {
            remote_addr: "127.0.0.1:9000".parse().unwrap(),
            data: vec![1, 2, 3],
        };
        mock_transport.inject_datagram(test_datagram.clone()).await;

        let received = rx.recv().await.unwrap();
        assert_eq!(received.remote_addr, test_datagram.remote_addr);
        assert_eq!(received.data, test_datagram.data);

        // Test send_to
        let target = "127.0.0.1:9001".parse().unwrap();
        let data = vec![4, 5, 6];
        mock_transport.send_to(target, &data).await.unwrap();

        let sent = mock_transport.get_sent_datagrams().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, target);
        assert_eq!(sent[0].1, data);

        // Test local_addr
        assert_eq!(mock_transport.local_addr().unwrap(), local_addr);
    }
}


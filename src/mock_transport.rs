use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::Duration;
use parking_lot::RwLock;
use crate::config::{DEFAULT_CHANNEL_BUFFER_SIZE, MAX_UDP_PACKET_SIZE};
use crate::node::Node;
use crate::transport::{NetworkTcpStream, NetworkUdpSocket, NodeTransport, TransportChannel};
use crate::DefaultMetadata;
use crate::{state::NodeState};


pub(crate) struct MockTransport {
    port: u16,
    ip_addr: IpAddr,
    tcp_listener: Arc<RwLock<Option<Arc<()>>>>, 
    udp_socket: Arc<RwLock<Option<Arc<()>>>>,
    dial_timeout: Duration,
    tcp_stream_tx: mpsc::Sender<NetworkTcpStream>,
    udp_socket_tx: mpsc::Sender<NetworkUdpSocket>,
    tcp_connections: Arc<Mutex<Vec<SocketAddr>>>,
    tcp_messages: Arc<Mutex<Vec<(SocketAddr, Vec<u8>)>>>,
    udp_messages: Arc<Mutex<Vec<(SocketAddr, Vec<u8>)>>>,
    tcp_dial_results: Arc<Mutex<VecDeque<Result<TcpStream>>>>,
}

impl MockTransport {
    pub(crate) fn new(port: u16, ip_addr: IpAddr, dial_timeout: Duration) -> (Self, TransportChannel) {
        let (tcp_stream_tx, tcp_stream_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE);
        let (udp_socket_tx, udp_socket_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE);

        (Self {
            port,
            ip_addr,
            tcp_listener: Arc::new(RwLock::new(None)),
            udp_socket: Arc::new(RwLock::new(None)),
            dial_timeout,
            tcp_stream_tx: tcp_stream_tx.clone(),
            udp_socket_tx: udp_socket_tx.clone(),
            tcp_connections: Arc::new(Mutex::new(Vec::new())),
            tcp_messages: Arc::new(Mutex::new(Vec::new())),
            udp_messages: Arc::new(Mutex::new(Vec::new())),
            tcp_dial_results: Arc::new(Mutex::new(VecDeque::new())),
        },
        TransportChannel {
            tcp_stream_rx,
            udp_socket_rx,
        })
    }

    pub async fn get_tcp_connections(&self) -> Vec<SocketAddr> {
        self.tcp_connections.lock().await.clone()
    }

    pub async fn get_tcp_messages(&self) -> Vec<(SocketAddr, Vec<u8>)> {
        self.tcp_messages.lock().await.clone()
    }

    pub async fn get_udp_messages(&self) -> Vec<(SocketAddr, Vec<u8>)> {
        self.udp_messages.lock().await.clone()
    }

    pub async fn get_last_udp_message(&self) -> Option<(SocketAddr, Vec<u8>)> {
        let messages =  self.udp_messages.lock().await;
        if messages.len() == 0 {
            return None;
        }
        Some(messages[messages.len()-1].clone())
    }

    pub async fn set_next_tcp_dial_result(&self, result: Result<TcpStream>) {
        self.tcp_dial_results.lock().await.push_back(result);
    }

    pub async fn clear(&self) {
        self.tcp_connections.lock().await.clear();
        self.tcp_messages.lock().await.clear();
        self.udp_messages.lock().await.clear();
        self.tcp_dial_results.lock().await.clear();
    }
}

#[async_trait]
impl NodeTransport for MockTransport {
    fn port(&self) -> u16 {
        self.port
    }

    fn ip_addr(&self) -> IpAddr {
        self.ip_addr
    }

    async fn dial_tcp(&self, addr: SocketAddr) -> Result<TcpStream> {
        self.tcp_connections.lock().await.push(addr);
        self.tcp_dial_results
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| Err(anyhow!("No more mock TCP dial results")))
    }

    async fn write_to_tcp(&self, stream: &mut TcpStream, message: &[u8]) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        self.tcp_messages
            .lock()
            .await
            .push((peer_addr, message.to_vec()));
        Ok(())
    }

    async fn write_to_udp(&self, addr: SocketAddr, message: &[u8]) -> Result<()> {
        if message.len() > MAX_UDP_PACKET_SIZE {
            return Err(anyhow!("Message too large for UDP packet"));
        }
        self.udp_messages
            .lock()
            .await
            .push((addr, message.to_vec()));
        Ok(())
    }

    async fn tcp_stream_listener(&self) -> Result<()> {
        Ok(())
    }

    async fn udp_socket_listener(&self) -> Result<()> {
        Ok(())
    }

    async fn bind_tcp_listener(&self) -> Result<()> {
        *self.tcp_listener.write() = Some(Arc::new(()));
        Ok(())
    }

    async fn bind_udp_socket(&self) -> Result<()> {
        *self.udp_socket.write() = Some(Arc::new(()));
        Ok(())
    }

    fn tcp_stream_tx(&self) -> mpsc::Sender<NetworkTcpStream> {
        self.tcp_stream_tx.clone()
    }

    fn udp_socket_tx(&self) -> mpsc::Sender<NetworkUdpSocket> {
        self.udp_socket_tx.clone()
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


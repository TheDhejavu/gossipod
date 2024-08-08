use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use log::{error, info, warn};
use tokio::net::TcpStream;
use tokio::net::{TcpListener as TokioTcpListener, UdpSocket as TokioUdpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use tokio::sync::mpsc;
use parking_lot::RwLock;
use async_trait::async_trait;
use crate::config::{DEFAULT_CHANNEL_BUFFER_SIZE, DEFAULT_MESSAGE_BUFFER_SIZE, MAX_UDP_PACKET_SIZE};
use crate::message::Message;

pub(crate) type NetworkTcpStream = (SocketAddr, TcpStream);
pub(crate) type NetworkUdpSocket = (SocketAddr, Vec<u8>);

#[async_trait]
pub trait NodeTransport: Send + Sync {
    fn port(&self) -> u16;
    fn ip_addr(&self) -> IpAddr;
    async fn dial_tcp(&self, addr: SocketAddr) -> Result<TcpStream>;
    async fn write_to_tcp(&self, stream: &mut TcpStream, message: &[u8]) -> Result<()>;
    async fn write_to_udp(&self, addr: SocketAddr, message: &[u8]) -> Result<()>;
    async fn tcp_stream_listener(&self) -> Result<()>;
    async fn udp_socket_listener(&self) -> Result<()>;
    async fn bind_tcp_listener(&self) -> Result<()>;
    async fn bind_udp_socket(&self) -> Result<()>;
    fn tcp_stream_tx(&self) -> mpsc::Sender<NetworkTcpStream>;
    fn udp_socket_tx(&self) -> mpsc::Sender<NetworkUdpSocket>;
}

/// Transport is responsible for sending messages to peers
#[derive(Clone)]
pub(crate) struct Transport {
    pub(crate) port: u16,
    pub(crate) ip_addr: IpAddr,
    // Local TCP listener, the idea is for TCP to handle node-specific messages where reliability and connection
    // are important
    pub(crate) tcp_listener: Arc<RwLock<Option<Arc<TokioTcpListener>>>>,
    // Local UDP listener, the idea is for UDP to handle protocol-related messages where connection is 
    // not a must
    pub(crate) udp_socket: Arc<RwLock<Option<Arc<TokioUdpSocket>>>>,
    pub(crate) dial_timeout: Duration,
    pub(crate) tcp_stream_tx: mpsc::Sender<NetworkTcpStream>,
    pub(crate) udp_socket_tx: mpsc::Sender<NetworkUdpSocket>,
}


pub struct TransportChannel {
    pub tcp_stream_rx: mpsc::Receiver<NetworkTcpStream>,
    pub udp_socket_rx: mpsc::Receiver<NetworkUdpSocket>,
}

impl Transport {
    /// Creates a new Transport instance and its associated TransportChannel
    pub fn new(port: u16, ip_addr: IpAddr, dial_timeout: Duration) -> (Self, TransportChannel) {
        let (tcp_stream_tx, tcp_stream_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE); 
        let (udp_socket_tx, udp_socket_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE); 

        (
            Self {
                port,
                ip_addr,
                tcp_listener: Arc::new(RwLock::new(None)),
                udp_socket: Arc::new(RwLock::new(None)),
                dial_timeout,
                tcp_stream_tx,
                udp_socket_tx
            }, 
            TransportChannel {
                tcp_stream_rx,
                udp_socket_rx
            }
        )
    }

    /// Reads a message from a TCP stream
    pub(crate) async fn read_stream(stream: &mut TcpStream) -> Result<Message> {
        let mut buffer = vec![0u8; DEFAULT_MESSAGE_BUFFER_SIZE];
        match stream.read(&mut buffer).await {
            Ok(n) => {
                buffer.truncate(n);
                Message::from_vec(&buffer).context("Failed to decode message")
            }
            Err(e) => {
                warn!("Failed to read from TCP stream: {:?}", e);
                Err(anyhow!("Stream read error: {}", e))
            }
        }
    }

    /// Reads a message from a UDP socket
    pub(crate)  async fn read_socket(buffer: Vec<u8>) -> Result<Message> {
        Message::from_vec(&buffer).context("Failed to decode message")
    }
}

#[async_trait]
impl NodeTransport for Transport {
    fn port(&self) -> u16 {
        self.port
    }

    fn ip_addr(&self) -> IpAddr {
        self.ip_addr
    }

    /// Establishes a TCP connection to the specified address
    async fn dial_tcp(&self, addr: SocketAddr) -> Result<TcpStream> {
        timeout(self.dial_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("TCP connection timed out"))?
            .context("Failed to establish TCP connection")
    }

    /// Writes data to an established TCP stream
    async fn write_to_tcp(&self, stream: &mut TcpStream, message: &[u8]) -> Result<()> {
        timeout(self.dial_timeout, stream.write_all(message))
            .await
            .map_err(|_| anyhow!("TCP write timed out"))?
            .context("Failed to write to TCP stream")
    }

    /// Sends a UDP message to the specified address
    async fn write_to_udp(&self, addr: SocketAddr, message: &[u8]) -> Result<()> {
        if message.len() > MAX_UDP_PACKET_SIZE {
            return Err(anyhow!("Message too large for UDP packet, allowed {}bytes but got {}bytes", MAX_UDP_PACKET_SIZE, message.len()));
        }

        let socket = self.udp_socket.read().as_ref()
            .ok_or_else(|| anyhow!("UDP socket not initialized"))?
            .clone();

        timeout(self.dial_timeout, socket.send_to(message, addr))
            .await
            .map_err(|_| anyhow!("UDP send timed out"))?
            .map(|_| ())
            .context("Failed to send UDP message")
    }

    /// Listens for incoming TCP connections
    async fn tcp_stream_listener(&self) -> Result<()> {
        let listener = self.tcp_listener.read().as_ref()
            .ok_or_else(|| anyhow!("TCP listener not initialized"))?
            .clone();
        let tcp_stream_tx = self.tcp_stream_tx.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("[RECV] Incoming TCP connection from: {}", addr);
                    if let Err(e) = tcp_stream_tx.send((addr, stream)).await {
                        warn!("Failed to send TCP stream to channel: {:?}", e);
                    }
                }
                Err(e) => warn!("[WARN] Failed to accept TCP connection: {:?}", e),
            }
        }
    }

    /// Listens for incoming UDP messages
    async fn udp_socket_listener(&self) -> Result<()> {
        let socket = self.udp_socket.read().as_ref()
            .ok_or_else(|| anyhow!("UDP socket not initialized"))?
            .clone();
        let udp_socket_tx = self.udp_socket_tx.clone();
    
        let mut buf = vec![0u8; DEFAULT_MESSAGE_BUFFER_SIZE];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    info!("[RECV] Incoming UDP message from: {}", addr);
                    if let Err(e) = udp_socket_tx.send((addr, buf[..len].to_vec())).await {
                        warn!("Failed to send UDP message to channel: {:?}", e);
                    }
                }
                Err(e) => error!("Failed to receive UDP message: {:?}", e),
            }
        }
    }

    /// Binds the TCP listener to the configured address and port
    async fn bind_tcp_listener(&self) -> Result<()> {
        let bind_addr = SocketAddr::new(self.ip_addr, self.port);
        let listener = TokioTcpListener::bind(bind_addr).await?;
        *self.tcp_listener.write() = Some(Arc::new(listener));
        Ok(())
    }

    /// Binds the UDP socket to the configured address and port
    async fn bind_udp_socket(&self) -> Result<()> {
        let bind_addr = SocketAddr::new(self.ip_addr, self.port);
        let socket = TokioUdpSocket::bind(bind_addr).await?;
        *self.udp_socket.write() = Some(Arc::new(socket));
        Ok(())
    }

    fn tcp_stream_tx(&self) -> mpsc::Sender<NetworkTcpStream> {
        self.tcp_stream_tx.clone()
    }

    fn udp_socket_tx(&self) -> mpsc::Sender<NetworkUdpSocket> {
        self.udp_socket_tx.clone()
    }
}

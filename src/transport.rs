use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use log::{error, info, warn};
use tokio::net::TcpStream;
use tokio::net::{TcpListener as TokioTcpListener, UdpSocket as TokioUdpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::config::{DEFAULT_CHANNEL_BUFFER_SIZE, DEFAULT_MESSAGE_BUFFER_SIZE};


type NetworkPacket = (SocketAddr, Vec<u8>);

/// Transport is responsible for sending messages to peers
#[derive(Clone)]
pub(crate) struct Transport{
    pub(crate) port: u16,

    pub(crate) ip_addr: IpAddr,

    // Local TCP listener, the idea is for TCP to handle node-specific messages where reliability and connection
    // are important
    pub(crate) tcp_listener: Arc<RwLock<Option<Arc<TokioTcpListener>>>>,

    // Local UDP listener, the idea is for UDP to handle protocol-related messages where connection is 
    // not a must
    pub(crate) udp_socket: Arc<RwLock<Option<Arc<TokioUdpSocket>>>>,

    pub(crate) dial_timeout: Duration,

    pub(crate) tcp_stream_tx: mpsc::Sender<NetworkPacket>,

    pub(crate) udp_packet_tx: mpsc::Sender<NetworkPacket>,
}

pub(crate) struct TransportChannel {
    pub(crate) tcp_stream_rx: mpsc::Receiver<NetworkPacket>,
    pub(crate) udp_packet_rx: mpsc::Receiver<NetworkPacket>,
}

#[async_trait::async_trait]
pub(crate) trait NodeTransport: Send + Sync  {
    async fn dial_tcp(&self, addr: SocketAddr) -> Result<TcpStream>;
    async fn write_to_tcp(&self, stream: &mut TcpStream, message: &[u8]) -> Result<()>;
    async fn write_to_udp(&self, addr: SocketAddr, message: &[u8]) -> Result<()>;
}

#[async_trait::async_trait]
impl NodeTransport for Transport {
    
    /// Establishes a TCP connection to the specified address
    async  fn dial_tcp(&self, addr: SocketAddr) -> Result<TcpStream> {
        timeout(self.dial_timeout, TcpStream::connect(addr))
            .await
            .context("TCP connection timed out")?
            .context("Failed to establish TCP connection")
    }

    /// Writes data to an established TCP stream
    async fn write_to_tcp(&self, stream: &mut TcpStream, message: &[u8]) -> Result<()> {
        timeout(self.dial_timeout, stream.write_all(message))
            .await
            .context("TCP write timed out")?
            .context("Failed to write to TCP stream")
    }

    /// Sends a UDP message to the specified address
    async fn write_to_udp(&self, addr: SocketAddr, message: &[u8]) -> Result<()> {
        let udp_socket = self.udp_socket.read().await;
        let socket = udp_socket.as_ref().ok_or_else(|| anyhow!("UDP socket not initialized"))?;
        timeout(self.dial_timeout, socket.send_to(message, addr))
            .await
            .context("UDP send timed out")?
            .map(|_| ())
            .context("Failed to send UDP message")
    }
}

impl Transport {
    pub(crate) fn new(port: u16, ip_addr: IpAddr, dial_timeout: Duration) -> (Self, TransportChannel) {
        let (tcp_stream_tx, tcp_stream_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE); 
        let (udp_packet_tx, udp_packet_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE); 

        (
            Self {
                port,
                ip_addr,
                tcp_listener: Arc::new(RwLock::new(None)),
                udp_socket: Arc::new(RwLock::new(None)),
                dial_timeout,
                tcp_stream_tx,
                udp_packet_tx
            }, 
            TransportChannel {
                tcp_stream_rx,
                udp_packet_rx
            }
        )
    } 
    pub(crate) async fn tcp_stream_listener(&self) -> Result<()> {
        let tcp_listener = self.tcp_listener.read().await;
        let listener = tcp_listener.as_ref().context("TCP listener not initialized")?;
        let tcp_stream_tx = self.tcp_stream_tx.clone();

        loop {
            match listener.accept().await {
                Ok((mut stream, addr)) => {
                    info!("[RECV] Incoming TCP Message");
                    // Spin-up a new background task for every incoming message
                    // NOTE: I have a concern on how feasible this will be in the long run, spinning
                    // up a new background task seems like a bad idea, what happens when we have loads of messages ?
                    // Or what's the Probability of out-of-order processing of message where one read is more than subsquent
                    // reads. In UDP, messages by default can be out-of-order but in a case like this, we might want some level
                    // of ordering.
                    // IDEA? use Timestamped Or Sequence Numbers for handling of each data
                    let tcp_stream_tx_clone = tcp_stream_tx.clone();
                    tokio::spawn(async move {
                        let mut buffer = vec![0u8; DEFAULT_MESSAGE_BUFFER_SIZE];
                        match stream.read(&mut buffer).await {
                            Ok(n) => {
                                buffer.truncate(n);
                                if let Err(e) = tcp_stream_tx_clone.send((addr, buffer)).await {
                                    warn!("Failed to send TCP message to channel: {:?}", e);
                                }
                            }
                            Err(e) => warn!("Failed to read from TCP stream: {:?}", e),
                        }
                    });
                }
                Err(e) => warn!("[WARN] Failed to accept TCP connection: {:?}", e),
            }
        }
    }

    pub(crate) async fn udp_packet_listener(&self) -> Result<()> {
        let udp_socket = self.udp_socket.read().await;
        let socket = udp_socket.as_ref().context("UDP socket not initialized")?;
        let udp_packet_tx = self.udp_packet_tx.clone();

        let mut buffer = [0u8; DEFAULT_MESSAGE_BUFFER_SIZE];
        loop {
            match socket.recv_from(&mut buffer).await {
                Ok((len, addr)) => {
                    info!("[RECV] Incoming UDP Message");
                    let data = buffer[..len].to_vec();
                    if let Err(e) = udp_packet_tx.send((addr, data)).await {
                        error!("Failed to send UDP message to channel: {:?}", e);
                    }
                }
                Err(e) => error!("Failed to receive UDP message: {:?}", e),
            }
        }
    }
    /// Binds the TCP listener to the configured address and port
    pub(crate) async fn bind_tcp_listener(&self) -> Result<()> {
        let bind_addr = SocketAddr::new(self.ip_addr, self.port);
        let listener = TokioTcpListener::bind(bind_addr).await?;
        let mut tcp_listener = self.tcp_listener.write().await;
        *tcp_listener = Some(Arc::new(listener));
        Ok(())
    }

    /// Binds the UDP socket to the configured address and port
    pub(crate) async fn bind_udp_socket(&self) -> Result<()> {
        let bind_addr = SocketAddr::new(self.ip_addr, self.port);
        let socket = TokioUdpSocket::bind(bind_addr).await?;
        let mut udp_socket = self.udp_socket.write().await;
        *udp_socket = Some(Arc::new(socket));
        Ok(())
    }
}
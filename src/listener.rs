use std::net::SocketAddr;
use anyhow::Result;
use log::{error, info, warn};
use tokio::{
    net::TcpStream,
    select,
    sync::broadcast,
};
use crate::{
    message::{Message, MessageType}, node::NodeMetadata, transport::{Transport, TransportChannel}, Gossipod
};

pub(crate) struct EventListener<M: NodeMetadata> {
    gossipod: Gossipod<M>,
    transport_channel: TransportChannel,
    shutdown: broadcast::Sender<()>,
}

impl<M: NodeMetadata> EventListener<M> {
    /// Creates a new EventListener instance.
    ///
    /// This listener will handle incoming TCP and UDP connections and process
    /// messages according to the Gossipod protocol.
    pub(crate) fn new(
        gossipod: Gossipod<M>, 
        transport_channel: TransportChannel,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self { 
            gossipod, 
            transport_channel,
            shutdown,
        }
    }

    /// Runs the main event loop for listening to incoming connections.
    ///
    /// This method will continuously listen for TCP and UDP connections
    /// until a shutdown signal is received.
    pub(crate) async fn run_listeners(&mut self) -> Result<()> {
        info!("Starting Listener...");
        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            select! {
                tcp_stream = self.transport_channel.tcp_stream_rx.recv() => {
                    if let Some((addr, stream)) = tcp_stream {
                        self.handle_stream(addr, stream).await?;
                    }
                },
                udp_socket = self.transport_channel.udp_socket_rx.recv() => {
                    if let Some((addr, socket)) = udp_socket {
                        self.handle_socket(addr, socket).await?;
                    }
                },
                _ = shutdown_rx.recv() => {
                    info!("[RECV] Closing Event Listeners...");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handles incoming TCP stream connections.
    ///
    /// This method reads a message from the stream and processes it
    /// based on its type.
    async fn handle_stream(&self, addr: SocketAddr, mut stream: TcpStream) -> Result<()> {
        let gossipod = self.gossipod.clone();
        tokio::spawn(async move {
            match Transport::read_stream(&mut stream).await {
                Ok(message) => Self::process_tcp_stream(gossipod, stream, message, addr).await,
                Err(e) => {
                    error!("[ERR] error reading from stream ({}): {:?}", addr, e);
                    Ok(())
                },
            }
        });
        Ok(())
    }

    /// Handles incoming UDP socket connections.
    ///
    /// This method reads a message from the socket and processes it
    /// based on its type.
    async fn handle_socket(&self, addr: SocketAddr, socket: Vec<u8>) -> Result<()> {
        let gossipod = self.gossipod.clone();
        tokio::spawn(async move {
            match Transport::read_socket(socket).await {
                Ok(message) => Self::process_udp_packet(gossipod, message, addr).await,
                Err(e) => {
                    error!("[ERR] error reading from socket ({}): {:?}", addr, e);
                    Ok(())
                },
            }
        });
        Ok(())
    }

    /// Processes a received message based on its type.
    ///
    /// This method delegates the handling of different message types
    /// to the appropriate methods in the Gossipod instance.
    async fn process_tcp_stream(gossipod: Gossipod<M>, stream: TcpStream, message: Message, addr: SocketAddr) -> Result<()> {
        match message.msg_type {
            MessageType::SyncReq => gossipod.handle_sync_req(stream, message).await,
            MessageType::AppMsg => gossipod.handle_app_msg(message).await,
            _ => {
                warn!("[ERR] Unexpected message type {} for TCP from {}", message.msg_type, addr);
                Ok(())
            }
        }
    }

    /// Processes a received UDP message based on its type.
    async fn process_udp_packet(gossipod: Gossipod<M>, message: Message, addr: SocketAddr) -> Result<()> {
        match message.msg_type {
            MessageType::Ping => gossipod.handle_ping(message).await,
            MessageType::PingReq => gossipod.handle_ping_req(message).await,
            MessageType::Ack => gossipod.handle_ack(message).await,
            MessageType::NoAck => gossipod.handle_no_ack(message).await,
            MessageType::Broadcast => gossipod.handle_broadcast(message).await,
            MessageType::AppMsg => gossipod.handle_app_msg(message).await,
            MessageType::SyncReq => {
                warn!("[ERR] Received SyncReq over UDP from {}, ignoring", addr);
                Ok(())
            },
            MessageType::AppMsg => gossipod.handle_app_msg(message).await,
            _ => {
                warn!("[ERR] Unexpected message type {} for UDP from {}", message.msg_type, addr);
                Ok(())
            }
        }
    }
}
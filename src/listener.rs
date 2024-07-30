use std::net::SocketAddr;
use anyhow::{Context as _, Result};
use log::{error, info, warn};
use tokio::{select, sync::broadcast};
use crate::{message::{Message, MessageType}, transport::{Protocol, TransportChannel}, Gossipod};

pub(crate) struct EventListener {
    gossipod: Gossipod,
    transport_channel: TransportChannel,
    shutdown: broadcast::Sender<()>,
}

impl EventListener {
    pub(crate) fn new(
        gossipod: Gossipod, 
        transport_channel: TransportChannel,
        shutdown: broadcast::Sender<()>,
    ) -> EventListener {
        Self { 
            gossipod, 
            transport_channel,
            shutdown,
        }
    }

    pub(crate) async fn run_listeners(&mut self) -> Result<()> {
        info!("Starting Listener...");
        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            select! {
                tcp_stream = self.transport_channel.tcp_stream_rx.recv() => {
                    if let Some((addr, data)) = tcp_stream {
                        self.spawn_incoming_handler(addr, data, Protocol::TCP).await;
                    }
                },
                udp_packet = self.transport_channel.udp_packet_rx.recv() => {
                    if let Some((addr, data)) = udp_packet {
                        self.spawn_incoming_handler(addr, data, Protocol::UDP).await;
                    }
                },
                _ = shutdown_rx.recv() => {
                    info!("[GOSSIPOD] Closing Event Listeners...");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn spawn_incoming_handler(&self, addr: SocketAddr, data: Vec<u8>, protocol: Protocol) {
        let gossipod = self.gossipod.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::handle_incoming(gossipod, addr, data, protocol.clone()).await {
                error!("Error handling {} message: {:?}", protocol, e);
            }
        });
    }

    async fn handle_incoming(gossipod: Gossipod, addr: SocketAddr, data: Vec<u8>, protocol: Protocol) -> Result<()> {
        // This decoding uses a generic MessageCodec. For a more granular approach where each 
        // message type handles its own decoding/encoding, this method may not be suitable. 
        // In such cases, we might pass the raw bytes and delegate decoding to the respective 
        // message type handlers.
        let message = Message::from_vec(&data)
            .context(format!("Failed to decode {} message", protocol))?;
        
        info!("Received {} message from {}: {:?}", protocol, addr, message);
        
        Self::process_message(gossipod, addr, message, protocol).await
    }
    
    async fn process_message(gossipod: Gossipod, addr: SocketAddr, message: Message, protocol: Protocol) -> Result<()> {
        match (message.msg_type, protocol.clone()) {
            (MessageType::Ping, Protocol::UDP) => Self::handle_ping(gossipod, addr, message).await,
            (MessageType::PingReq, Protocol::UDP) => Self::handle_ping_req(gossipod, addr, message).await,
            (MessageType::Ack, Protocol::UDP) => Self::handle_ack(gossipod, addr, message).await,
            (MessageType::Join, Protocol::TCP) => Self::handle_join(gossipod, addr, message).await,
            (MessageType::Leave, Protocol::TCP) => Self::handle_leave(gossipod, addr, message).await,
            (MessageType::Dead, Protocol::UDP) => Self::handle_dead(gossipod, addr, message).await,
            (MessageType::SyncReq, Protocol::TCP) => Self::handle_sync_req(gossipod, addr, message).await,
            (MessageType::AppMsg, Protocol::TCP) => Self::handle_app_msg(gossipod, addr, message).await,
            _ => {
                warn!("Unexpected message type {} for protocol {}", message.msg_type, protocol);
                Ok(())
            }
        }
    }

    async fn handle_ping(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[UDP] Handling `PING` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_join(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[TCP] Handling `JOIN` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_dead(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[UDP] Handling `DEAD` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_ping_req(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[UDP] Handling `PING_REQ` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_ack(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[UDP] Handling `ACK` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_leave(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[TCP] Handling `LEAVE` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_sync_req(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[TCP] Handling `SYNC_REQ` message with ID: {}", message.id);
        unimplemented!()
    }

    async fn handle_app_msg(gossipod: Gossipod, addr: SocketAddr, message: Message) -> Result<()> {
        info!("[TCP] Handling `APP_MSG` message with ID: {}", message.id);
        unimplemented!()
    }
}
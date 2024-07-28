use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use log::{error, info};
use tokio::{select, sync::broadcast};
use tokio_util::{bytes::BytesMut, codec::Decoder as _};

use crate::{message::{MessageCodec, MessageType}, transport::TransportChannel, Gossipod};

/*
 *
 * ===== SwimListener =====
 *
 */
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

    pub(crate) async fn run_listeners(&mut self) {

        info!("Starting Listener....");
        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            select! {
                tcp_result = self.transport_channel.tcp_stream_rx.recv() => {
                    if let Some((addr, data)) = tcp_result {
                        let gossipod_clone = self.gossipod.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_tcp_message(gossipod_clone, addr, data).await {
                                error!("Error handling TCP stream: {:?}", e);
                            }
                        });
                    }
                },
                udp_result = self.transport_channel.udp_packet_rx.recv() => {
                    if let Some((addr, data)) = udp_result {
                        let gossipod_clone = self.gossipod.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_udp_message(gossipod_clone, addr, data).await {
                                error!("Error handling UDP packet: {:?}", e);
                            }
                        });
                    }
                },
                _ = shutdown_rx.recv() => {
                    info!("[SWIM] Closing Listener...");
                }
            }
        }
    }
    async fn handle_tcp_message(gossipod: Gossipod, addr: SocketAddr, data: Vec<u8>) -> Result<()>{
        // This decoding uses a generic MessageCodec. For a more granular approach where each 
        // message type handles its own decoding/encoding, this method may not be suitable. 
        // In such cases, we might pass the raw bytes and delegate decoding to the respective 
        // message type handlers.

        let mut codec = MessageCodec::new(); 
        let mut bytes = BytesMut::from(&data[..]);
        let message  = match codec.decode(&mut bytes) {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(anyhow!("unable to decode message")),
            Err(e) => Err(anyhow!(e.to_string()))
        }?;

        info!("Received message from {}: {:?}", addr, message);
        // Handle the UDP Message Types
        match message.msg_type {
            MessageType::Ping => {
                info!("[MSG] Handling `PING` message with ID: {}", message.id);
                // let sender = swim.get_local_node().await?;
                // swim.inner.message_broker.send_ping(sender.socket_addr()?, addr);
            }
            MessageType::PingReq => {
                info!("MSG] Handling `PING_REQ` message with ID: {}", message.id);
            }
            MessageType::Ack => {
                info!("MSG] Handling `ACK` message with ID: {}", message.id);
            }
            MessageType::Leave => {
                info!("MSG] Handling `LEAVE` message with ID: {}", message.id);
            }
            MessageType::Join => {
                info!("MSG] Handling `JOIN]` message with ID: {}", message.id);
            }
            MessageType::Update => {
                info!("MSG] Handling `UPDATE` message with ID: {}", message.id);
            }
            MessageType::Fail => {
                info!("MSG] Handling `FAIL` message with ID: {}", message.id);
            }
            MessageType::AppMsg => {
                info!("MSG] Handling `APP_MSG` message with ID: {}", message.id);
            }
        }
        Ok(())
    }
    async fn handle_udp_message(gossipod: Gossipod,addr: SocketAddr, data: Vec<u8>) -> Result<()>{
        let mut codec = MessageCodec::new(); 
        let mut bytes = BytesMut::from(&data[..]);
        let message  = match codec.decode(&mut bytes) {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(anyhow!("unable to decode message")),
            Err(e) => Err(anyhow!(e.to_string()))
        }?;

        info!("Received message from {}: {:?}", addr, message);
        // Handle the UDP Message Types
        match message.msg_type {
            MessageType::Ping => {
                info!("[MSG] Handling `PING` message with ID: {}", message.id);
                // self.inner.net_message.send_ping(sender, self);
            }
            MessageType::PingReq => {
                info!("MSG] Handling `PING_REQ` message with ID: {}", message.id);
            }
            MessageType::Ack => {
                info!("MSG] Handling `ACK` message with ID: {}", message.id);
            }
            MessageType::Leave => {
                info!("MSG] Handling `LEAVE` message with ID: {}", message.id);
            }
            MessageType::Join => {
                info!("MSG] Handling `JOIN]` message with ID: {}", message.id);
            }
            MessageType::Update => {
                info!("MSG] Handling `UPDATE` message with ID: {}", message.id);
            }
            MessageType::Fail => {
                info!("MSG] Handling `FAIL` message with ID: {}", message.id);
            }
            MessageType::AppMsg => {
                info!("MSG] Handling `APP_MSG` message with ID: {}", message.id);
            }
        }
        Ok(())
    }
    
}
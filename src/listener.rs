use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use log::{error, info};
use tokio::{select, sync::broadcast};
use tokio_util::{bytes::BytesMut, codec::Decoder as _};

use crate::{message::{Message, MessageCodec, MessageType}, transport::TransportChannel, Swim};

pub(crate) struct SwimListener {
    swim: Swim,
    transport_channel: TransportChannel,
    shutdown: broadcast::Sender<()>,
}

impl SwimListener {
    pub(crate) fn new(
        swim: Swim, 
        transport_channel: TransportChannel,
        shutdown: broadcast::Sender<()>,
    ) -> SwimListener {
        Self { 
            swim, 
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
                        let swim_clone = self.swim.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_tcp_message(swim_clone, addr, data).await {
                                error!("Error handling TCP stream: {:?}", e);
                            }
                        });
                    }
                },
                udp_result = self.transport_channel.udp_packet_rx.recv() => {
                    if let Some((addr, data)) = udp_result {
                        let swim_clone = self.swim.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_udp_message(swim_clone, addr, data).await {
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
    async fn handle_tcp_message(swim: Swim, addr: SocketAddr, data: Vec<u8>) -> Result<()>{
        // Decoding here, it uses generic MessageCodec, for a more granular approach where we want,
        // each message to handle it's own decoding / encoding, then this might not work. We might result to
        // passing the raw byte and leave it the decoding to it's respective message type handler.

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
    async fn handle_udp_message(swim: Swim,addr: SocketAddr, data: Vec<u8>) -> Result<()>{
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
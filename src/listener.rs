use log::{error, info};
use tokio::select;

use crate::{transport::TransportChannel, Swim};

pub(crate) struct SwimListener {
    swim: Swim,
    transport_channel: TransportChannel,
}

impl SwimListener {
    pub(crate) fn new(swim: Swim, transport_channel: TransportChannel) -> SwimListener {
        Self { 
            swim, 
            transport_channel,
        }
    }

    pub(crate) async fn run_listeners(&mut self) {
        info!("Starting Listener....");
        
        loop {
            select! {
                tcp_result = self.transport_channel.tcp_stream_rx.recv() => {
                    if let Some((addr, data)) = tcp_result {
                        let mut swim_clone = self.swim.clone();
                        tokio::spawn(async move {
                            if let Err(e) = swim_clone.handle_tcp_stream(addr, data).await {
                                error!("Error handling TCP stream: {:?}", e);
                            }
                        });
                    }
                },
                udp_result = self.transport_channel.udp_packet_rx.recv() => {
                    if let Some((addr, packet)) = udp_result {
                        let mut swim_clone = self.swim.clone();
                        tokio::spawn(async move {
                            if let Err(e) = swim_clone.handle_udp_packet(addr, packet).await {
                                error!("Error handling UDP packet: {:?}", e);
                            }
                        });
                    }
                }
            }
        }
    }
}
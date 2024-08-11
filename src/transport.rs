use tokio::{sync::broadcast, time};
use anyhow::{Result, Context};
use async_trait::async_trait;
use std::{net::IpAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use std::net::SocketAddr;
use log::*;

use crate::{backoff::BackOff, config::{DEFAULT_CHANNEL_BUFFER_SIZE, DEFAULT_IP_ADDR}, ip_addr::IpAddress};

#[derive(Debug, Clone)]
pub struct Datagram {
    pub remote_addr: SocketAddr,
    pub data: Vec<u8>,
}

#[async_trait]
pub trait DatagramTransport: Send + Sync {
    fn incoming(&self) -> broadcast::Receiver<Datagram>;
    async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<()>;
    fn local_addr(&self) -> Result<SocketAddr>;
    async fn shutdown(&self) -> Result<()>;
}


pub struct DefaultTransport {
    datagram_tx: broadcast::Sender<Datagram>,
    udp_socket: Arc<UdpSocket>,
    shutdown_signal: broadcast::Sender<()>,
}

impl DefaultTransport {
    pub async fn new(ip_addr: IpAddr, port: u16) -> Result<Self> {
        let addr = SocketAddr::new(ip_addr, port);
        let udp_socket = UdpSocket::bind(addr).await.context("Failed to bind UDP socket")?;
        let local_addr = udp_socket.local_addr().context("Failed to get local address")?;

        let (datagram_tx, _) = broadcast::channel(DEFAULT_CHANNEL_BUFFER_SIZE);
        let (shutdown_signal, _) = broadcast::channel(1);

        let transport = Self {
            datagram_tx,
            udp_socket: Arc::new(udp_socket),
            shutdown_signal,
        };

        // Note: When the Local Host (127.0.0.1 or 0.0.0.0) is provisioned, it is automatically bound to the system's private IP.
        // If a Private IP address is bound, the local host (127.0.0.1) becomes irrelevant.
        let private_ip_addr = IpAddress::find_system_ip()?;
        if addr.ip().to_string() == DEFAULT_IP_ADDR || addr.ip().to_string() == "0.0.0.0" {
            info!(
                "> [GOSSIPOD] Binding to all network interfaces: {}:{} (Private IP: {}:{})",
                addr.ip(),
                addr.port(),
                private_ip_addr.to_string(),
                addr.port(),
            );
        } else {
            info!(
                "> [GOSSIPOD] Binding to specific IP: {}:{}",
                addr.ip(),
                addr.port(),
            );
        }

        transport.spawn_datagram_listener();

        Ok(transport)
    }

    fn spawn_datagram_listener(&self) {
        let socket = self.udp_socket.clone();
        let tx = self.datagram_tx.clone();
        let mut shutdown_rx = self.shutdown_signal.subscribe();
        let backoff = BackOff::new();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
               
                if backoff.is_circuit_open() {
                    let wait_time = backoff.time_until_open()
                        .unwrap_or(Duration::from_secs(2)); 
                    
                    warn!("Circuit breaker is open. Waiting for {:?} before next attempt...", wait_time);
                    
                    tokio::select! {
                        _ = time::sleep(wait_time) => {
                            if backoff.is_circuit_open() {
                                warn!("Circuit breaker is still open after waiting. Will retry.");
                            } else {
                                info!("Circuit breaker has closed. Resuming normal operation.");
                            }
                        },
                        _ = shutdown_rx.recv() => {
                            info!("[RECV] UDP listener received shutdown signal");
                            break;
                        }
                    }
                    continue;
                }
    
                tokio::select! {
                    result = socket.recv_from(&mut buf) => {
                        match result {
                            Ok((len, src)) => {
                                debug!("Successful operation. BackOff reset.");
                                let datagram = Datagram {
                                    remote_addr: src,
                                    data: buf[..len].to_vec(),
                                };
                                if tx.send(datagram).is_err() {
                                    error!("All UDP receivers have been dropped");
                                   let  _ = backoff.record_failure();
                                } else {
                                    let _ = backoff.record_success();
                                    continue;
                                }
                            }
                            Err(e) => {
                                let (failures, _) = backoff.record_failure();
                                error!("Error receiving UDP datagram: {} Consecutive failures: {}", e, failures);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("[RECV] UDP listener received shutdown signal");
                        break;
                    }
                }


                let delay = backoff.calculate_delay();
                if delay > Duration::from_secs(0) {
                    time::sleep(delay).await;
                }
            }
            info!("UDP listener shut down");
        });
    }
}

#[async_trait]
impl DatagramTransport for DefaultTransport {
    fn incoming(&self) -> broadcast::Receiver<Datagram> {
        self.datagram_tx.subscribe()
    }

    async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<()> {
        self.udp_socket.send_to(&data, target).await
            .context("Failed to send UDP datagram")?;
        Ok(())
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.udp_socket.local_addr()?)
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown_signal.send(())
            .map_err(|_| anyhow::anyhow!("Failed to send shutdown signal"))?;
        Ok(())
    }
}

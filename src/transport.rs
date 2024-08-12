use pin_project::pin_project;
use tokio::{sync::broadcast, time};
use anyhow::{Result, Context};
use async_trait::async_trait;
use std::{net::IpAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use std::net::SocketAddr;
use std::error::Error;
use log::*;
use std::{
    io,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use futures::{ready, Stream};
use tokio::net::{TcpListener, TcpStream};
use crate::{backoff::BackOff, config::{DEFAULT_CHANNEL_BUFFER_SIZE, DEFAULT_IP_ADDR}, ip_addr::IpAddress};

#[derive(Debug, Clone)]
pub struct Datagram {
    pub remote_addr: SocketAddr,
    pub data: Vec<u8>,
}

#[async_trait]
pub trait DatagramTransport: Send + Sync {
    fn incoming(&self) -> broadcast::Receiver<Datagram>;
    async fn send_to(&self, target: SocketAddr, data: &[u8]) ->Result<(), Box<dyn Error + Send + Sync>>;
    fn local_addr(&self) -> Result<SocketAddr, Box<dyn Error + Send + Sync>>;
    async fn shutdown(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
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
                "> [UDP] Binding to all network interfaces: {}:{} (Private IP: {}:{})",
                addr.ip(),
                addr.port(),
                private_ip_addr.to_string(),
                addr.port(),
            );
        } else {
            info!(
                "> [UDP] Binding to specific IP: {}:{}",
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
               
                if backoff.is_circuit_open().unwrap_or(false) {
                    let wait_time = backoff.time_until_open()
                        .unwrap_or(Duration::from_secs(2)); 
                    
                    warn!("Circuit breaker is open. Waiting for {:?} before next attempt...", wait_time);
                    
                    tokio::select! {
                        _ = time::sleep(wait_time) => {
                            if backoff.is_circuit_open().unwrap_or(false) {
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
                                if let Ok((failures, _)) = backoff.record_failure(){
                                    error!("Error receiving UDP datagram: {} Consecutive failures: {}", e, failures);
                                }
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

type TransportError = Box<dyn Error + Send + Sync>;

#[async_trait]
impl DatagramTransport for DefaultTransport {
    fn incoming(&self) -> broadcast::Receiver<Datagram> {
        self.datagram_tx.subscribe()
    }

    async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<(), TransportError> {
        self.udp_socket.send_to(&data, target).await
            .context("Failed to send UDP datagram")?;
        Ok(())
    }

    fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.udp_socket.local_addr()?)
    }

    async fn shutdown(&self) -> Result<(),TransportError> {
        self.shutdown_signal.send(())
            .map_err(|_| anyhow::anyhow!("Failed to send shutdown signal"))?;
        Ok(())
    }
}

#[pin_project]
#[derive(Debug)]
pub(crate) struct TcpConnectionListener {
    local_addr: SocketAddr,
    #[pin]
    incoming: TcpListenerStream,
}

impl TcpConnectionListener {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        info!(
            "> [TCP] Binding to specific IP: {}:{}",
            addr.ip(),
            addr.port(),
        );

        Ok(Self::new(listener, local_addr))
    }

    pub(crate) const fn new(listener: TcpListener, local_addr: SocketAddr) -> Self {
        Self {
            local_addr,
            incoming: TcpListenerStream { inner: listener },
        }
    }

    pub(crate) fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<ListenerEvent> {
        let this = self.project();
        match ready!(this.incoming.poll_next(cx)) {
            Some(Ok((stream, remote_addr))) => {
                if let Err(err) = stream.set_nodelay(true) {
                    warn!(target: "net", "set nodelay failed: {:?}", err);
                }
                Poll::Ready(ListenerEvent::Incoming { stream, remote_addr })
            }
            Some(Err(err)) => Poll::Ready(ListenerEvent::Error(err)),
            None => Poll::Ready(ListenerEvent::ListenerClosed {
                local_addr: *this.local_addr,
            }),
        }
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

pub(crate) enum ListenerEvent {
    Incoming {
        stream: TcpStream,
        remote_addr: SocketAddr,
    },
    ListenerClosed {
        local_addr: SocketAddr,
    },
    Error(io::Error),
}

#[derive(Debug)]
struct TcpListenerStream {
    inner: TcpListener,
}

impl Stream for TcpListenerStream {
    type Item = io::Result<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_accept(cx) {
            Poll::Ready(Ok(conn)) => Poll::Ready(Some(Ok(conn))),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }
}
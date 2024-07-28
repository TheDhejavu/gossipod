use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use gethostname::gethostname;
use std::time::Duration;

use anyhow::Result;
use crate::ip_addr::IpAddress;

pub(crate) const DEFAULT_IP_ADDR: &str = "127.0.0.1";
pub(crate) const DEFAULT_PORT: u16 = 5870;
pub(crate) const DEFAULT_PING_TIMEOUT: u64 = 1_000; 
pub(crate) const DEFAULT_ACK_TIMEOUT: u64 = 1_000;
pub(crate) const DEFAULT_RETRY_COUNT: u8 = 3;
pub(crate) const DEFAULT_GOSSIP_INTERVAL: u64 = 5_000; 
pub(crate) const DEFAULT_SUSPECT_TIMEOUT: u64 = 10_000; 
pub(crate) const DEFAULT_MESSAGE_BUFFER_SIZE: usize = 1_024; 
pub(crate) const DEFAULT_TRANSPORT_TIMEOUT: u64 = 5_000;
pub(crate) const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub(crate) struct GossipodConfig {
    pub(crate) name: String,
    pub(crate) port: u16,
    pub(crate) ip_addrs: Vec<IpAddr>,
    pub(crate) ping_timeout: Duration,
    pub(crate) ack_timeout: Duration,
    pub(crate) retry_count: u8,
    pub(crate) gossip_interval: Duration,
    pub(crate) suspect_timeout: Duration,
}

impl GossipodConfig {
    /// Get the IP addresses.
    pub fn ip_addrs(&self) -> &[IpAddr] {
        &self.ip_addrs
    }

    pub fn addr(&self) -> IpAddr {
        self.ip_addrs[0]
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}


// Configuration for SWIM protocol.
#[derive(Debug, Clone)]
pub(crate) struct GossipodConfigBuilder {
    /// Optional name for the node.
    pub(crate) name: Option<String>,
    
    /// Port on which the node will bind.
    pub(crate) port: u16,
    
    /// IP address the node will bind to.
    pub(crate) ip_addrs: Vec<IpAddr>,
    
    /// Timeout for ping messages.
    pub(crate) ping_timeout: Duration,
    
    /// Timeout for ack messages.
    pub(crate) ack_timeout: Duration,
    
    /// Number of retries for sending messages.
    pub(crate) retry_count: u8,
    
    /// Interval between gossip protocol messages.
    pub(crate) gossip_interval: Duration,
    
    /// Timeout to mark a node as suspect.
    pub(crate) suspect_timeout: Duration,
}


impl Default for GossipodConfigBuilder {
    fn default() -> GossipodConfigBuilder {
        let ip_addr = IpAddr::V4(Ipv4Addr::from_str(DEFAULT_IP_ADDR)
            .expect(&format!("unable to parse default ip addr: {}", DEFAULT_IP_ADDR)));

        Self {
            name: None,
            port: DEFAULT_PORT,
            ip_addrs: vec![ip_addr],
            ping_timeout: Duration::from_millis(DEFAULT_PING_TIMEOUT),
            ack_timeout: Duration::from_millis(DEFAULT_ACK_TIMEOUT),
            retry_count: DEFAULT_RETRY_COUNT,
            gossip_interval: Duration::from_millis(DEFAULT_GOSSIP_INTERVAL),
            suspect_timeout: Duration::from_millis(DEFAULT_SUSPECT_TIMEOUT),
        }
    }
}

impl GossipodConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the name for the node.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the port to bind to.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the IP address to bind to.
    pub fn addr(mut self, addr: impl Into<IpAddress>) -> Self {
        self.ip_addrs = vec![addr.into().0];
        self
    }

    pub fn ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = timeout;
        self
    }

    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    pub fn retry_count(mut self, count: u8) -> Self {
        self.retry_count = count;
        self
    }

    pub fn gossip_interval(mut self, interval: Duration) -> Self {
        self.gossip_interval = interval;
        self
    }

    pub fn suspect_timeout(mut self, timeout: Duration) -> Self {
        self.suspect_timeout = timeout;
        self
    }

    /// Validate the configuration to ensure all values are set.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.ip_addrs.is_empty() {
            anyhow::bail!("Bind address is not set");
        }
        if self.port == 0 {
            anyhow::bail!("Bind port is not set");
        }
        if self.ping_timeout.as_millis() == 0 {
            anyhow::bail!("Ping timeout is not set");
        }
        if self.ack_timeout.as_millis() == 0 {
            anyhow::bail!("Ack timeout is not set");
        }
        if self.retry_count == 0 {
            anyhow::bail!("Retry count is not set");
        }
        if self.gossip_interval.as_millis() == 0 {
            anyhow::bail!("Gossip interval is not set");
        }
        if self.suspect_timeout.as_millis() == 0 {
            anyhow::bail!("Suspect timeout is not set");
        }
        Ok(())
    }

    pub async fn build(mut self) -> Result<GossipodConfig> {
        self.fill();
        self.validate()?;

        Ok(GossipodConfig {
            name: self.name.unwrap(),
            port: self.port,
            ip_addrs: self.ip_addrs,
            ping_timeout: self.ping_timeout,
            ack_timeout: self.ack_timeout,
            retry_count: self.retry_count,
            gossip_interval: self.gossip_interval,
            suspect_timeout: self.suspect_timeout,
        })
    }

    fn fill(&mut self) {
        if self.name.is_none() {
            self.name = Some(gethostname().into_string().unwrap())
        }
    }
}


use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use gethostname::gethostname;
use std::time::Duration;

use anyhow::Result;
use crate::ip_addr::IpAddress;

pub(crate) const DEFAULT_IP_ADDR: &str = "127.0.0.1";
pub(crate) const DEFAULT_PORT: u16 = 5870;
pub(crate) const DEFAULT_PING_TIMEOUT: u64 = 10_000; 
pub(crate) const DEFAULT_ACK_TIMEOUT: u64 = 10_000;
pub(crate) const DEFAULT_GOSSIP_INTERVAL: u64 = 5_000; 
pub(crate) const DEFAULT_PROBING_INTERVAL: u64 = 10_000; 
pub(crate) const DEFAULT_SUSPECT_TIMEOUT: u64 = 10_000; 
pub(crate) const DEFAULT_MESSAGE_BUFFER_SIZE: usize = 1_024; 
pub(crate) const DEFAULT_TRANSPORT_TIMEOUT: u64 = 5_000;
pub(crate) const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 100;
pub(crate) const MAX_RETRY_DELAY: u64 = 60; // in secs
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 2;
pub(crate) const MAX_UDP_PACKET_SIZE: usize = 1400; 
pub(crate) const BROADCAST_FANOUT: usize = 2; 
pub(crate) const METADATA_OVERHEAD: usize = 50;  


#[derive(Debug, Clone)]
pub struct GossipodConfig {
    pub(crate) name: String,
    pub(crate) port: u16,
    pub(crate) ip_addrs: Vec<IpAddr>,
    pub(crate) ping_timeout: Duration,
    pub(crate) ack_timeout: Duration,
    pub(crate) gossip_interval: Duration,
    pub(crate) probing_interval: Duration,
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
pub struct GossipodConfigBuilder {
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
 
    /// Interval between gossip protocol messages.
    pub(crate) gossip_interval: Duration,

    /// Interval between probing protocol messages.
    pub(crate) probing_interval: Duration,
    
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
            gossip_interval: Duration::from_millis(DEFAULT_GOSSIP_INTERVAL),
            probing_interval: Duration::from_millis(DEFAULT_PROBING_INTERVAL),
            suspect_timeout: Duration::from_millis(DEFAULT_SUSPECT_TIMEOUT),
        }
    }
}

impl GossipodConfigBuilder {
    /// Creates a new instance of `GossipodConfigBuilder` with default values.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Sets the name for the node.
    ///
    /// # Arguments
    ///
    /// * `name` - A string-like value that can be converted into a `String`.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the port to bind to.
    ///
    /// # Arguments
    ///
    /// * `port` - A `u16` value representing the port number.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the IP address to bind to.
    ///
    /// # Arguments
    ///
    /// * `addr` - An `IpAddress` or a type that can be converted into an `IpAddress`.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn addr(mut self, addr: impl Into<IpAddress>) -> Self {
        self.ip_addrs = vec![addr.into().0];
        self
    }

    /// Sets the ping timeout duration.
    ///
    /// # Arguments
    ///
    /// * `timeout` - A `Duration` representing the ping timeout.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = timeout;
        self
    }

    /// Sets the acknowledgment timeout duration.
    ///
    /// # Arguments
    ///
    /// * `timeout` - A `Duration` representing the acknowledgment timeout.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Sets the gossip interval duration.
    ///
    /// # Arguments
    ///
    /// * `interval` - A `Duration` representing the interval between gossip protocol messages.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn gossip_interval(mut self, interval: Duration) -> Self {
        self.gossip_interval = interval;
        self
    }


    /// Sets the probing interval duration.
    ///
    /// # Arguments
    ///
    /// * `interval` - A `Duration` representing the interval between probing protocol messages.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn probing_interval(mut self, interval: Duration) -> Self {
        self.probing_interval = interval;
        self
    }

    /// Sets the suspect timeout duration.
    ///
    /// # Arguments
    ///
    /// * `timeout` - A `Duration` representing the timeout to mark a node as suspect.
    ///
    /// # Returns
    ///
    /// Returns `self` to allow for method chaining.
    pub fn suspect_timeout(mut self, timeout: Duration) -> Self {
        self.suspect_timeout = timeout;
        self
    }
    /// Validate the configuration to ensure all values are set.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.ip_addrs.is_empty() {
            anyhow::bail!("bind address is not set");
        }
        if self.port == 0 {
            anyhow::bail!("bind port is not set");
        }
        if self.ping_timeout.as_millis() == 0 {
            anyhow::bail!("ping timeout is not set");
        }
        if self.probing_interval.as_millis() == 0 {
            anyhow::bail!("probing timeout is not set");
        }
        if self.ack_timeout.as_millis() == 0 {
            anyhow::bail!("ack timeout is not set");
        }
        if self.gossip_interval.as_millis() == 0 {
            anyhow::bail!("gossip interval is not set");
        }
        if self.suspect_timeout.as_millis() == 0 {
            anyhow::bail!("suspect timeout is not set");
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
            gossip_interval: self.gossip_interval,
            suspect_timeout: self.suspect_timeout,
            probing_interval: self.probing_interval,
        })
    }

    fn fill(&mut self) {
        if self.name.is_none() {
            self.name = Some(gethostname().into_string().unwrap())
        }
    }
}


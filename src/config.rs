use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use gethostname::gethostname;
use std::time::Duration;

use anyhow::Result;
use crate::ip_addr::IpAddress;

// Default configuration constants
pub(crate) const DEFAULT_IP_ADDR: &str = "127.0.0.1";
pub(crate) const DEFAULT_PORT: u16 = 5870;
pub(crate) const DEFAULT_BASE_PROBING_INTERVAL: u64 = 1_000; // 1 second base interval
pub(crate) const DEFAULT_ACK_TIMEOUT: u64 = 500; // 500 milliseconds
pub(crate) const DEFAULT_INDIRECT_ACK_TIMEOUT: u64 = 1_000; // 1 second
pub(crate) const DEFAULT_BASE_SUSPICIOUS_TIMEOUT: u64 = 5_000; // 5 seconds
pub(crate) const DEFAULT_MESSAGE_BUFFER_SIZE: usize = 1_024; 
pub(crate) const DEFAULT_TRANSPORT_TIMEOUT: u64 = 5_000;
pub(crate) const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 100;
pub(crate) const MAX_RETRY_DELAY: u64 = 60; // in secs
pub(crate) const MAX_UDP_PACKET_SIZE: usize = 1400; 
pub(crate) const BROADCAST_FANOUT: usize = 2; 
pub(crate) const INDIRECT_REQ: usize = 2;

/// Represents the type of network environment the gossip protocol is operating in.
/// This affects various timing and timeout calculations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NetworkType {
    /// Local network (e.g., localhost or same machine)
    /// Characterized by very low latency and high reliability
    Local,
    /// Local Area Network (LAN)
    /// Typically has low latency and high reliability
    LAN,
    /// Wide Area Network (WAN)
    /// Can have higher latency and lower reliability compared to LAN
    WAN,
}

impl Default for NetworkType {
    fn default() -> Self {
        NetworkType::Local
    }
}

/// Main configuration structure for the Gossipod protocol
#[derive(Debug, Clone)]
pub struct GossipodConfig {
    /// Name of the node, used for identification in the cluster
    pub(crate) name: String,

    /// Port number on which the node will listen for gossip messages
    pub(crate) port: u16,

    /// List of IP addresses the node will bind to
    pub(crate) ip_addrs: Vec<IpAddr>,

    /// Base interval for probing other nodes in the cluster
    /// This value is adjusted based on cluster size and network type
    pub(crate) base_probing_interval: Duration,

    /// Timeout for receiving an ACK after sending a direct probe
    /// This is a fixed value, not affected by cluster size or network type
    pub(crate) ack_timeout: Duration,

    /// Timeout for receiving an ACK after sending an indirect probe
    /// This is a fixed value, not affected by cluster size or network type
    pub(crate) indirect_ack_timeout: Duration,

    /// Base timeout for considering a node suspicious
    /// This value is adjusted based on cluster size
    pub(crate) base_suspicious_timeout: Duration,

    /// Type of network the node is operating in (Local, LAN, or WAN)
    /// This affects various timing calculations
    pub(crate) network_type: NetworkType,
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

    /// Calculates the probing interval based on cluster size and network type
    ///
    /// The probing interval increases logarithmically with cluster size to reduce
    /// network load in larger clusters. It's further adjusted based on the network type:
    /// - Local: No additional adjustment
    /// - LAN: 1.5x increase to account for slightly higher latency
    /// - WAN: 3x increase to account for significantly higher latency
    pub(crate) fn probing_interval(&self, cluster_size: usize) -> Duration {
        let base_ms = self.base_probing_interval.as_millis() as f64;
        let log_factor = (cluster_size as f64).ln().max(1.0);
        let interval_ms = base_ms * log_factor;
        let network_factor = match self.network_type {
            NetworkType::Local => 1.0,
            NetworkType::LAN => 1.5,
            NetworkType::WAN => 3.0,
        };
        Duration::from_millis((interval_ms * network_factor) as u64)
    }

    /// Returns the fixed ACK timeout
    ///
    /// This timeout is used when waiting for an ACK after sending a direct probe.
    /// It's a fixed value and not affected by cluster size or network type.
    pub fn ack_timeout(&self) -> Duration {
        self.ack_timeout
    }

    /// Returns the fixed indirect ACK timeout
    ///
    /// This timeout is used when waiting for an ACK after sending an indirect probe.
    /// It's a fixed value and not affected by cluster size or network type.
    pub fn indirect_ack_timeout(&self) -> Duration {
        self.indirect_ack_timeout
    }

    /// Calculates the suspicious timeout based on cluster size
    ///
    /// The suspicious timeout increases logarithmically with cluster size to reduce
    /// false positives in larger clusters. It's not directly affected by network type,
    /// but the base value can be set differently for different network types if needed.
    pub(crate) fn suspicious_timeout(&self, cluster_size: usize) -> Duration {
        let base_ms = self.base_suspicious_timeout.as_millis() as f64;
        let log_factor = (cluster_size as f64).ln().max(1.0);
        Duration::from_millis((base_ms * log_factor) as u64)
    }

}

#[derive(Debug, Clone)]
pub struct GossipodConfigBuilder {
    pub(crate) name: Option<String>,
    pub(crate) port: u16,
    pub(crate) ip_addrs: Vec<IpAddr>,
    pub(crate) base_probing_interval: Duration,
    pub(crate) ack_timeout: Duration,
    pub(crate) indirect_ack_timeout: Duration,
    pub(crate) base_suspicious_timeout: Duration,
    pub(crate) network_type: NetworkType,
}

impl Default for GossipodConfigBuilder {
    fn default() -> GossipodConfigBuilder {
        let ip_addr = IpAddr::V4(Ipv4Addr::from_str(DEFAULT_IP_ADDR)
            .expect(&format!("unable to parse default ip addr: {}", DEFAULT_IP_ADDR)));

        Self {
            name: None,
            port: DEFAULT_PORT,
            ip_addrs: vec![ip_addr],
            base_probing_interval: Duration::from_millis(DEFAULT_BASE_PROBING_INTERVAL),
            ack_timeout: Duration::from_millis(DEFAULT_ACK_TIMEOUT),
            indirect_ack_timeout: Duration::from_millis(DEFAULT_INDIRECT_ACK_TIMEOUT),
            base_suspicious_timeout: Duration::from_millis(DEFAULT_BASE_SUSPICIOUS_TIMEOUT),
            network_type: NetworkType::default(),
        }
    }
}

impl GossipodConfigBuilder {
    /// Creates a new GossipodConfigBuilder with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the name of the node
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the port number for the node
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the IP address for the node
    pub fn addr(mut self, addr: impl Into<IpAddress>) -> Self {
        self.ip_addrs = vec![addr.into().0];
        self
    }

    /// Sets the base probing interval
    ///
    /// This is the starting point for calculating the actual probing interval,
    /// which will be adjusted based on cluster size and network type.
    pub fn probing_interval(mut self, interval: Duration) -> Self {
        self.base_probing_interval = interval;
        self
    }

    /// Sets the ACK timeout
    ///
    /// This is a fixed timeout used when waiting for an ACK after sending a direct probe.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Sets the indirect ACK timeout
    ///
    /// This is a fixed timeout used when waiting for an ACK after sending an indirect probe.
    pub fn indirect_ack_timeout(mut self, timeout: Duration) -> Self {
        self.indirect_ack_timeout = timeout;
        self
    }

    /// Sets the base suspicious timeout
    ///
    /// This is the starting point for calculating the actual suspicious timeout,
    /// which will be adjusted based on cluster size.
    pub fn suspicious_timeout(mut self, timeout: Duration) -> Self {
        self.base_suspicious_timeout = timeout;
        self
    }


    /// Sets the network type (Local, LAN, or WAN)
    ///
    /// This affects various timing calculations, particularly the probing interval.
    pub fn network_type(mut self, network_type: NetworkType) -> Self {
        self.network_type = network_type;
        self
    }

    /// Validates the current configuration
    ///
    /// Checks that all necessary fields are set and have non-zero values where appropriate.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.ip_addrs.is_empty() {
            anyhow::bail!("bind address is not set");
        }
        if self.port == 0 {
            anyhow::bail!("bind port is not set");
        }
        if self.base_probing_interval.as_millis() == 0 {
            anyhow::bail!("base probing interval is not set");
        }
        if self.ack_timeout.as_millis() == 0 {
            anyhow::bail!("ACK timeout is not set");
        }
        if self.indirect_ack_timeout.as_millis() == 0 {
            anyhow::bail!("indirect ACK timeout is not set");
        }
        if self.base_suspicious_timeout.as_millis() == 0 {
            anyhow::bail!("base suspicious timeout is not set");
        }
       
        Ok(())
    }

    /// Builds the final GossipodConfig
    ///
    /// This method validates the configuration and creates a GossipodConfig instance.
    /// If the name is not set, it uses the hostname of the machine.
    pub async fn build(mut self) -> Result<GossipodConfig> {
        self.fill();
        self.validate()?;

        Ok(GossipodConfig {
            name: self.name.unwrap(),
            port: self.port,
            ip_addrs: self.ip_addrs,
            base_probing_interval: self.base_probing_interval,
            ack_timeout: self.ack_timeout,
            indirect_ack_timeout: self.indirect_ack_timeout,
            base_suspicious_timeout: self.base_suspicious_timeout,
            network_type: self.network_type,
        })
    }

    /// Fills in any missing fields with default values
    fn fill(&mut self) {
        if self.name.is_none() {
            self.name = Some(gethostname().into_string().unwrap())
        }
    }
}
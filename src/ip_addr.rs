use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use anyhow::{anyhow, Result};
use sysinfo::Networks;

/// A wrapper around `std::net::IpAddr` providing additional functionality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpAddress(pub(crate) IpAddr);

impl From<IpAddr> for IpAddress {
    fn from(ip: IpAddr) -> Self {
        Self(ip)
    }
}

impl From<Ipv4Addr> for IpAddress {
    fn from(ip: Ipv4Addr) -> Self {
        Self(IpAddr::V4(ip))
    }
}

impl From<Ipv6Addr> for IpAddress {
    fn from(ip: Ipv6Addr) -> Self {
        Self(IpAddr::V6(ip))
    }
}

impl FromStr for IpAddress {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.parse().map(IpAddress)
    }
}

impl IpAddress {
    /// Finds the system's non-loopback IPv4 address.
    ///
    /// Returns the first non-loopback IPv4 address found, or an error if none is available.
    pub fn find_system_ip() -> Result<IpAddr> {
        let networks = Networks::new_with_refreshed_list();
        for (_, data) in &networks {
            for ip in data.ip_networks() {
                println!("DR {}", ip.addr);
                if let IpAddr::V4(ipv4) = ip.addr {
                    if !ipv4.is_loopback() {
                        return Ok(IpAddr::V4(ipv4));
                    }
                }
            }
        }

        Err(anyhow!("No suitable IPv4 address found"))
    }
}
impl AsRef<IpAddr> for IpAddress {
    fn as_ref(&self) -> &IpAddr {
        &self.0
    }
}

impl std::fmt::Display for IpAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_ipaddr() {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip_address = IpAddress::from(ip);
        assert_eq!(ip_address.0, ip);
    }

    #[test]
    fn test_from_ipv4addr() {
        let ip = Ipv4Addr::new(192, 168, 0, 1);
        let ip_address = IpAddress::from(ip);
        assert_eq!(ip_address.0, IpAddr::V4(ip));
    }

    #[test]
    fn test_from_str() {
        let ip_str = "192.168.0.1";
        let ip_address: IpAddress = ip_str.parse().unwrap();
        assert_eq!(ip_address.0, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)));
    }

    #[test]
    fn test_display() {
        let ip = IpAddress(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)));
        assert_eq!(ip.to_string(), "192.168.0.1");
    }
}
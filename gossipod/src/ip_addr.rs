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
   /// Finds the system's non-loopback IPv4 & IPv6 addresses.
    ///
    /// Returns a struct containing the first non-loopback IPv4 and IPv6 addresses found,
    /// or an error if neither is available.
    pub fn find_system_ip() -> Result<(IpAddr, IpAddr)> {
        let networks = Networks::new_with_refreshed_list();
        let mut ipv4: Option<IpAddr> = None;
        let mut ipv6: Option<IpAddr> = None;

        for (_, data) in &networks {
            for ip in data.ip_networks() {
                if let IpAddr::V4(ipv4_addr) = ip.addr {
                    if !ipv4_addr.is_loopback() {
                        ipv4 = Some(IpAddr::V4(ipv4_addr));
                    }
                }

                if let IpAddr::V6(ipv6_addr) = ip.addr {
                    if !ipv6_addr.is_loopback() {
                        ipv6 = Some(IpAddr::V6(ipv6_addr));
                    }
                }
            }
        }
        if ipv4.is_none() && ipv6.is_none() {
            return Err(anyhow!("No suitable IPv4 or IPV6 address found"))
        }

        return Ok((ipv4.unwrap(), ipv6.unwrap()));
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
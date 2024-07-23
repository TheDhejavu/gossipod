use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use anyhow::{anyhow, Result};
use if_addrs::get_if_addrs;

pub struct IpAddress(pub(crate) IpAddr);

impl From<IpAddr> for IpAddress {
    fn from(ip: IpAddr) -> Self {
        IpAddress(ip)
    }
}

impl From<Ipv4Addr> for IpAddress {
    fn from(ip: Ipv4Addr) -> Self {
        IpAddress(std::net::IpAddr::V4(ip))
    }
}

impl From<&str> for IpAddress {
    fn from(s: &str) -> Self {
        IpAddress(IpAddr::from_str(s).unwrap_or_else(|_| IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)))
    }
}

impl IpAddress {
    /// find_system_ip finds system IP Address
    pub(crate) fn find_system_ip() -> Result<IpAddr> {
        let interfaces = get_if_addrs()?;
        for iface in interfaces {
            if !iface.is_loopback() && iface.addr.ip().is_ipv4() {
                return Ok(iface.addr.ip());
            }
        }
        Err(anyhow!("No suitable IP address found"))
    }
}

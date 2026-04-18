use std::collections::BTreeSet;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::{Mutex, OnceLock};

use super::*;
use url::Url;

#[derive(Debug, Clone, Default)]
struct NetGlobals {
    minio_host: String,
    minio_port: String,
    is_tls: bool,
    endpoint: String,
}

fn net_globals() -> &'static Mutex<NetGlobals> {
    static NET_GLOBALS: OnceLock<Mutex<NetGlobals>> = OnceLock::new();
    NET_GLOBALS.get_or_init(|| {
        Mutex::new(NetGlobals {
            minio_port: "9000".to_string(),
            ..NetGlobals::default()
        })
    })
}

pub fn set_api_endpoint_globals(host: &str, port: &str, is_tls: bool, endpoint: &str) {
    let mut globals = net_globals().lock().expect("net globals lock");
    globals.minio_host = host.to_string();
    globals.minio_port = port.to_string();
    globals.is_tls = is_tls;
    globals.endpoint = endpoint.to_string();
}

pub fn must_split_host_port(host_port: &str) -> (String, String) {
    extract_host_port(host_port).expect("must split host port")
}

pub fn must_get_local_ip4() -> BTreeSet<String> {
    let mut ips = BTreeSet::new();
    ips.insert("127.0.0.1".to_string());
    if let Ok(addrs) = local_interface_ips() {
        for ip in addrs {
            if ip.is_ipv4() {
                ips.insert(ip.to_string());
            }
        }
    }
    ips
}

fn local_interface_ips() -> Result<Vec<IpAddr>, String> {
    let localhost = ("localhost", 0)
        .to_socket_addrs()
        .map_err(|err| err.to_string())?;
    Ok(localhost.map(|addr| addr.ip()).collect())
}

pub fn get_host_ip(host: &str) -> Result<BTreeSet<String>, String> {
    let addrs = (host, 0).to_socket_addrs().map_err(|err| err.to_string())?;
    let mut ips = BTreeSet::new();
    for addr in addrs {
        ips.insert(addr.ip().to_string());
    }
    Ok(ips)
}

pub fn sort_ips(ip_list: &[String]) -> Vec<String> {
    if ip_list.len() <= 1 {
        return ip_list.to_vec();
    }

    let mut ipv4s = Vec::<(usize, std::net::Ipv4Addr)>::new();
    let mut non_ips = Vec::<String>::new();
    for (index, ip) in ip_list.iter().enumerate() {
        match ip.parse::<std::net::Ipv4Addr>() {
            Ok(parsed) => ipv4s.push((index, parsed)),
            Err(_) => non_ips.push(ip.clone()),
        }
    }

    ipv4s.sort_by(|left, right| {
        if left.1.is_loopback() && !right.1.is_loopback() {
            return std::cmp::Ordering::Greater;
        }
        if !left.1.is_loopback() && right.1.is_loopback() {
            return std::cmp::Ordering::Less;
        }
        right.1.octets()[3]
            .cmp(&left.1.octets()[3])
            .then(left.0.cmp(&right.0))
    });

    let mut out = non_ips;
    out.extend(ipv4s.into_iter().map(|(_, ip)| ip.to_string()));
    out
}

pub fn get_api_endpoints() -> Vec<String> {
    let globals = net_globals().lock().expect("net globals lock").clone();
    if !globals.endpoint.is_empty() {
        return vec![globals.endpoint];
    }

    let scheme = if globals.is_tls { "https" } else { "http" };
    let hosts = if globals.minio_host.is_empty() {
        sort_ips(&must_get_local_ip4().into_iter().collect::<Vec<_>>())
    } else {
        vec![globals.minio_host]
    };

    hosts
        .into_iter()
        .map(|host| format!("{scheme}://{}", join_host_port(&host, &globals.minio_port)))
        .collect()
}

pub fn is_host_ip(ip_address: &str) -> bool {
    if ip_address.contains("://") {
        return false;
    }
    let host = if let Ok((host, _, _)) = extract_host_port_with_scheme(ip_address) {
        host
    } else if let Ok((host, port)) = split_host_port_raw(ip_address) {
        if port.is_empty() {
            ip_address.to_string()
        } else {
            host
        }
    } else {
        ip_address.to_string()
    };
    let stripped = host.split('%').next().unwrap_or(&host);
    stripped.parse::<IpAddr>().is_ok()
}

fn normalize_input_addr(host_addr: &str) -> String {
    if host_addr.starts_with("http://") || host_addr.starts_with("https://") {
        host_addr.to_string()
    } else {
        host_addr.to_string()
    }
}

fn extract_host_port_with_scheme(host_addr: &str) -> Result<(String, String, String), String> {
    if host_addr.is_empty() {
        return Err("unable to process empty address".to_string());
    }
    if !host_addr.starts_with("http://") && !host_addr.starts_with("https://") {
        let (host, port) = split_host_port_or_default(host_addr, "")?;
        return Ok((host, port, String::new()));
    }

    if host_addr.starts_with("http://:") || host_addr.starts_with("https://:") {
        let scheme = if host_addr.starts_with("https://") {
            "https"
        } else {
            "http"
        };
        let remainder = host_addr
            .strip_prefix(&format!("{scheme}://"))
            .unwrap_or(host_addr);
        let addr = remainder.trim_end_matches('/');
        let (host, port) = split_host_port_or_default(addr, scheme)?;
        return Ok((host, port, scheme.to_string()));
    }

    let normalized = normalize_input_addr(host_addr);
    let parsed = Url::parse(&normalized).map_err(|err| err.to_string())?;
    let scheme = parsed.scheme().to_string();
    let host = parsed.host_str().unwrap_or_default().to_string();
    let port = parsed
        .port()
        .map(|value| value.to_string())
        .unwrap_or_else(|| match scheme.as_str() {
            "https" => "443".to_string(),
            _ => "80".to_string(),
        });
    Ok((host, port, scheme))
}

pub fn extract_host_port(host_addr: &str) -> Result<(String, String), String> {
    let (host, port, _) = extract_host_port_with_scheme(host_addr)?;
    Ok((host, port))
}

fn split_host_port_raw(addr: &str) -> Result<(String, String), String> {
    if let Some(stripped) = addr.strip_prefix('[') {
        let end = stripped
            .find(']')
            .ok_or_else(|| "invalid IPv6 address".to_string())?;
        let host = stripped[..end].to_string();
        let rest = &stripped[end + 1..];
        if let Some(port) = rest.strip_prefix(':') {
            return Ok((host, port.to_string()));
        }
        return Ok((host, String::new()));
    }
    match addr.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && !host.contains(':') => {
            Ok((host.to_string(), port.to_string()))
        }
        _ => Ok((addr.to_string(), String::new())),
    }
}

fn split_host_port_or_default(addr: &str, scheme: &str) -> Result<(String, String), String> {
    let (host, port) = split_host_port_raw(addr)?;
    if !port.is_empty() {
        if port == "http" {
            return Ok((host, "80".to_string()));
        }
        if port == "https" {
            return Ok((host, "443".to_string()));
        }
        return Ok((host, port));
    }

    let default_port = match scheme {
        "https" => "443",
        "" | "http" => "80",
        _ => return Err("unable to guess port from scheme".to_string()),
    };
    Ok((host, default_port.to_string()))
}

fn is_local_host(host: &str, port: &str, local_port: &str) -> Result<bool, String> {
    let host_ips = get_host_ip(host)?;
    let local_ipv4 = must_get_local_ip4();
    let local_match = host_ips
        .iter()
        .any(|ip| local_ipv4.contains(ip) || ip == "::1" || ip == "127.0.0.1");
    if port.is_empty() {
        Ok(local_match)
    } else {
        Ok(local_match && port == local_port)
    }
}

pub fn same_local_addrs(addr1: &str, addr2: &str) -> Result<bool, String> {
    let (host1, port1) = extract_host_port(addr1)?;
    let (host2, port2) = extract_host_port(addr2)?;

    let addr1_local = if host1.is_empty() {
        true
    } else {
        is_local_host(&host1, &port1, &port1)?
    };
    let addr2_local = if host2.is_empty() {
        true
    } else {
        is_local_host(&host2, &port2, &port2)?
    };

    Ok(addr1_local && addr2_local && port1 == port2)
}

pub fn check_local_server_addr(server_addr: &str) -> Result<(), String> {
    if server_addr.is_empty() {
        return Err(ERR_INVALID_ARGUMENT.to_string());
    }

    let (host, port) = if server_addr.contains("://") {
        extract_host_port(server_addr)?
    } else {
        must_split_host_port(server_addr)
    };

    let parsed_port: u16 = port
        .parse()
        .map_err(|_| "port must be between 0 to 65535".to_string())?;

    if parsed_port.to_string() != port {
        return Err("port must be between 0 to 65535".to_string());
    }

    if !host.is_empty() && host != "0.0.0.0" && host != "::" {
        let local_host = is_local_host(&host, &port, &port)?;
        if !local_host {
            return Err("host in server address should be this server".to_string());
        }
    }

    Ok(())
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

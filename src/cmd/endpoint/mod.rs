use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;
use std::path::PathBuf;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointType {
    Path,
    Url,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub url: Url,
    pub is_local: bool,
    pub pool_idx: i32,
    pub set_idx: i32,
    pub disk_idx: i32,
}

impl Endpoint {
    pub fn endpoint_type(&self) -> EndpointType {
        if self.url.host_str().is_some() {
            EndpointType::Url
        } else {
            EndpointType::Path
        }
    }

    pub fn update_is_local(&mut self) -> Result<(), String> {
        if let Some(host) = self.url.host_str() {
            self.is_local = is_local_host(host);
        } else {
            self.is_local = true;
        }
        Ok(())
    }

    pub fn hostname(&self) -> String {
        self.url.host_str().unwrap_or_default().to_string()
    }

    pub fn port(&self) -> String {
        self.url.port().map(|v| v.to_string()).unwrap_or_default()
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.url.host_str().is_some() {
            f.write_str(self.url.as_str())
        } else {
            f.write_str(self.url.path())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Endpoints(pub Vec<Endpoint>);

impl Endpoints {
    pub fn update_is_local(&mut self) -> Result<(), String> {
        for endpoint in &mut self.0 {
            endpoint.update_is_local()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    pub prefix: String,
    pub suffix: String,
    pub seq: Vec<String>,
}

pub type ArgPattern = Vec<Pattern>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndpointSet {
    pub arg_patterns: Vec<ArgPattern>,
    pub endpoints: Vec<String>,
    pub set_indexes: Vec<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PoolDisksLayout {
    pub cmdline: String,
    pub layout: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisksLayout {
    pub legacy: bool,
    pub pools: Vec<PoolDisksLayout>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolEndpoints {
    pub legacy: bool,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoints: Vec<String>,
    pub resolved_endpoints: Vec<Endpoint>,
    pub cmd_line: String,
}

fn is_empty_path(value: &str) -> bool {
    value.is_empty() || value == "/" || value == "\\"
}

fn is_local_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

pub fn new_endpoint(arg: &str) -> Result<Endpoint, String> {
    if is_empty_path(arg) {
        return Err("empty or root endpoint is not supported".to_string());
    }

    if let Ok(mut url) = Url::parse(arg) {
        if url.host_str().is_some() {
            if !matches!(url.scheme(), "http" | "https") {
                return Err("invalid URL endpoint format".to_string());
            }
            if url.query().is_some() || url.fragment().is_some() {
                return Err("invalid URL endpoint format".to_string());
            }
            let host = url.host_str().unwrap_or_default();
            if host.is_empty() {
                return Err("invalid URL endpoint format: empty host name".to_string());
            }
            if url.port().is_some_and(|port| port == 0) {
                return Err("invalid URL endpoint format: invalid port number".to_string());
            }
            let cleaned = {
                let path = url.path().trim_end_matches('/');
                if path.is_empty() {
                    return Err("empty or root path is not supported in URL endpoint".to_string());
                }
                path.to_string()
            };
            url.set_path(&cleaned);
            return Ok(Endpoint {
                url,
                is_local: false,
                pool_idx: -1,
                set_idx: -1,
                disk_idx: -1,
            });
        }
    }

    if arg.contains(':')
        && !arg.starts_with('/')
        && !arg.starts_with("./")
        && !arg.starts_with("../")
    {
        return Err("invalid URL endpoint format: missing scheme http or https".to_string());
    }

    let abs = std::fs::canonicalize(arg)
        .unwrap_or_else(|_| {
            let path = PathBuf::from(arg);
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("/"))
                    .join(path)
            }
        })
        .to_string_lossy()
        .to_string();
    let url = Url::from_file_path(&abs).map_err(|_| "absolute path failed".to_string())?;
    Ok(Endpoint {
        url,
        is_local: true,
        pool_idx: -1,
        set_idx: -1,
        disk_idx: -1,
    })
}

pub fn new_endpoints(args: &[&str]) -> Result<Endpoints, String> {
    let mut endpoints = Vec::new();
    let mut seen = BTreeSet::new();
    let mut endpoint_type = None;
    let mut scheme = None::<String>;

    for arg in args {
        let endpoint = new_endpoint(arg).map_err(|err| format!("'{arg}': {err}"))?;
        match endpoint_type {
            None => {
                endpoint_type = Some(endpoint.endpoint_type());
                if endpoint.endpoint_type() == EndpointType::Url {
                    scheme = Some(endpoint.url.scheme().to_string());
                }
            }
            Some(kind) if kind != endpoint.endpoint_type() => {
                return Err("mixed style endpoints are not supported".to_string())
            }
            _ => {}
        }
        if endpoint.endpoint_type() == EndpointType::Url
            && scheme.as_deref() != Some(endpoint.url.scheme())
        {
            return Err("mixed scheme is not supported".to_string());
        }
        let key = endpoint.to_string();
        if !seen.insert(key) {
            return Err("duplicate endpoints found".to_string());
        }
        endpoints.push(endpoint);
    }

    Ok(Endpoints(endpoints))
}

pub fn update_domain_ips(minio_port: &str, endpoints: &BTreeSet<String>) -> BTreeSet<String> {
    let mut ip_list = BTreeSet::new();
    for endpoint in endpoints {
        let (host, port) = match endpoint.rsplit_once(':') {
            Some((host, port)) if port.parse::<u16>().is_ok() => {
                (host.to_string(), port.to_string())
            }
            _ => (endpoint.to_string(), minio_port.to_string()),
        };

        let is_loopback = host == "localhost"
            || host
                .parse::<IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false);

        if !is_loopback {
            ip_list.insert(format!("{host}:{port}"));
        }
    }
    ip_list
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupType {
    ErasureSD,
    Erasure,
    DistErasure,
}

const SET_SIZES: [u64; 15] = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

pub fn get_divisible_size(total_sizes: &[u64]) -> u64 {
    fn gcd(mut left: u64, mut right: u64) -> u64 {
        while right != 0 {
            (left, right) = (right, left % right);
        }
        left
    }

    let mut result = total_sizes[0];
    for &size in &total_sizes[1..] {
        result = gcd(result, size);
    }
    result
}

fn is_valid_set_size(count: u64) -> bool {
    (SET_SIZES[0]..=SET_SIZES[SET_SIZES.len() - 1]).contains(&count)
}

fn common_set_drive_count(divisible_size: u64, set_counts: &[u64]) -> u64 {
    if divisible_size < set_counts[set_counts.len() - 1] {
        return divisible_size;
    }

    let mut set_size = 0;
    let mut previous = divisible_size / set_counts[0];
    for &count in set_counts {
        if divisible_size % count == 0 {
            let ratio = divisible_size / count;
            if ratio <= previous {
                previous = ratio;
                set_size = count;
            }
        }
    }
    set_size
}

fn possible_set_counts_with_symmetry(set_counts: &[u64], arg_patterns: &[ArgPattern]) -> Vec<u64> {
    if arg_patterns.is_empty() {
        return set_counts.to_vec();
    }

    let mut new_counts = BTreeSet::new();
    for &count in set_counts {
        let mut symmetry = false;
        for arg_pattern in arg_patterns {
            for pattern in arg_pattern {
                let seq_len = pattern.seq.len() as u64;
                symmetry = if seq_len > count {
                    seq_len % count == 0
                } else {
                    count % seq_len == 0
                };
            }
        }
        if symmetry {
            new_counts.insert(count);
        }
    }

    new_counts.into_iter().collect()
}

pub fn get_set_indexes(
    args: &[String],
    total_sizes: &[u64],
    set_drive_count: u64,
    arg_patterns: &[ArgPattern],
) -> Result<Vec<Vec<u64>>, String> {
    if total_sizes.is_empty() || args.is_empty() {
        return Err("invalid argument".to_string());
    }

    let mut set_indexes = vec![Vec::new(); total_sizes.len()];
    for &total_size in total_sizes {
        if total_size < SET_SIZES[0] || total_size < set_drive_count {
            return Err(format!("incorrect number of endpoints provided: {args:?}"));
        }
    }

    let common_size = get_divisible_size(total_sizes);
    let mut set_counts = SET_SIZES
        .iter()
        .copied()
        .filter(|size| common_size % size == 0)
        .collect::<Vec<_>>();
    if set_counts.is_empty() {
        return Err(format!(
            "number of drives {common_size} is not divisible by any supported erasure set size"
        ));
    }

    let set_size = if set_drive_count > 0 {
        if !set_counts.contains(&set_drive_count) {
            return Err("invalid erasure set size".to_string());
        }
        set_drive_count
    } else {
        set_counts = possible_set_counts_with_symmetry(&set_counts, arg_patterns);
        if set_counts.is_empty() {
            return Err("no symmetric distribution detected".to_string());
        }
        common_set_drive_count(common_size, &set_counts)
    };

    if !is_valid_set_size(set_size) {
        return Err("invalid erasure set size".to_string());
    }

    for (index, &total_size) in total_sizes.iter().enumerate() {
        let count = total_size / set_size;
        set_indexes[index] = vec![set_size; count as usize];
    }

    Ok(set_indexes)
}

fn parse_numeric_sequence(range: &str) -> Result<Vec<String>, String> {
    let (start, end) = range
        .split_once("...")
        .ok_or_else(|| "missing ellipses range".to_string())?;
    if start.is_empty() || end.is_empty() {
        return Err("invalid ellipses range".to_string());
    }
    if start.starts_with('-') || end.starts_with('-') {
        return Err("negative ranges are not supported".to_string());
    }

    let is_hex = start
        .chars()
        .chain(end.chars())
        .any(|ch| ch.is_ascii_hexdigit() && ch.is_ascii_alphabetic());
    let base = if is_hex { 16 } else { 10 };
    if !start.chars().all(|ch| ch.is_digit(base)) || !end.chars().all(|ch| ch.is_digit(base)) {
        return Err("unsupported characters in ellipses range".to_string());
    }

    let start_num = u64::from_str_radix(start, base).map_err(|err| err.to_string())?;
    let end_num = u64::from_str_radix(end, base).map_err(|err| err.to_string())?;
    if start_num > end_num {
        return Err("range start cannot be greater than range end".to_string());
    }

    let width =
        if (start.starts_with('0') && start.len() > 1) || (end.starts_with('0') && end.len() > 1) {
            start.len().max(end.len())
        } else {
            0
        };
    let mut seq = Vec::new();
    for value in start_num..=end_num {
        if is_hex {
            if width > 0 {
                seq.push(format!("{value:0width$x}"));
            } else {
                seq.push(format!("{value:x}"));
            }
        } else if width > 0 {
            seq.push(format!("{value:0width$}"));
        } else {
            seq.push(value.to_string());
        }
    }
    Ok(seq)
}

fn parse_arg_pattern(arg: &str) -> Result<ArgPattern, String> {
    let bytes = arg.as_bytes();
    let mut parts = Vec::new();
    let mut last_end = 0usize;
    let mut cursor = 0usize;

    while cursor < bytes.len() {
        if bytes[cursor] == b'{' {
            let start = cursor;
            let end = arg[start + 1..]
                .find('}')
                .map(|index| start + 1 + index)
                .ok_or_else(|| "unclosed ellipses expression".to_string())?;
            let literal = arg[last_end..start].to_string();
            let seq = parse_numeric_sequence(&arg[start + 1..end])?;
            parts.push((literal, seq));
            last_end = end + 1;
            cursor = end + 1;
        } else {
            cursor += 1;
        }
    }

    if parts.is_empty() {
        return Err("no ellipses patterns found".to_string());
    }

    let trailing = arg[last_end..].to_string();
    let mut patterns = Vec::with_capacity(parts.len());
    for (index, (prefix, seq)) in parts.into_iter().enumerate() {
        let prefix = if index == 0 { prefix } else { String::new() };
        let suffix = if index + 1 == patterns.capacity() {
            trailing.clone()
        } else {
            // Temporary placeholder, updated below.
            String::new()
        };
        patterns.push(Pattern {
            prefix,
            suffix,
            seq,
        });
    }

    // The suffix for each non-last range is the literal that preceded the next range.
    let mut literals = Vec::new();
    let mut last_end = 0usize;
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] == b'{' {
            let start = cursor;
            let end = arg[start + 1..]
                .find('}')
                .map(|index| start + 1 + index)
                .ok_or_else(|| "unclosed ellipses expression".to_string())?;
            literals.push(arg[last_end..start].to_string());
            last_end = end + 1;
            cursor = end + 1;
        } else {
            cursor += 1;
        }
    }
    literals.push(arg[last_end..].to_string());
    for index in 0..patterns.len() {
        patterns[index].suffix = literals[index + 1].clone();
    }

    patterns.reverse();
    Ok(patterns)
}

pub fn find_ellipses_patterns(arg: &str) -> Result<ArgPattern, String> {
    parse_arg_pattern(arg)
}

pub fn get_total_sizes(arg_patterns: &[ArgPattern]) -> Vec<u64> {
    let mut total_sizes = Vec::with_capacity(arg_patterns.len());
    for arg_pattern in arg_patterns {
        let mut total = 1u64;
        for pattern in arg_pattern {
            total *= pattern.seq.len() as u64;
        }
        total_sizes.push(total);
    }
    total_sizes
}

fn expand_arg_pattern(arg_pattern: &ArgPattern) -> Vec<String> {
    fn recurse(index: usize, patterns: &[&Pattern], current: String, out: &mut Vec<String>) {
        if index == patterns.len() {
            out.push(current);
            return;
        }
        let pattern = patterns[index];
        for part in &pattern.seq {
            let mut next = current.clone();
            next.push_str(&pattern.prefix);
            next.push_str(part);
            next.push_str(&pattern.suffix);
            recurse(index + 1, patterns, next, out);
        }
    }

    let patterns = arg_pattern.iter().rev().collect::<Vec<_>>();
    let mut out = Vec::new();
    recurse(0, &patterns, String::new(), &mut out);
    out
}

impl EndpointSet {
    pub fn get_endpoints(&self) -> Vec<String> {
        if !self.endpoints.is_empty() {
            return self.endpoints.clone();
        }
        let mut endpoints = Vec::new();
        for arg_pattern in &self.arg_patterns {
            endpoints.extend(expand_arg_pattern(arg_pattern));
        }
        endpoints
    }

    pub fn get_sets(&self) -> Vec<Vec<String>> {
        let endpoints = self.get_endpoints();
        let mut sets = Vec::new();
        let mut offset = 0usize;
        for indexes in &self.set_indexes {
            for &size in indexes {
                let size = size as usize;
                sets.push(endpoints[offset..offset + size].to_vec());
                offset += size;
            }
        }
        sets
    }
}

pub fn parse_endpoint_set(set_drive_count: u64, arg: &str) -> Result<EndpointSet, String> {
    let arg_pattern = parse_arg_pattern(arg)?;
    let args = vec![arg.to_string()];
    let arg_patterns = vec![arg_pattern];
    let set_indexes = get_set_indexes(
        &args,
        &get_total_sizes(&arg_patterns),
        set_drive_count,
        &arg_patterns,
    )?;

    Ok(EndpointSet {
        arg_patterns,
        endpoints: Vec::new(),
        set_indexes,
    })
}

pub fn get_all_sets(set_drive_count: u64, args: &[String]) -> Result<Vec<Vec<String>>, String> {
    if args.is_empty() {
        return Err("invalid argument".to_string());
    }

    let sets = if args.iter().all(|arg| !arg.contains('{')) {
        let set_indexes = if args.len() > 1 {
            get_set_indexes(
                args,
                &[args.len() as u64],
                set_drive_count,
                &Vec::<ArgPattern>::new(),
            )?
        } else {
            vec![vec![args.len() as u64]]
        };
        EndpointSet {
            arg_patterns: Vec::new(),
            endpoints: args.to_vec(),
            set_indexes,
        }
        .get_sets()
    } else {
        if args.len() != 1 {
            return Err("all args must have ellipses for pool expansion".to_string());
        }
        parse_endpoint_set(set_drive_count, &args[0])?.get_sets()
    };

    let mut unique = BTreeSet::new();
    for set in &sets {
        for endpoint in set {
            if !unique.insert(endpoint.clone()) {
                return Err("input args have duplicate ellipses".to_string());
            }
        }
    }
    Ok(sets)
}

pub fn merge_disks_layout_from_args(args: &[String]) -> Result<DisksLayout, String> {
    if args.is_empty() {
        return Err("invalid argument".to_string());
    }

    let plain = args.iter().all(|arg| !arg.contains('{'));
    if plain {
        let set_args = get_all_sets(0, args)?;
        return Ok(DisksLayout {
            legacy: true,
            pools: vec![PoolDisksLayout {
                cmdline: args.join(" "),
                layout: set_args,
            }],
        });
    }

    let mut layout = DisksLayout::default();
    for arg in args {
        if !arg.contains('{') && args.len() > 1 {
            return Err("all args must have ellipses for pool expansion".to_string());
        }
        layout.pools.push(PoolDisksLayout {
            cmdline: arg.clone(),
            layout: get_all_sets(0, std::slice::from_ref(arg))?,
        });
    }
    Ok(layout)
}

pub fn create_server_endpoints(
    server_addr: &str,
    pool_args: &[PoolDisksLayout],
    legacy: bool,
) -> Result<(Vec<PoolEndpoints>, SetupType), String> {
    if server_addr.is_empty() || pool_args.is_empty() {
        return Err("invalid argument".to_string());
    }

    let (_, server_port) = server_addr
        .rsplit_once(':')
        .ok_or_else(|| "invalid server address".to_string())?;
    if server_port.is_empty() || server_port.parse::<u16>().is_err() {
        return Err("invalid server address".to_string());
    }

    let mut endpoint_server_pools = Vec::new();
    let mut all = Vec::new();
    for pool in pool_args {
        let endpoints = pool.layout.iter().flatten().cloned().collect::<Vec<_>>();
        all.extend(endpoints.iter().cloned());
        let mut resolved_endpoints = Vec::new();
        for endpoint in &endpoints {
            let mut resolved = new_endpoint(endpoint)?;
            if resolved.endpoint_type() == EndpointType::Url && resolved.url.port().is_none() {
                let _ = resolved.url.set_port(Some(
                    server_port
                        .parse::<u16>()
                        .map_err(|_| "invalid server address".to_string())?,
                ));
            }
            resolved.update_is_local()?;
            resolved_endpoints.push(resolved);
        }
        endpoint_server_pools.push(PoolEndpoints {
            legacy,
            set_count: pool.layout.len(),
            drives_per_set: pool.layout.first().map(|set| set.len()).unwrap_or(0),
            endpoints,
            resolved_endpoints,
            cmd_line: pool.cmdline.clone(),
        });
    }

    let mut seen = BTreeSet::new();
    for endpoint in &all {
        if !seen.insert(endpoint.clone()) {
            return Err("duplicate endpoints found".to_string());
        }
    }

    let mut host_path_ports = HashMap::<(String, String), String>::new();
    for endpoint in &all {
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            continue;
        }
        let parsed = url::Url::parse(endpoint).map_err(|err| err.to_string())?;
        let host = parsed.host_str().unwrap_or_default().to_string();
        let path = parsed.path().to_string();
        let port = parsed
            .port_or_known_default()
            .map(|port| port.to_string())
            .unwrap_or_default();
        let key = (host.clone(), path);
        if let Some(existing_port) = host_path_ports.get(&key) {
            if existing_port != &port {
                return Err("path can not be served by different port on same address".to_string());
            }
        } else {
            host_path_ports.insert(key, port);
        }
        if is_local_host(&host) {
            // Localhost-specific guard from the upstream createServerEndpoints tests.
            let normalized = ("localhost".to_string(), parsed.path().to_string());
            if let Some(existing_port) = host_path_ports.get(&normalized) {
                if existing_port
                    != &parsed
                        .port_or_known_default()
                        .map(|port| port.to_string())
                        .unwrap_or_default()
                {
                    return Err(
                        "path can not be served by different port on same address".to_string()
                    );
                }
            } else {
                host_path_ports.insert(
                    normalized,
                    parsed
                        .port_or_known_default()
                        .map(|port| port.to_string())
                        .unwrap_or_default(),
                );
            }
        }
    }

    let has_url = all
        .iter()
        .any(|endpoint| endpoint.starts_with("http://") || endpoint.starts_with("https://"));
    let distinct_hosts = all
        .iter()
        .filter_map(|endpoint| url::Url::parse(endpoint).ok())
        .filter_map(|url| url.host_str().map(str::to_string))
        .collect::<BTreeSet<_>>()
        .len();

    let setup_type = if all.len() == 1 && !has_url {
        SetupType::ErasureSD
    } else if has_url && distinct_hosts > 1 {
        SetupType::DistErasure
    } else {
        SetupType::Erasure
    };

    Ok((endpoint_server_pools, setup_type))
}

pub fn get_local_peer(pools: &[PoolEndpoints], host: &str, port: &str) -> String {
    let mut peers = BTreeSet::new();
    for pool in pools {
        for endpoint in &pool.resolved_endpoints {
            if endpoint.endpoint_type() != EndpointType::Url {
                continue;
            }
            if endpoint.is_local {
                let host = endpoint.hostname();
                let port = if endpoint.port().is_empty() {
                    port.to_string()
                } else {
                    endpoint.port()
                };
                peers.insert(format!("{host}:{port}"));
            }
        }
    }
    peers.into_iter().next().unwrap_or_else(|| {
        let fallback_host = if host.is_empty() { "127.0.0.1" } else { host };
        format!("{fallback_host}:{port}")
    })
}

pub fn get_remote_peers(pools: &[PoolEndpoints], port: &str) -> (Vec<String>, String) {
    let mut peers = BTreeSet::new();
    let mut local = String::new();
    for pool in pools {
        for endpoint in &pool.resolved_endpoints {
            if endpoint.endpoint_type() != EndpointType::Url {
                continue;
            }
            let host = endpoint.hostname();
            let endpoint_port = if endpoint.port().is_empty() {
                port.to_string()
            } else {
                endpoint.port()
            };
            let peer = format!("{host}:{endpoint_port}");
            if endpoint.is_local && endpoint_port == port && local.is_empty() {
                local = peer.clone();
            }
            peers.insert(peer);
        }
    }
    (peers.into_iter().collect(), local)
}

//! SSRF protection for configured upstream URLs.
//!
//! Every URL that will be contacted (initial config URL *and* every redirect
//! target) passes through [`check_upstream_url`]:
//!   * scheme allowlist (http/https only);
//!   * hostname allowlist when configured;
//!   * DNS resolution with **all** answers validated;
//!   * loopback / RFC1918 / link-local / unique-local / metadata-service and
//!     other prohibited ranges rejected unless explicitly permitted.

use std::net::{IpAddr, ToSocketAddrs};

use ipnet::IpNet;

use crate::config::AppConfig;
use crate::error::{EngineError, Result};

/// Ranges that are always refused for upstreams unless
/// `security.allow_private_upstream_addresses` is true.
fn prohibited_networks() -> Vec<IpNet> {
    let cidrs = [
        "0.0.0.0/8",          // "this host" on Linux
        "10.0.0.0/8",         // RFC1918
        "100.64.0.0/10",      // CGNAT / carrier-grade NAT
        "127.0.0.0/8",        // loopback
        "169.254.0.0/16",     // link-local incl. cloud metadata service
        "172.16.0.0/12",      // RFC1918
        "192.0.0.0/24",       // IETF protocol assignments
        "192.168.0.0/16",     // RFC1918
        "198.18.0.0/15",      // benchmarking
        "::1/128",            // IPv6 loopback
        "fc00::/7",           // IPv6 unique local
        "fe80::/10",          // IPv6 link-local
        "::ffff:0:0/96",      // IPv4-mapped handled via unmapping too
    ];
    cidrs.iter().filter_map(|c| c.parse().ok()).collect()
}

pub fn ip_is_prohibited(ip: IpAddr) -> bool {
    let ip = match ip {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
    };
    prohibited_networks().iter().any(|n| n.contains(&ip))
}

/// Validate a single IP literal or resolved host against policy.
pub fn check_upstream_url(cfg: &AppConfig, raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).map_err(|_| {
        EngineError::BadParameter("invalid upstream url".into())
    })?;

    // 1. scheme
    if !cfg
        .security
        .allowed_upstream_schemes
        .iter()
        .any(|s| *s == url.scheme())
    {
        tracing::warn!(scheme = %url.scheme(), "rejected upstream url scheme");
        return Err(EngineError::BadParameter(
            "upstream scheme not allowed".into(),
        ));
    }

    let host = url.host_str().unwrap_or("");
    if host.is_empty() {
        return Err(EngineError::BadParameter("upstream url missing host".into()));
    }

    // 2. explicit hostname allowlist wins over everything else
    if !cfg.security.upstream_host_allowlist.is_empty() {
        let ok = cfg
            .security
            .upstream_host_allowlist
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host));
        if !ok {
            tracing::warn!("rejected upstream host outside allowlist");
            return Err(EngineError::BadParameter(
                "upstream host not permitted".into(),
            ));
        }
    }

    // 3. IP policy (skip only when the host is explicitly allowlisted AND
    //    private addresses were consciously permitted by the operator)
    if cfg.security.allow_private_upstream_addresses {
        return Ok(url);
    }

    let ips = resolve(host, url.port_or_known_default())
        .map_err(|_| EngineError::UpstreamUnavailable)?;
    if ips.is_empty() {
        return Err(EngineError::UpstreamUnavailable);
    }
    for ip in ips {
        if ip_is_prohibited(ip) {
            // Log the *decision*, never full internal topology details.
            tracing::warn!(%ip, "blocked upstream address by network policy");
            return Err(EngineError::BadParameter(
                "upstream destination not permitted by network policy".into(),
            ));
        }
    }
    Ok(url)
}

fn resolve(host: &str, port: Option<u16>) -> std::io::Result<Vec<IpAddr>> {
    let h = host.trim_end_matches('.');
    // Strip IPv6 brackets if present.
    let h = h.strip_prefix('[').unwrap_or(h);
    let h = h.strip_suffix(']').unwrap_or(h);
    let port = port.unwrap_or(80);
    let addrs = (h, port).to_socket_addrs()?;
    Ok(addrs.map(|a| a.ip()).collect())
}

#[derive(Debug, thiserror::Error)]
#[error("upstream check failed")]
pub struct UpstreamCheckError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    fn cfg() -> AppConfig {
        let mut c = AppConfig::default();
        c.security.api_tokens = vec!["t".into()];
        c
    }

    #[test]
    fn blocks_literal_loopback() {
        let e = check_upstream_url(&cfg(), "http://127.0.0.1:8000/live").unwrap_err();
        assert_eq!(e.code(), "bad_parameter");
        let e = check_upstream_url(&cfg(), "http://localhost/live").unwrap_err();
        assert_eq!(e.code(), "bad_parameter");
        let e = check_upstream_url(&cfg(), "http://[::1]/live").unwrap_err();
        assert_eq!(e.code(), "bad_parameter");
    }

    #[test]
    fn blocks_rfc1918_and_metadata() {
        for u in [
            "http://10.1.2.3/x",
            "http://192.168.4.5/x",
            "http://172.16.0.9/x",
            "http://169.254.169.254/latest/meta-data/",
            "http://100.64.1.2/x",
            "http://[fd00::5]/x",
            "http://[fe80::1]/x",
        ] {
            let e = check_upstream_url(&cfg(), u).expect_err(u);
            assert_eq!(e.code(), "bad_parameter", "{u} should be blocked");
        }
    }

    #[test]
    fn blocks_bad_schemes() {
        for u in ["file:///etc/passwd", "gopher://x.test/1", "ftp://x.test/a"] {
            assert!(check_upstream_url(&cfg(), u).is_err());
        }
    }

    #[test]
    fn ipv4_mapped_ipv6_blocked() {
        assert!(ip_is_prohibited("::ffff:127.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(ip_is_prohibited("::ffff:169.254.169.254".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn public_ip_allowed() {
        assert!(!ip_is_prohibited("93.184.216.34".parse().unwrap()));
        assert!(!ip_is_prohibited("2606::1".parse().unwrap()));
    }

    #[test]
    fn allowlist_overrides_when_private_permitted() {
        let mut c = cfg();
        c.security.allow_private_upstream_addresses = true;
        c.security.upstream_host_allowlist = vec!["internal.radio.lan".into()];
        // With private allowed we skip DNS policy entirely.
        assert!(check_upstream_url(&c, "http://127.0.0.1/live").is_ok());
        // But unknown hosts still fail the allowlist.
        assert!(check_upstream_url(&c, "http://evil.example/live").is_err());
    }

    #[test]
    fn host_allowlist_enforced_when_set() {
        let mut c = cfg();
        c.security.upstream_host_allowlist = vec!["stream.example.org".into()];
        assert!(check_upstream_url(&c, "https://anything-else.org/live").is_err());
    }
}

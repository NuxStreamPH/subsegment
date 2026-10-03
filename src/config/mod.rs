//! Configuration loading, defaults and validation.
//!
//! Sources (later wins):
//! 1. built-in defaults;
//! 2. YAML config file (`--config path` or `NUXSTREAM_CONFIG=path`, else `./config.yaml`);
//! 3. environment variables prefixed with `NUXSTREAM__` (figment nesting), e.g.
//!    `NUXSTREAM__SERVER__BIND=0.0.0.0:8080`;
//! 4. `NUXSTREAM_TOKENS=key1,key2` for bearer tokens so secrets can be
//!    injected by the deployment environment without touching the repo.

pub mod validation;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};
use crate::types::{Codec, Quality, SourceType};

pub const ENV_CONFIG_PATH: &str = "NUXSTREAM_CONFIG";
pub const ENV_TOKENS: &str = "NUXSTREAM_TOKENS";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub transcoding: TranscodingConfig,
    /// mountpoint id -> broadcast definition
    #[serde(default)]
    pub broadcasts: BTreeMap<String, BroadcastConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub bind: String,
    /// Public name used in headers/metadata.
    pub name: String,
    /// Emit JSON logs instead of human-readable.
    pub log_json: bool,
    /// `tracing` env-filter style directive default.
    pub log_level: String,
    /// Graceful shutdown timeout seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
            name: "NuxStream Stream Engine".into(),
            log_json: false,
            log_level: "info,nuxstream_engine=info".into(),
            shutdown_timeout_secs: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    pub require_authentication: bool,
    pub allow_anonymous_streaming: bool,
    pub allow_private_upstream_addresses: bool,
    /// Allowed URL schemes for upstreams.
    pub allowed_upstream_schemes: Vec<String>,
    /// Optional hostname allowlist; when non-empty, only these hosts may be
    /// contacted (exact match, case-insensitive).
    pub upstream_host_allowlist: Vec<String>,
    /// Static bearer tokens accepted by the engine.
    pub api_tokens: Vec<String>,
    /// Max inbound request header size (bytes) per connection.
    pub max_header_bytes: usize,
    /// Per-client-IP request rate limit (requests/second, token bucket).
    pub requests_per_second_per_ip: u32,
    /// Burst allowance on top of the sustained rate.
    pub request_burst_per_ip: u32,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            require_authentication: true,
            allow_anonymous_streaming: false,
            allow_private_upstream_addresses: false,
            allowed_upstream_schemes: vec!["http".into(), "https".into()],
            upstream_host_allowlist: Vec::new(),
            api_tokens: Vec::new(),
            max_header_bytes: 16 * 1024,
            requests_per_second_per_ip: 20,
            request_burst_per_ip: 40,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    pub max_clients_global: usize,
    pub max_clients_per_broadcast: usize,
    pub max_transcoding_pipelines: usize,
    /// Bounded fan-out buffer per listener (chunk count).
    pub listener_queue_chunks: usize,
    /// A listener whose queue is full for longer than this gets dropped.
    pub listener_lag_timeout_secs: u64,
    /// Retain a pipeline this long after the last listener leaves.
    pub pipeline_grace_secs: u64,
    /// Upstream read timeout; no data within it triggers reconnect.
    pub upstream_read_timeout_secs: u64,
    /// TCP/TLS connect timeout for upstreams.
    pub upstream_connect_timeout_secs: u64,
    /// Maximum redirects followed for upstream requests.
    pub upstream_max_redirects: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_clients_global: 500,
            max_clients_per_broadcast: 100,
            max_transcoding_pipelines: 16,
            listener_queue_chunks: 128,
            listener_lag_timeout_secs: 10,
            pipeline_grace_secs: 30,
            upstream_read_timeout_secs: 30,
            upstream_connect_timeout_secs: 10,
            upstream_max_redirects: 3,
        }
    }
}

impl LimitsConfig {
    pub fn listener_lag_timeout(&self) -> Duration {
        Duration::from_secs(self.listener_lag_timeout_secs)
    }
    pub fn pipeline_grace(&self) -> Duration {
        Duration::from_secs(self.pipeline_grace_secs)
    }
    pub fn upstream_read_timeout(&self) -> Duration {
        Duration::from_secs(self.upstream_read_timeout_secs)
    }
    pub fn upstream_connect_timeout(&self) -> Duration {
        Duration::from_secs(self.upstream_connect_timeout_secs)
    }
}

/// Bitrate targets (kbps) for LOW/MEDIUM/HIGH per codec. Configurable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamingConfig {
    /// Codec chosen when clients send `codec=auto`.
    pub auto_codec: Codec,
    pub low_kbps: u32,
    pub medium_kbps: u32,
    pub high_kbps: u32,
    /// Per-codec overrides win over the generic values above.
    pub opus_low_kbps: Option<u32>,
    pub opus_medium_kbps: Option<u32>,
    pub opus_high_kbps: Option<u32>,
    pub mp3_low_kbps: Option<u32>,
    pub mp3_medium_kbps: Option<u32>,
    pub mp3_high_kbps: Option<u32>,
    pub aac_low_kbps: Option<u32>,
    pub aac_medium_kbps: Option<u32>,
    pub aac_high_kbps: Option<u32>,
    pub aacplus_low_kbps: Option<u32>,
    pub aacplus_medium_kbps: Option<u32>,
    pub aacplus_high_kbps: Option<u32>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            auto_codec: Codec::Mp3,
            low_kbps: 64,
            medium_kbps: 128,
            high_kbps: 192,
            opus_low_kbps: None,
            opus_medium_kbps: None,
            opus_high_kbps: None,
            mp3_low_kbps: None,
            mp3_medium_kbps: None,
            mp3_high_kbps: None,
            aac_low_kbps: None,
            aac_medium_kbps: None,
            aac_high_kbps: None,
            aacplus_low_kbps: None,
            aacplus_medium_kbps: None,
            aacplus_high_kbps: None,
        }
    }
}

impl StreamingConfig {
    /// Resolve target bitrate in bits/s for a quality+codec pair.
    pub fn target_bitrate(&self, q: Quality, c: Codec) -> Option<u32> {
        let kbps = match (q, c) {
            (Quality::Low, Codec::Opus) => self.opus_low_kbps,
            (Quality::Medium, Codec::Opus) => self.opus_medium_kbps,
            (Quality::High, Codec::Opus) => self.opus_high_kbps,
            (Quality::Low, Codec::Mp3) => self.mp3_low_kbps,
            (Quality::Medium, Codec::Mp3) => self.mp3_medium_kbps,
            (Quality::High, Codec::Mp3) => self.mp3_high_kbps,
            (Quality::Low, Codec::Aac) => self.aac_low_kbps,
            (Quality::Medium, Codec::Aac) => self.aac_medium_kbps,
            (Quality::High, Codec::Aac) => self.aac_high_kbps,
            (Quality::Low, Codec::AacPlus) => self.aacplus_low_kbps,
            (Quality::Medium, Codec::AacPlus) => self.aacplus_medium_kbps,
            (Quality::High, Codec::AacPlus) => self.aacplus_high_kbps,
            _ => None,
        }
        .unwrap_or(match q {
            Quality::Low => self.low_kbps,
            Quality::Medium => self.medium_kbps,
            Quality::High => self.high_kbps,
            Quality::Original => return None,
        });
        Some(kbps.saturating_mul(1000))
    }

    /// Concrete codec for a request where the client asked for `auto`.
    pub fn resolve_auto(&self, requested: Codec) -> Codec {
        match requested {
            Codec::Auto => self.auto_codec,
            other => other,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TranscodingConfig {
    /// `ffmpeg` | `passthrough_only` (disable re-encoding entirely).
    pub backend: String,
    pub ffmpeg_path: String,
    /// Concurrent transcode hard cap independent from limits (belt & braces).
    pub max_concurrent: usize,
    pub threads_per_pipeline: u32,
    /// Bounded output queue length per pipeline.
    pub output_queue_len: usize,
    /// Set true when the local ffmpeg build includes libfdk_aac (HE-AAC).
    pub aacplus_supported: bool,
}

impl Default for TranscodingConfig {
    fn default() -> Self {
        Self {
            backend: "ffmpeg".into(),
            ffmpeg_path: "ffmpeg".into(),
            max_concurrent: 16,
            threads_per_pipeline: 2,
            output_queue_len: 64,
            aacplus_supported: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BroadcastConfig {
    pub enabled: bool,
    pub source: SourceConfig,
    /// Human readable station name exposed via metadata.
    #[serde(default)]
    pub station_name: Option<String>,
    #[serde(default)]
    pub allowed_qualities: Vec<Quality>,
    #[serde(default)]
    pub allowed_codecs: Vec<Codec>,
    /// Overrides global `security.require_authentication` for this broadcast.
    #[serde(default)]
    pub authentication_required: Option<bool>,
    /// Extra tokens that may access *only* this broadcast.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// If false, ORIGINAL passthrough is refused even when technically fine.
    #[serde(default = "default_true")]
    pub allow_passthrough: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub r#type: SourceType,
    pub url: String,
    /// Optional extra headers sent to the upstream (e.g. icy password).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

impl BroadcastConfig {
    pub fn quality_allowed(&self, q: Quality) -> bool {
        self.allowed_qualities.is_empty() || self.allowed_qualities.contains(&q)
    }
    pub fn codec_allowed(&self, c: Codec) -> bool {
        matches!(c, Codec::Auto)
            || self.allowed_codecs.is_empty()
            || self.allowed_codecs.contains(&c)
    }
    pub fn auth_required(&self, global: bool) -> bool {
        self.authentication_required.unwrap_or(global)
    }
    pub fn allowed_quality_names(&self) -> BTreeSet<&'static str> {
        if self.allowed_qualities.is_empty() {
            [Quality::Low, Quality::Medium, Quality::High, Quality::Original]
                .iter()
                .map(|q| q.as_str())
                .collect()
        } else {
            self.allowed_qualities.iter().map(|q| q.as_str()).collect()
        }
    }
}

impl AppConfig {
    /// Load + validate configuration. Fails clearly on invalid input.
    pub fn load(path_override: Option<PathBuf>) -> Result<Self> {
        use figment::{
            providers::{Env, Format, Serialized, Yaml},
            Figment,
        };

        let path = path_override
            .or_else(|| std::env::var(ENV_CONFIG_PATH).ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("config.yaml"));

        let mut fig = Figment::from(Serialized::defaults(AppConfig::default()));
        if path.exists() {
            fig = fig.merge(Yaml::file(&path));
        } else if path_override.is_some() || std::env::var(ENV_CONFIG_PATH).is_ok() {
            return Err(EngineError::Config(format!(
                "configuration file not found: {}",
                path.display()
            )));
        }
        // NUXSTREAM__SECTION__KEY style nesting.
        fig = fig.merge(Env::prefixed("NUXSTREAM__").split("__"));

        let mut cfg: AppConfig =
            fig.extract().map_err(|e| EngineError::Config(e.to_string()))?;

        // Secrets from the deployment environment (never committed).
        if let Ok(tokens) = std::env::var(ENV_TOKENS) {
            let parsed: Vec<String> = tokens
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect();
            if !parsed.is_empty() {
                cfg.security.api_tokens = parsed;
            }
        }

        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }

    pub fn broadcast(&self, mountpoint: &str) -> Option<&BroadcastConfig> {
        self.broadcasts.get(mountpoint)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            security: SecurityConfig::default(),
            limits: LimitsConfig::default(),
            streaming: StreamingConfig::default(),
            transcoding: TranscodingConfig::default(),
            broadcasts: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        AppConfig::default().validate().unwrap();
    }

    #[test]
    fn yaml_roundtrip_and_bitrates() {
        let yaml = r#"
server:
  bind: "127.0.0.1:9999"
security:
  require_authentication: true
  api_tokens:
    - "s3cret"
streaming:
  auto_codec: opus
  low_kbps: 48
broadcasts:
  dzyw_broadcast_digital:
    enabled: true
    source:
      type: icecast
      url: "https://example.com/live"
    allowed_qualities: [low, medium, high, original]
    allowed_codecs: [opus, mp3, aac]
    authentication_required: true
"#;
        let cfg: AppConfig = serde_yaml_helper(yaml);
        cfg.validate().unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:9999");
        assert_eq!(cfg.streaming.low_kbps, 48);
        let b = cfg.broadcast("dzyw_broadcast_digital").unwrap();
        assert_eq!(b.source.r#type, SourceType::Icecast);
        assert!(b.codec_allowed(Codec::Opus));
        assert!(!b.codec_allowed(Codec::AacPlus));
        assert_eq!(
            cfg.streaming.target_bitrate(Quality::Low, Codec::Opus),
            Some(48_000)
        );
    }

    fn serde_yaml_helper(yaml: &str) -> AppConfig {
        use figment::providers::{Format, Yaml};
        use figment::Figment;
        Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Yaml::string(yaml))
            .extract()
            .unwrap()
    }

    #[test]
    fn unknown_source_type_fails_extraction() {
        use figment::providers::{Format, Yaml};
        use figment::Figment;
        let yaml = r#"
broadcasts:
  broken:
    enabled: true
    source:
      type: carrier-pigeon
      url: "http://x.test/a"
"#;
        let res: std::result::Result<AppConfig, figment::Error> =
            Figment::from(Serialized::defaults(AppConfig::default()))
                .merge(Yaml::string(yaml))
                .extract();
        assert!(res.is_err());
    }

    #[test]
    fn disabled_broadcast_still_parses() {
        let yaml = r#"
broadcasts:
  off_air:
    enabled: false
    source:
      type: http
      url: "https://x.test/a.mp3"
"#;
        let cfg: AppConfig = serde_yaml_helper(yaml);
        cfg.validate().unwrap();
        assert!(!cfg.broadcast("off_air").unwrap().enabled);
    }
}

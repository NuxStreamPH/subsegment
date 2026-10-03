//! Authorization policy: turns authentication results plus broadcast
//! configuration into allow/deny decisions, mapping every failure to a
//! plain HTTP error *before* any stream resources are allocated.

use crate::config::{AppConfig, BroadcastConfig};
use crate::error::{EngineError, Result};

use super::bearer::BearerTokenProvider;
use super::{AuthError, AuthProvider, AuthRequest, Identity};

/// Composite provider used by the HTTP layer.
pub struct AccessPolicy {
    provider: BearerTokenProvider,
    require_authentication: bool,
    allow_anonymous_streaming: bool,
}

impl AccessPolicy {
    pub fn from_config(cfg: &AppConfig) -> Self {
        let scoped = cfg
            .broadcasts
            .iter()
            .filter(|(_, b)| !b.tokens.is_empty())
            .map(|(name, b)| (name.clone(), b.tokens.clone()))
            .collect();
        Self {
            provider: BearerTokenProvider::new(&cfg.security.api_tokens, scoped),
            require_authentication: cfg.security.require_authentication,
            allow_anonymous_streaming: cfg.security.allow_anonymous_streaming,
        }
    }

    /// Authenticate a request for `mountpoint`.
    ///
    /// Order of operations guarantees no upstream/transcoder work happens
    /// until this returns Ok.
    pub async fn authenticate(&self, mountpoint: &str, authorization: Option<&str>) -> Result<Identity> {
        let req = AuthRequest {
            authorization,
            mountpoint,
        };

        // Anonymous is only allowed when auth is globally off *and* anonymous
        // streaming is enabled, and the broadcast itself does not demand auth.
        if authorization.is_none() {
            if self.require_authentication || !self.allow_anonymous_streaming {
                return Err(map_auth_error(AuthError::Missing));
            }
            return Ok(Identity::anonymous());
        }

        self.provider
            .authenticate(&req)
            .await
            .map_err(map_auth_error)
    }

    /// Per-broadcast access check performed after authentication succeeds.
    pub fn authorize_broadcast(&self, identity: &Identity, b: &BroadcastConfig) -> Result<()> {
        // Broadcast-level opt-in to auth overrides global settings.
        if b.auth_required(self.require_authentication) && identity.anonymous {
            return Err(EngineError::Unauthorized);
        }
        Ok(())
    }
}

fn map_auth_error(e: AuthError) -> EngineError {
    match e {
        AuthError::Missing => EngineError::Unauthorized,
        AuthError::Invalid => EngineError::Unauthorized,
        AuthError::Forbidden => EngineError::Forbidden,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BroadcastConfig, SourceConfig};
    use crate::types::SourceType;

    fn cfg(auth_global: bool, anon: bool, tokens: Vec<&str>) -> AppConfig {
        let mut c = AppConfig::default();
        c.security.require_authentication = auth_global;
        c.security.allow_anonymous_streaming = anon;
        c.security.api_tokens = tokens.into_iter().map(String::from).collect();
        c.broadcasts.insert(
            "pub_fm".into(),
            BroadcastConfig {
                enabled: true,
                source: SourceConfig {
                    r#type: SourceType::Http,
                    url: "https://x.test/a".into(),
                    headers: Default::default(),
                },
                station_name: None,
                allowed_qualities: vec![],
                allowed_codecs: vec![],
                authentication_required: Some(false),
                tokens: vec![],
                allow_passthrough: true,
            },
        );
        c.broadcasts.insert(
            "priv_fm".into(),
            BroadcastConfig {
                enabled: true,
                source: SourceConfig {
                    r#type: SourceType::Http,
                    url: "https://y.test/a".into(),
                    headers: Default::default(),
                },
                station_name: None,
                allowed_qualities: vec![],
                allowed_codecs: vec![],
                authentication_required: Some(true),
                tokens: vec!["scoped".into()],
                allow_passthrough: true,
            },
        );
        c
    }

    #[tokio::test]
    async fn anonymous_denied_when_auth_required() {
        let p = AccessPolicy::from_config(&cfg(true, false, vec!["t"]));
        let e = p.authenticate("pub_fm", None).await.unwrap_err();
        assert_eq!(e.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn anonymous_allowed_for_public_broadcast() {
        let p = AccessPolicy::from_config(&cfg(false, true, vec![]));
        let id = p.authenticate("pub_fm", None).await.unwrap();
        assert!(id.anonymous);
        // but denied for broadcasts that explicitly require auth
        let priv_b = p_dummy_broadcast();
        assert_eq!(
            p.authorize_broadcast(&id, &priv_b)
                .unwrap_err()
                .status(),
            axum::http::StatusCode::UNAUTHORIZED
        );
    }

    fn p_dummy_broadcast() -> BroadcastConfig {
        BroadcastConfig {
            enabled: true,
            source: SourceConfig {
                r#type: SourceType::Http,
                url: "https://z.test/a".into(),
                headers: Default::default(),
            },
            station_name: None,
            allowed_qualities: vec![],
            allowed_codecs: vec![],
            authentication_required: Some(true),
            tokens: vec![],
            allow_passthrough: true,
        }
    }

    #[tokio::test]
    async fn bad_token_rejected_before_resources() {
        let p = AccessPolicy::from_config(&cfg(true, false, vec!["good"]));
        let e = p
            .authenticate("priv_fm", Some("Bearer wrong"))
            .await
            .unwrap_err();
        assert_eq!(e.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn scoped_token_works_only_on_its_broadcast() {
        let p = AccessPolicy::from_config(&cfg(true, false, vec!["good"]));
        p.authenticate("priv_fm", Some("Bearer scoped")).await.unwrap();
        let e = p
            .authenticate("pub_fm", Some("Bearer scoped"))
            .await
            .unwrap_err();
        assert_eq!(e.code(), "unauthorized");
    }
}

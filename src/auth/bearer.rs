//! Static bearer-token authentication provider.
//!
//! Tokens are compared in constant time (SHA-256 digest comparison) and a
//! short one-way label is derived for logging so raw tokens never appear in
//! logs or metrics.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::{AuthError, AuthProvider, AuthRequest, Identity};

/// Finalized token digest.
type TokenDigest = [u8; 32];

#[derive(Clone)]
pub struct BearerTokenProvider {
    /// Global tokens: `(digest, label)`.
    global: Vec<(TokenDigest, String)>,
    /// Per-broadcast tokens: mountpoint -> list of `(digest, label)`.
    scoped: std::collections::BTreeMap<String, Vec<(TokenDigest, String)>>,
}

fn digest(token: &str) -> TokenDigest {
    let mut d = Sha256::new();
    d.update(token.as_bytes());
    d.finalize().into()
}

/// Short stable label for logs: first 8 hex chars of the token digest.
fn label(d: &TokenDigest) -> String {
    let mut s = String::with_capacity(8);
    for b in &d[..4] {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

impl BearerTokenProvider {
    pub fn new(global_tokens: &[String], scoped: Vec<(String, Vec<String>)>) -> Self {
        Self {
            global: global_tokens
                .iter()
                .map(|t| {
                    let d = digest(t);
                    (d, label(&d))
                })
                .collect(),
            scoped: scoped
                .into_iter()
                .map(|(mp, toks)| {
                    (
                        mp,
                        toks.iter()
                            .map(|t| {
                                let d = digest(t);
                                (d, label(&d))
                            })
                            .collect(),
                    )
                })
                .collect(),
        }
    }

    /// Extract the token from an `Authorization: Bearer <token>` value.
    pub fn parse_bearer(header: &str) -> Option<&str> {
        let mut parts = header.splitn(2, ' ');
        match (parts.next(), parts.next()) {
            (Some(scheme), Some(rest)) if scheme.eq_ignore_ascii_case("bearer") => {
                let t = rest.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t)
                }
            }
            _ => None,
        }
    }

    fn matches(list: &[(TokenDigest, String)], token: &str) -> Option<String> {
        // Compare fixed-size digests: avoids early-exit prefix leaks that a
        // raw string comparison would have; hashing dominates the timing.
        let cand = digest(token);
        list.iter()
            .find(|(d, _)| d == &cand)
            .map(|(_, l)| l.clone())
    }
}

#[async_trait]
impl AuthProvider for BearerTokenProvider {
    async fn authenticate(&self, req: &AuthRequest<'_>) -> std::result::Result<Identity, AuthError> {
        let header = req.authorization.ok_or(AuthError::Missing)?;
        let token = Self::parse_bearer(header).ok_or(AuthError::Invalid)?;

        if let Some(lbl) = Self::matches(&self.global, token) {
            return Ok(Identity {
                principal: format!("bearer:{lbl}"),
                anonymous: false,
            });
        }
        if let Some(list) = self.scoped.get(req.mountpoint) {
            if let Some(lbl) = Self::matches(list, token) {
                return Ok(Identity {
                    principal: format!("bearer:{lbl}"),
                    anonymous: false,
                });
            }
        }
        Err(AuthError::Invalid)
    }

    fn name(&self) -> &'static str {
        "bearer"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov() -> BearerTokenProvider {
        BearerTokenProvider::new(
            &["global-token".to_string()],
            vec![("private_fm".to_string(), vec!["scoped-token".to_string()])],
        )
    }

    #[tokio::test]
    async fn valid_global_token() {
        let p = prov();
        let req = AuthRequest {
            authorization: Some("Bearer global-token"),
            mountpoint: "any",
        };
        let id = p.authenticate(&req).await.unwrap();
        assert!(!id.anonymous);
        assert!(id.principal.starts_with("bearer:"));
        // label must not contain the token itself
        assert!(!id.principal.contains("global-token"));
    }

    #[tokio::test]
    async fn scoped_token_only_for_its_broadcast() {
        let p = prov();
        let ok = AuthRequest {
            authorization: Some("Bearer scoped-token"),
            mountpoint: "private_fm",
        };
        p.authenticate(&ok).await.unwrap();
        let bad = AuthRequest {
            authorization: Some("Bearer scoped-token"),
            mountpoint: "other_fm",
        };
        assert!(matches!(
            p.authenticate(&bad).await,
            Err(AuthError::Invalid)
        ));
    }

    #[tokio::test]
    async fn invalid_and_missing() {
        let p = prov();
        let wrong = AuthRequest {
            authorization: Some("Bearer nope"),
            mountpoint: "x",
        };
        assert!(matches!(p.authenticate(&wrong).await, Err(AuthError::Invalid)));
        let missing = AuthRequest {
            authorization: None,
            mountpoint: "x",
        };
        assert!(matches!(p.authenticate(&missing).await, Err(AuthError::Missing)));
        let basic = AuthRequest {
            authorization: Some("Basic dXNlcjpwYXNz"),
            mountpoint: "x",
        };
        assert!(matches!(p.authenticate(&basic).await, Err(AuthError::Invalid)));
    }

    #[test]
    fn parse_bearer_variants() {
        assert_eq!(BearerTokenProvider::parse_bearer("Bearer abc"), Some("abc"));
        assert_eq!(BearerTokenProvider::parse_bearer("bearer abc"), Some("abc"));
        assert_eq!(BearerTokenProvider::parse_bearer("BEARER  abc "), Some("abc"));
        assert_eq!(BearerTokenProvider::parse_bearer("Bearer "), None);
        assert_eq!(BearerTokenProvider::parse_bearer("Token abc"), None);
    }
}

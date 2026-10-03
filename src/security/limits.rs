//! Concurrency & resource limit accounting (global listeners, per-broadcast
//! listeners, transcoding pipelines) and a per-IP token-bucket rate limiter.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::config::{AppConfig, LimitsConfig};
use crate::error::{EngineError, Result};

/// RAII permit for one listener slot at both global and broadcast level.
pub struct ListenerPermit {
    pub broadcast: String,
    guard: Arc<LimitGuard>,
}

impl Drop for ListenerPermit {
    fn drop(&mut self) {
        self.guard.release_listener(&self.broadcast);
    }
}

/// RAII permit for one transcoding pipeline slot.
pub struct PipelinePermit {
    pub guard: Arc<LimitGuard>,
    pub is_transcode: bool,
}

impl Drop for PipelinePermit {
    fn drop(&mut self) {
        if self.is_transcode {
            self.guard.release_pipeline();
        }
    }
}

#[derive(Clone)]
pub struct LimitGuard {
    limits: LimitsConfig,
    global_listeners: Arc<AtomicU64>,
    per_broadcast: Arc<RwLock<BTreeMap<String, u64>>>,
    pipelines: Arc<RwLock<u64>>,
}

impl LimitGuard {
    pub fn new(cfg: &AppConfig) -> Self {
        Self {
            limits: cfg.limits.clone(),
            global_listeners: Arc::new(AtomicU64::new(0)),
            per_broadcast: Arc::new(RwLock::new(BTreeMap::new())),
            pipelines: Arc::new(RwLock::new(0)),
        }
    }

    /// Reserve a listener slot for `broadcast`; fails with 429/503 *before*
    /// any pipeline work happens.
    pub fn acquire_listener(self: &Arc<Self>, broadcast: &str) -> Result<ListenerPermit> {
        let mut per = self.per_broadcast.write().unwrap();
        let count = per.get(broadcast).copied().unwrap_or(0);
        if count >= self.limits.max_clients_per_broadcast as u64 {
            return Err(EngineError::LimitExceeded);
        }
        let prev = self.global_listeners.fetch_add(1, Ordering::SeqCst);
        if prev >= self.limits.max_clients_global as u64 {
            self.global_listeners.fetch_sub(1, Ordering::SeqCst);
            return Err(EngineError::LimitExceeded);
        }
        *per.entry(broadcast.to_string()).or_insert(0) += 1;
        Ok(ListenerPermit {
            broadcast: broadcast.to_string(),
            guard: self.clone(),
        })
    }

    fn release_listener(&self, broadcast: &str) {
        self.global_listeners.fetch_sub(1, Ordering::SeqCst);
        let mut per = self.per_broadcast.write().unwrap();
        if let Some(c) = per.get_mut(broadcast) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                per.remove(broadcast);
            }
        }
    }

    /// Reserve a transcode pipeline slot (passthrough pipelines do not
    /// consume transcoding capacity).
    pub fn acquire_pipeline(self: &Arc<Self>, is_transcode: bool) -> Result<PipelinePermit> {
        if !is_transcode {
            return Ok(PipelinePermit {
                guard: self.clone(),
                is_transcode: false,
            });
        }
        let mut p = self.pipelines.write().unwrap();
        if *p >= self.limits.max_transcoding_pipelines as u64 {
            return Err(EngineError::LimitExceeded);
        }
        *p += 1;
        Ok(PipelinePermit {
            guard: self.clone(),
            is_transcode: true,
        })
    }

    fn release_pipeline(&self) {
        let mut p = self.pipelines.write().unwrap();
        *p = p.saturating_sub(1);
    }

    pub fn active_listeners(&self) -> u64 {
        self.global_listeners.load(Ordering::Relaxed)
    }

    pub fn active_pipelines(&self) -> u64 {
        *self.pipelines.read().unwrap()
    }

    pub fn listeners_for(&self, broadcast: &str) -> u64 {
        self.per_broadcast
            .read()
            .unwrap()
            .get(broadcast)
            .copied()
            .unwrap_or(0)
    }
}

/// Token-bucket per client IP. Cheap, bounded memory: entries idle longer
/// than `RETENTION` are pruned periodically (at most once per minute).
pub struct RateLimiter {
    rate: f64, // tokens per second
    burst: f64, // bucket capacity
    buckets: Mutex<BTreeMap<u128, Bucket>>,
    last_prune: AtomicU64, // unix-seconds of previous prune
}

struct Bucket {
    tokens: f64,
    updated: Instant,
}

const RETENTION: Duration = Duration::from_secs(600);

impl RateLimiter {
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self {
            rate: cfg.security.requests_per_second_per_ip as f64,
            burst: cfg.security.request_burst_per_ip.max(1) as f64
                + cfg.security.requests_per_second_per_ip as f64,
            buckets: Mutex::new(BTreeMap::new()),
            last_prune: AtomicU64::new(unix_now()),
        }
    }

    /// Returns Ok(()) when the request may proceed, Err(429) otherwise.
    pub fn check(&self, ip_bits: u128) -> Result<()> {
        let now = Instant::now();
        let mut g = self.buckets.lock().unwrap();
        self.prune_if_due(&mut g, now);
        let b = g.entry(ip_bits).or_insert(Bucket {
            tokens: self.burst,
            updated: now,
        });
        let elapsed = now.duration_since(b.updated).as_secs_f64();
        b.updated = now;
        b.tokens = (b.tokens + elapsed * self.rate).min(self.burst);
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err(EngineError::RateLimited)
        }
    }

    fn prune_if_due(&self, g: &mut BTreeMap<u128, Bucket>, now: Instant) {
        let secs = unix_now();
        let last = self.last_prune.load(Ordering::Relaxed);
        if secs.saturating_sub(last) < 60 {
            return;
        }
        self.last_prune.store(secs, Ordering::Relaxed);
        g.retain(|_, b| now.duration_since(b.updated) < RETENTION);
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(global: usize, per_bc: usize, pipes: usize) -> AppConfig {
        let mut c = AppConfig::default();
        c.security.api_tokens = vec!["t".into()];
        c.limits.max_clients_global = global;
        c.limits.max_clients_per_broadcast = per_bc;
        c.limits.max_transcoding_pipelines = pipes;
        c
    }

    #[test]
    fn listener_limits_and_release() {
        let g = Arc::new(LimitGuard::new(&cfg_with(3, 2, 8)));
        let a = g.acquire_listener("fm").unwrap();
        let b = g.acquire_listener("fm").unwrap();
        assert!(g.acquire_listener("fm").is_err()); // per-broadcast cap
        let c = g.acquire_listener("other").unwrap();
        assert_eq!(g.active_listeners(), 3);
        assert!(g.acquire_listener("x").is_err()); // global cap
        drop(a);
        let d = g.acquire_listener("fm").unwrap();
        assert_eq!(g.listeners_for("fm"), 2);
        drop((b, c, d));
        assert_eq!(g.active_listeners(), 0);
    }

    #[test]
    fn pipeline_limits() {
        let g = Arc::new(LimitGuard::new(&cfg_with(10, 5, 2)));
        let p1 = g.acquire_pipeline(true).unwrap();
        let p2 = g.acquire_pipeline(true).unwrap();
        assert!(g.acquire_pipeline(true).is_err());
        // passthrough doesn't consume transcode slots
        let _p3 = g.acquire_pipeline(false).unwrap();
        drop(p1);
        assert_eq!(g.active_pipelines(), 1);
        let _p4 = g.acquire_pipeline(true).unwrap();
        drop((p2, _p3, _p4));
        assert_eq!(g.active_pipelines(), 0);
    }

    #[test]
    fn rate_limiter_bucket() {
        let rl = RateLimiter::from_config(&{
            let mut c = cfg_with(10, 5, 2);
            c.security.requests_per_second_per_ip = 2;
            c.security.request_burst_per_ip = 3;
            c
        });
        let ip = 1u128;
        for _ in 0..5 {
            rl.check(ip).unwrap(); // burst(3)+rate allowance
        }
        // bucket should now be empty-ish
        assert!(rl.check(ip).is_err());
        // different ip unaffected
        assert!(rl.check(2u128).is_ok());
        std::thread::sleep(Duration::from_millis(600));
        assert!(rl.check(ip).is_ok());
    }
}

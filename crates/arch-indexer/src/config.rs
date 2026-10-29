//! Host configuration from the environment. Variable names are those
//! arch-prime-analytics reads for the same settings. Program ids are not here:
//! each indexer reads its own (see [`optional_program_id`]).

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Config {
    /// `DATABASE_URL`, required.
    pub database_url: String,
    /// `INDEXER_BASE_URL`, required: the arch-rust-indexer API origin.
    pub indexer_base_url: String,
    /// `INDEXER_API_KEY`: sent as `X-API-Key` on every indexer request.
    pub indexer_api_key: Option<String>,
    /// `INDEXER_NETWORK`: network path segment for the indexer's scoped routes
    /// (`/api/v1/{network}/…`). None = the deployment's default (un-prefixed)
    /// routes. Each service serves exactly ONE network.
    pub indexer_network: Option<String>,
    /// `API_BIND_ADDR`, else `[::]:$PORT`, else `[::]:8080`.
    pub api_bind_addr: String,
    /// `TAIL_INTERVAL_SECS`, default 5.
    pub tail_interval_secs: u64,
    /// `SAMPLE_INTERVAL_SECS`, default 300.
    pub sample_interval_secs: u64,
    /// `ENRICH_INTERVAL_SECS`, default 60.
    pub enrich_interval_secs: u64,
    /// `EXPORT_PAGE_LIMIT`, default 1000.
    pub export_page_limit: u32,
    /// `INGEST_CONFIRMATION_DEPTH`, default 0: ingest only up to `tip - N`.
    /// See [`crate::runner::Runner::confirmation_depth`].
    pub ingest_confirmation_depth: i64,
    /// `INGEST_END_HEIGHT`: ingest nothing above this block height. Unset = to
    /// the tip. The checkpoint stops at the bound, so removing the variable
    /// resumes from exactly there.
    pub ingest_end_height: Option<i64>,
    /// `CACHE_TTL_SECS`, default 30; 0 disables the response cache.
    pub cache_ttl_secs: u64,
    /// `API_QUERY_CONCURRENCY`, default 3: permits of the `cached` route group.
    pub query_concurrency: usize,
    /// `API_PORTFOLIO_CONCURRENCY`, default 2: permits of the `per_address`
    /// route group (analytics' name for the same load domain).
    pub per_address_concurrency: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: required("DATABASE_URL")?,
            indexer_base_url: required("INDEXER_BASE_URL")?,
            indexer_api_key: std::env::var("INDEXER_API_KEY").ok(),
            indexer_network: indexer_network(std::env::var("INDEXER_NETWORK").ok())?,
            api_bind_addr: api_bind_addr(),
            tail_interval_secs: optional("TAIL_INTERVAL_SECS", "5").parse()?,
            sample_interval_secs: optional("SAMPLE_INTERVAL_SECS", "300").parse()?,
            enrich_interval_secs: optional("ENRICH_INTERVAL_SECS", "60").parse()?,
            export_page_limit: optional("EXPORT_PAGE_LIMIT", "1000").parse()?,
            ingest_confirmation_depth: optional("INGEST_CONFIRMATION_DEPTH", "0")
                .parse::<i64>()?
                .max(0),
            ingest_end_height: match std::env::var("INGEST_END_HEIGHT") {
                Ok(v) if !v.trim().is_empty() => Some(
                    v.trim()
                        .parse::<i64>()
                        .context("INGEST_END_HEIGHT must be a block height")?,
                ),
                _ => None,
            },
            cache_ttl_secs: optional("CACHE_TTL_SECS", "30").parse()?,
            query_concurrency: positive("API_QUERY_CONCURRENCY", DEFAULT_QUERY_CONCURRENCY),
            per_address_concurrency: positive(
                "API_PORTFOLIO_CONCURRENCY",
                DEFAULT_PER_ADDRESS_CONCURRENCY,
            ),
        })
    }

    /// Which network this deployment serves, `default` when unset.
    pub fn network(&self) -> &str {
        self.indexer_network.as_deref().unwrap_or("default")
    }
}

pub const DEFAULT_QUERY_CONCURRENCY: usize = 3;
pub const DEFAULT_PER_ADDRESS_CONCURRENCY: usize = 2;

/// API_BIND_ADDR wins; otherwise PaaS-injected PORT (Railway/Heroku); otherwise
/// port 8080. `[::]` binds dual-stack on Linux — Railway's private network is
/// IPv6-only, so an IPv4-only bind would refuse `*.railway.internal` traffic.
fn api_bind_addr() -> String {
    std::env::var("API_BIND_ADDR").unwrap_or_else(|_| match std::env::var("PORT") {
        Ok(port) => format!("[::]:{port}"),
        Err(_) => "[::]:8080".to_string(),
    })
}

/// INDEXER_NETWORK, validated to a safe path segment (it is interpolated
/// into request paths).
fn indexer_network(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(v) = raw else {
        return Ok(None);
    };
    let v = v.trim().to_lowercase();
    if v.is_empty() {
        return Ok(None);
    }
    anyhow::ensure!(
        v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
        "INDEXER_NETWORK must be alphanumeric/hyphen, got '{v}'"
    );
    Ok(Some(v))
}

/// A required environment variable.
pub fn required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var {key}"))
}

/// An environment variable with a default.
pub fn optional(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// An optional program id from `key`: unset or blank means "not deployed
/// here". A set value must be the 64-hex on-chain id, lowercased — a base58 or
/// truncated id would match no transaction and fail silently, so it is
/// refused up front.
pub fn optional_program_id(key: &str) -> anyhow::Result<Option<String>> {
    parse_program_id(key, std::env::var(key).ok())
}

/// A program id that must be set; see [`optional_program_id`].
pub fn required_program_id(key: &str) -> anyhow::Result<String> {
    optional_program_id(key)?.with_context(|| format!("missing required env var {key}"))
}

fn parse_program_id(key: &str, raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let v = raw.trim().to_lowercase();
    if v.is_empty() {
        return Ok(None);
    }
    anyhow::ensure!(
        v.len() == 64 && v.chars().all(|c| c.is_ascii_hexdigit()),
        "{key} must be the 64-hex program id (got {raw:?})"
    );
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_ids_are_normalized_and_malformed_ones_refused() {
        let id = "AB".repeat(32);
        assert_eq!(
            parse_program_id("K", Some(format!(" {id}\n"))).unwrap(),
            Some("ab".repeat(32))
        );
        assert_eq!(parse_program_id("K", None).unwrap(), None);
        assert_eq!(parse_program_id("K", Some("  ".into())).unwrap(), None);
        assert!(parse_program_id("K", Some("not-a-program".into())).is_err());
        assert!(parse_program_id("K", Some("gg".repeat(32))).is_err());
    }

    #[test]
    fn the_network_is_a_lowercased_path_segment() {
        assert_eq!(
            indexer_network(Some(" Mainnet ".into()))
                .unwrap()
                .as_deref(),
            Some("mainnet")
        );
        assert_eq!(indexer_network(Some(String::new())).unwrap(), None);
        assert_eq!(indexer_network(None).unwrap(), None);
        assert!(indexer_network(Some("main/net".into())).is_err());
    }
}

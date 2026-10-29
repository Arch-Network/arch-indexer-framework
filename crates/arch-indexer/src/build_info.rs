//! What the running binary is, so an incident can be traced to a build.
//! Exposed on `GET /v1/version`, as the `program_indexer_build_info` gauge and
//! as the `x-indexer-version` header on every response.
//!
//! Captured in the *binary* crate by [`crate::build_info!`], since this
//! crate's own version and commit say nothing about the service built on it.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// The binary's package name.
    pub service: &'static str,
    /// The binary's package version.
    pub version: &'static str,
    /// Short commit the binary was built from, or `unknown`.
    pub git_sha: String,
    pub built_at: Option<DateTime<Utc>>,
}

impl BuildInfo {
    /// `git_sha`: the first of `RAILWAY_GIT_COMMIT_SHA` (set by Railway on a
    /// GitHub-connected build) and `GIT_SHA` (a `--build-arg`, or emitted by
    /// the binary's build script with `cargo:rustc-env=GIT_SHA=…`) that is
    /// set, cut to 7 characters. `source_date_epoch`: `SOURCE_DATE_EPOCH`.
    pub fn new(
        service: &'static str,
        version: &'static str,
        git_shas: [Option<&'static str>; 2],
        source_date_epoch: Option<&'static str>,
    ) -> Self {
        let git_sha = git_shas
            .into_iter()
            .flatten()
            .map(str::trim)
            .find(|s| !s.is_empty() && *s != "unknown")
            .map_or_else(|| "unknown".to_string(), |s| s.chars().take(7).collect());
        let built_at = source_date_epoch
            .and_then(|s| s.trim().parse::<i64>().ok())
            .and_then(|s| DateTime::from_timestamp(s, 0));
        Self {
            service,
            version,
            git_sha,
            built_at,
        }
    }

    /// `0.2.0+ea07a48` — the one string to put in a log line or a bug report.
    pub fn describe(&self) -> String {
        format!("{}+{}", self.version, self.git_sha)
    }
}

/// The calling crate's [`BuildInfo`], read at its compile time.
#[macro_export]
macro_rules! build_info {
    () => {
        $crate::BuildInfo::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            [
                option_env!("RAILWAY_GIT_COMMIT_SHA"),
                option_env!("GIT_SHA"),
            ],
            option_env!("SOURCE_DATE_EPOCH"),
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_set_sha_wins_cut_to_seven() {
        let b = BuildInfo::new("svc", "0.1.0", [None, Some(" 0123456789abcdef ")], None);
        assert_eq!(b.git_sha, "0123456");
        let b = BuildInfo::new("svc", "0.1.0", [Some("fedcba987"), Some("0123456")], None);
        assert_eq!(b.git_sha, "fedcba9");
        let b = BuildInfo::new("svc", "0.1.0", [Some("unknown"), Some("")], None);
        assert_eq!(b.git_sha, "unknown");
    }

    #[test]
    fn describe_is_a_valid_header_value() {
        let b = crate::build_info!();
        let d = b.describe();
        assert_eq!(d, format!("{}+{}", b.version, b.git_sha));
        assert!(d.contains('+') && !d.contains(' '));
        assert!(d.chars().all(|c| c.is_ascii_graphic()));
        assert_eq!(
            BuildInfo::new("s", "1", [None, None], Some("1700000000")).built_at,
            DateTime::from_timestamp(1_700_000_000, 0)
        );
    }
}

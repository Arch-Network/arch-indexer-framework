//! The process around a set of indexers: migrations, the ingest tail, the
//! sampler, background tasks and the HTTP API, in one of several modes.

use crate::build_info::BuildInfo;
use crate::config::Config;
use crate::db::{self, PgPool};
use crate::indexer::{validate, ApiState, BoxFuture, Ctx, DynIndexer};
use crate::indexer_client::IndexerClient;
use crate::runner::Runner;
use crate::sampler::{Enricher, Sampler};
use anyhow::Context;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

/// What a process runs, from its first argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// No argument or `all`: ingest, sampler, background tasks and HTTP in
    /// one process.
    All,
    /// `api`: the HTTP API only, on the serving pool.
    Api,
    /// `ingest`: the tail, the sampler and background tasks, no HTTP.
    Ingest,
    /// `backfill`: one catch-up pass to the tip (or `INGEST_END_HEIGHT`),
    /// then exit. The same code path as the tail.
    Backfill,
    /// `delete-tx <txid>`: remove every row derived from a transaction
    /// rolled back after it was ingested (`store::delete_tx`), then exit.
    DeleteTx(String),
}

impl Mode {
    /// Parse the arguments after the program name.
    pub fn from_args(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut args = args.into_iter();
        let mode = match args.next().as_deref() {
            None | Some("all") => Self::All,
            Some("api") => Self::Api,
            Some("ingest") => Self::Ingest,
            Some("backfill") => Self::Backfill,
            Some("delete-tx") => {
                Self::DeleteTx(args.next().context("delete-tx needs the txid to delete")?)
            }
            Some(other) => anyhow::bail!(
                "unknown mode {other:?}: expected all, api, ingest, backfill or delete-tx <txid>"
            ),
        };
        anyhow::ensure!(args.next().is_none(), "unexpected extra arguments");
        Ok(mode)
    }
}

/// Run `indexers` as one service, in the mode named by the process's first
/// argument (see [`Mode`]). Initializes `tracing` from `RUST_LOG` (default
/// `info`). Returns when the mode completes, on SIGTERM/Ctrl-C (after HTTP
/// drains), or with an error when any long-running loop exits.
///
/// ```text
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     arch_program_indexer::run(
///         arch_program_indexer::build_info!(),
///         arch_program_indexer::Config::from_env()?,
///         vec![Box::new(MyIndexer::from_env()?)],
///     )
///     .await
/// }
/// ```
pub async fn run(
    build: BuildInfo,
    config: Config,
    indexers: Vec<Box<dyn DynIndexer>>,
) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let mode = Mode::from_args(std::env::args().skip(1))?;
    Host::new(build, config, indexers)?.run(mode).await
}

pub struct Host {
    pub build: BuildInfo,
    pub config: Config,
    pub indexers: Vec<Arc<dyn DynIndexer>>,
}

impl Host {
    /// Refuses indexers that cannot share a host (see [`validate`]).
    pub fn new(
        build: BuildInfo,
        config: Config,
        indexers: Vec<Box<dyn DynIndexer>>,
    ) -> anyhow::Result<Self> {
        let indexers: Vec<Arc<dyn DynIndexer>> = indexers.into_iter().map(Arc::from).collect();
        validate(&indexers)?;
        Ok(Self {
            build,
            config,
            indexers,
        })
    }

    pub fn client(&self) -> IndexerClient {
        IndexerClient::new(
            self.config.indexer_base_url.clone(),
            self.config.indexer_api_key.clone(),
            self.config.indexer_network.clone(),
        )
    }

    /// The runtime's migrations, then each indexer's under its own namespace.
    pub async fn migrate(&self, pool: &PgPool) -> anyhow::Result<()> {
        db::migrate(pool, db::RUNTIME_NAMESPACE, &db::RUNTIME_MIGRATOR).await?;
        for ix in &self.indexers {
            db::migrate(pool, ix.id(), ix.migrator())
                .await
                .with_context(|| format!("migrating indexer {}", ix.id()))?;
        }
        Ok(())
    }

    /// Every indexer's `prepare` hook, in order.
    pub async fn prepare(&self, ctx: &Ctx) -> anyhow::Result<()> {
        for ix in &self.indexers {
            ix.prepare(ctx)
                .await
                .with_context(|| format!("preparing indexer {}", ix.id()))?;
        }
        Ok(())
    }

    pub fn runner(&self, pool: PgPool) -> Runner {
        Runner {
            pool,
            client: self.client(),
            indexers: self.indexers.clone(),
            page_limit: self.config.export_page_limit,
            confirmation_depth: self.config.ingest_confirmation_depth,
            end_height: self.config.ingest_end_height,
        }
    }

    /// The HTTP API over `serving_pool` (use [`db::connect_serving`]).
    pub fn router(&self, serving_pool: PgPool) -> axum::Router {
        crate::http::router(
            ApiState {
                pool: serving_pool,
                client: self.client(),
            },
            &self.config,
            self.build.clone(),
            &self.indexers,
        )
    }

    pub async fn run(&self, mode: Mode) -> anyhow::Result<()> {
        let ids: Vec<String> = self
            .indexers
            .iter()
            .map(|ix| format!("{}@{}", ix.id(), ix.data_revision()))
            .collect();
        tracing::info!(
            service = self.build.service,
            version = %self.build.describe(),
            network = self.config.network(),
            indexers = ?ids,
            ?mode,
            "starting"
        );
        let pool = db::connect(&self.config.database_url).await?;
        self.migrate(&pool).await?;

        match mode {
            Mode::DeleteTx(txid) => {
                let deleted = crate::store::delete_tx(&pool, &self.indexers, &txid).await?;
                tracing::info!(txid, deleted, "transaction rows deleted");
                Ok(())
            }
            Mode::Backfill => {
                let runner = self.runner(pool);
                self.prepare(&runner.ctx()).await?;
                let written = runner.catch_up().await?;
                tracing::info!(rows = written, "backfill complete; running one enrich pass");
                Enricher {
                    ctx: runner.ctx(),
                    indexers: self.indexers.clone(),
                }
                .enrich_once()
                .await;
                Ok(())
            }
            Mode::Api => {
                // Migrations ran unbounded on `pool`; serving connections
                // carry the statement timeout.
                pool.close().await;
                self.supervise(Vec::new(), Some(self.serve().await?)).await
            }
            Mode::Ingest => {
                let tasks = self.ingest_tasks(pool).await?;
                self.supervise(tasks, None).await
            }
            Mode::All => {
                let http = self.serve().await?;
                let tasks = self.ingest_tasks(pool).await?;
                self.supervise(tasks, Some(http)).await
            }
        }
    }

    /// The tail, the sampler, the enricher and every indexer's background
    /// tasks.
    async fn ingest_tasks(
        &self,
        pool: PgPool,
    ) -> anyhow::Result<Vec<(&'static str, BoxFuture<'static, anyhow::Result<()>>)>> {
        let runner = self.runner(pool);
        let ctx = runner.ctx();
        self.prepare(&ctx).await?;
        let mut tasks: Vec<(&'static str, BoxFuture<'static, anyhow::Result<()>>)> = Vec::new();
        let sampler = Sampler {
            ctx: ctx.clone(),
            indexers: self.indexers.clone(),
        };
        let sample_interval = Duration::from_secs(self.config.sample_interval_secs);
        tasks.push((
            "sampler",
            Box::pin(async move { sampler.run_forever(sample_interval).await }),
        ));
        let enricher = Enricher {
            ctx: ctx.clone(),
            indexers: self.indexers.clone(),
        };
        let enrich_interval = Duration::from_secs(self.config.enrich_interval_secs);
        tasks.push((
            "enricher",
            Box::pin(async move { enricher.run_forever(enrich_interval).await }),
        ));
        for ix in &self.indexers {
            for task in ix.background(&ctx) {
                tasks.push((task.name, task.future));
            }
        }
        let tail_interval = Duration::from_secs(self.config.tail_interval_secs);
        tasks.push((
            "ingest",
            Box::pin(async move { runner.run_forever(tail_interval).await }),
        ));
        Ok(tasks)
    }

    /// Bind the API and return the server future, which drains in-flight
    /// requests once `stop` fires.
    async fn serve(&self) -> anyhow::Result<Server> {
        let serving = db::connect_serving(&self.config.database_url).await?;
        let app = self.router(serving);
        let addr = &self.config.api_bind_addr;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding {addr} (API_BIND_ADDR / PORT)"))?;
        tracing::info!("listening on {addr}");
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let future = Box::pin(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopped.wait_for(|s| *s).await;
                })
                .await?;
            Ok(())
        });
        Ok(Server { stop, future })
    }

    /// Run every task and the server until one exits (an error: none should)
    /// or a shutdown signal arrives (the server drains, then Ok). Exiting on
    /// any loop's death lets the platform restart the process — safe because
    /// ingest is idempotent and checkpointed, and a silently dead sampler is
    /// worse than a restart.
    async fn supervise(
        &self,
        tasks: Vec<(&'static str, BoxFuture<'static, anyhow::Result<()>>)>,
        server: Option<Server>,
    ) -> anyhow::Result<()> {
        let mut set = JoinSet::new();
        for (name, task) in tasks {
            set.spawn(async move { (name, task.await) });
        }
        let (stop, mut http) = match server {
            Some(s) => (Some(s.stop), Some(tokio::spawn(s.future))),
            None => (None, None),
        };
        let failure = tokio::select! {
            Some(joined) = set.join_next() => Some(match joined {
                Ok((name, result)) => anyhow::anyhow!("{name} exited: {result:?}"),
                Err(err) => anyhow::anyhow!("a task panicked: {err}"),
            }),
            result = async {
                match http.as_mut() {
                    Some(handle) => handle.await,
                    None => std::future::pending().await,
                }
            } => Some(anyhow::anyhow!("http server exited: {result:?}")),
            () = shutdown_signal() => None,
        };
        if let Some(err) = failure {
            tracing::error!(%err, "shutting down");
            return Err(err);
        }
        if let (Some(stop), Some(http)) = (stop, http) {
            let _ = stop.send(true);
            http.await??;
        }
        Ok(())
    }
}

struct Server {
    stop: tokio::sync::watch::Sender<bool>,
    future: BoxFuture<'static, anyhow::Result<()>>,
}

/// Resolve on SIGTERM (deploy/rollout) or Ctrl-C.
async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(err) => tracing::error!(%err, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received; draining connections");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mode(args: &[&str]) -> anyhow::Result<Mode> {
        Mode::from_args(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn modes_parse_from_the_first_argument() {
        assert_eq!(mode(&[]).unwrap(), Mode::All);
        assert_eq!(mode(&["all"]).unwrap(), Mode::All);
        assert_eq!(mode(&["api"]).unwrap(), Mode::Api);
        assert_eq!(mode(&["ingest"]).unwrap(), Mode::Ingest);
        assert_eq!(mode(&["backfill"]).unwrap(), Mode::Backfill);
        assert_eq!(
            mode(&["delete-tx", "ab"]).unwrap(),
            Mode::DeleteTx("ab".into())
        );
        assert!(mode(&["delete-tx"]).is_err());
        assert!(mode(&["serve"]).is_err());
        assert!(mode(&["api", "extra"]).is_err());
    }
}

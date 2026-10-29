//! Framework for Arch program indexers over arch-rust-indexer's program export.
//!
//! A program implements [`ProgramIndexer`] (decoder, persistence, schema,
//! sampling, price derivation, enrichment, routes, metrics, day-export
//! facts); the runtime owns everything around it: paging the export from
//! per-cursor checkpoints, confirmation gating, replays gated by the
//! indexer's data revision, scan marks, mint decimals, unknown-instruction
//! markers, the price spine and its feeds ([`prices`]), the sampler and
//! enricher loops, namespaced migrations and the HTTP scaffolding. A service
//! binary is one call:
//!
//! ```text
//! arch_indexer::run(
//!     arch_indexer::build_info!(),
//!     arch_indexer::Config::from_env()?,
//!     vec![Box::new(MyIndexer::from_env()?)],
//! )
//! .await
//! ```
//!
//! The decoder-side types live in [`arch_indexer_core`], re-exported here, so
//! a program's decoder can depend on that crate alone.

pub mod build_info;
pub mod checkpoints;
pub mod config;
pub mod db;
pub mod enrich;
pub mod explorer;
pub mod export;
pub mod host;
pub mod http;
pub mod indexer;
pub mod portfolio;
pub mod prices;
pub mod runner;
pub mod sampler;
pub mod store;
pub mod twa;

pub use arch_indexer_core;
pub use arch_indexer_core::{
    anchor_discriminator, event, event_discriminator, input, ByteReader, IndexedTx, IndexerEvent,
    InstructionNode, TokenTransfer, UnknownInstruction,
};
pub use build_info::BuildInfo;
pub use config::Config;
pub use db::{PgPool, PgTransaction};
pub use explorer::ExplorerClient;
pub use host::{run, Host, Mode};
pub use indexer::{ApiState, BackgroundTask, BoxFuture, Ctx, DynIndexer, ProgramIndexer, Routes};
pub use prices::PriceCtx;

/// The versions the trait's signatures are written against; a program crate
/// can name them through these rather than pinning its own.
pub use axum;
pub use sqlx;

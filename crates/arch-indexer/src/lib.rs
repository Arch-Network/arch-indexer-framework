//! Runtime for per-program indexers over arch-rust-indexer's program export.
//!
//! A program implements [`ProgramIndexer`] (decoder, persistence, schema,
//! sampling, routes, metrics); the runtime owns everything around it: paging
//! the export from per-cursor checkpoints, confirmation gating, replays gated
//! by the indexer's data revision, scan marks, mint decimals, unknown-
//! instruction markers, the sampler loop, namespaced migrations and the HTTP
//! scaffolding. A service binary is one call:
//!
//! ```text
//! arch_program_indexer::run(
//!     arch_program_indexer::build_info!(),
//!     arch_program_indexer::Config::from_env()?,
//!     vec![Box::new(MyIndexer::from_env()?)],
//! )
//! .await
//! ```

pub mod build_info;
pub mod checkpoints;
pub mod config;
pub mod db;
pub mod event;
pub mod host;
pub mod http;
pub mod indexer;
pub mod indexer_client;
pub mod input;
pub mod runner;
pub mod sampler;
pub mod store;
pub mod twa;

pub use build_info::BuildInfo;
pub use config::Config;
pub use db::{PgPool, PgTransaction};
pub use event::{IndexerEvent, UnknownInstruction};
pub use host::{run, Host, Mode};
pub use indexer::{ApiState, BackgroundTask, BoxFuture, Ctx, DynIndexer, ProgramIndexer, Routes};
pub use indexer_client::IndexerClient;
pub use input::{ByteReader, IndexedTx, InstructionNode, TokenTransfer};

/// The versions the trait's signatures are written against; a program crate
/// can name them through these rather than pinning its own.
pub use axum;
pub use sqlx;

/// Anchor-style 8-byte instruction discriminator: sha256("global:<name>")[..8].
/// Computed, not hardcoded — verify against golden fixtures before trusting.
pub fn anchor_discriminator(name: &str) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(format!("global:{name}").as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

/// Anchor-style 8-byte event discriminator: sha256("event:<Name>")[..8]. The
/// event's Rust type name is part of the wire format.
pub fn event_discriminator(name: &str) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(format!("event:{name}").as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

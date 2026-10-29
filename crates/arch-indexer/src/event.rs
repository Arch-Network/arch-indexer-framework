use chrono::{DateTime, Utc};

/// What a [`crate::ProgramIndexer`]'s decoder emits. A program defines one
/// event type (usually an enum over its rows) and implements this for it.
pub trait IndexerEvent: Send + Sync + 'static {
    /// Label the runner counts the event under in its per-pass log line
    /// (`by_kind`), e.g. `"swap"` or `"lending_deposit"`.
    fn kind(&self) -> &'static str;

    /// The marker inside the event, when it is one. The runner writes markers
    /// to `decoder_unknowns` itself and never hands them to `persist`.
    fn as_unknown(&self) -> Option<&UnknownInstruction> {
        None
    }
}

/// An instruction or event a decoder recognized as its program's but had no
/// decode path for. Dropping it silently would hide a program upgrade that
/// outran the decoder, so the decoder emits this marker and the runner
/// persists it to `decoder_unknowns`, in the page's transaction.
#[derive(Debug, Clone)]
pub struct UnknownInstruction {
    /// The emitting indexer's [`crate::ProgramIndexer::id`]; `/metrics` and
    /// the per-host views select markers by it.
    pub decoder: &'static str,
    /// What was not understood, in the decoder's own vocabulary (a hex
    /// discriminator, `no-event:<ix>`, `event:<disc>:v<n>`, …).
    pub discriminator: String,
    pub txid: String,
    pub ix_path: String,
    pub block_height: i64,
    pub ts: DateTime<Utc>,
}

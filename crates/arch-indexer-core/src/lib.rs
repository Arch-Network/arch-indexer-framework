//! What a program decoder needs and nothing else: the transaction shape
//! arch-rust-indexer exports ([`IndexedTx`]), log-line attribution with the
//! node's 255-byte line limit ([`input::ProgramLogs`]), a little-endian
//! [`ByteReader`], strict base64, Anchor discriminators, and the event
//! contract ([`IndexerEvent`], [`UnknownInstruction`]).
//!
//! No database, HTTP or async runtime: a decoder built on this crate alone can
//! run in any host — the `arch-indexer` runtime, or a process that receives
//! transactions some other way.

pub mod event;
pub mod input;

pub use event::{IndexerEvent, UnknownInstruction};
pub use input::{
    base64_decode, ByteReader, IndexedTx, InstructionNode, ProgramData, ProgramLogs,
    ProgramMessage, TokenTransfer, TxStatus, LOG_LINE_LIMIT,
};

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The Anchor convention: the first 8 bytes of sha256 over the namespaced name.
    #[test]
    fn discriminators_are_the_namespaced_sha256_prefix() {
        use sha2::{Digest, Sha256};
        assert_eq!(
            anchor_discriminator("swap")[..],
            Sha256::digest(b"global:swap")[..8]
        );
        assert_eq!(
            event_discriminator("SwapExecuted")[..],
            Sha256::digest(b"event:SwapExecuted")[..8]
        );
        assert_ne!(anchor_discriminator("swap"), event_discriminator("swap"));
    }
}

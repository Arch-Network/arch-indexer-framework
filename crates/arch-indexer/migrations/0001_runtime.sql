-- The runtime's shared tables, in the `runtime` migration namespace. Every
-- program indexer's own migrations run after these, so they may insert
-- `replay_requests` rows.
--
-- IF NOT EXISTS throughout: the definitions are column-for-column those of
-- arch-prime-analytics (migrations 0001, 0002, 0008, 0027, 0029), so a
-- database those migrations built is adopted as it stands.

-- Per-cursor ingestion progress: one row per (indexer, program) checkpoint key.
CREATE TABLE IF NOT EXISTS checkpoints (
    name        text PRIMARY KEY,
    block_height bigint NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Mint decimals observed on token transfers during ingestion.
CREATE TABLE IF NOT EXISTS mint_decimals (
    mint     text PRIMARY KEY,
    decimals int  NOT NULL
);

-- Instructions or events a decoder recognized as its program's but had no
-- decode path for. Decoders stay pure: they emit an `UnknownInstruction`
-- marker and the runner writes it here, in the page's transaction.
-- Idempotent on (txid, ix_path), like every event table.
CREATE TABLE IF NOT EXISTS decoder_unknowns (
    decoder       text        NOT NULL,
    discriminator text        NOT NULL,
    txid          text        NOT NULL,
    ix_path       text        NOT NULL,
    block_height  bigint      NOT NULL,
    ts            timestamptz NOT NULL,
    PRIMARY KEY (txid, ix_path)
);
-- /metrics groups by (decoder, discriminator); the distinct set is tiny.
CREATE INDEX IF NOT EXISTS idx_decoder_unknowns_disc ON decoder_unknowns (decoder, discriminator);

-- A checkpoint whose history must be decoded again by a build whose indexer
-- reports `data_revision` or later. The previous build, still running through
-- a deploy, never acts on it: a runner whose indexer qualifies walks from
-- genesis while the request stands, keeps its progress in through_height,
-- resumes there after a failed page, and deletes the request once a pass
-- reaches the tip. A later replay raises a pending request's revision and
-- starts it over rather than adding a row:
--   INSERT INTO replay_requests (checkpoint, data_revision) VALUES ('<key>', <rev>)
--   ON CONFLICT (checkpoint) DO UPDATE
--     SET data_revision = GREATEST(replay_requests.data_revision, EXCLUDED.data_revision),
--         through_height = 0;
CREATE TABLE IF NOT EXISTS replay_requests (
    checkpoint text PRIMARY KEY,
    data_revision int NOT NULL,
    through_height bigint NOT NULL DEFAULT 0
);

-- How far each checkpoint has provably scanned the chain: the indexer block
-- that was the safe tip when a catch-up pass began, recorded once that pass
-- reached the program's last page and committed. checkpoint_height is the
-- checkpoint at the time of the mark, so a checkpoint rewound since
-- invalidates the mark.
CREATE TABLE IF NOT EXISTS scan_marks (
    name              text PRIMARY KEY,          -- a checkpoints.name
    checkpoint_height bigint NOT NULL,
    tip_height        bigint NOT NULL,
    tip_ts            timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

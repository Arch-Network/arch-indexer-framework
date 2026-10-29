-- The price spine: one USD close per (mint, interval, bucket). The runtime
-- writes the feed-priced mints (`PRICE_FEEDS`: Coinbase hourly candles and
-- constants, see `arch_indexer::prices`); each indexer's `derive_prices`
-- writes the mints it prices from its own state (`pool:<address>`,
-- `vault:<address>`), and every indexer reads it to stamp USD.
--
-- Column-for-column arch-prime-analytics' `price_candles` (migration 0001),
-- with IF NOT EXISTS, so a database analytics built, or one a program
-- crate's baseline created first, is adopted as it stands.
CREATE TABLE IF NOT EXISTS price_candles (
    mint       text        NOT NULL,
    "interval" text        NOT NULL,
    bucket_ts  timestamptz NOT NULL,
    close_usd  numeric     NOT NULL,
    -- 'coinbase' | 'const' | 'pool:<address>' | 'vault:<address>'; 'pyth'
    -- rows are history from before that feed was retired (2026-08-26).
    source     text        NOT NULL,
    PRIMARY KEY (mint, "interval", bucket_ts)
);

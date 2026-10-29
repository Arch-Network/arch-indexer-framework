CREATE TABLE example_transfers (
    txid         text        NOT NULL,
    ix_path      text        NOT NULL,
    program_id   text        NOT NULL,
    amount       bigint      NOT NULL,
    memo         text,
    block_height bigint      NOT NULL,
    ts           timestamptz NOT NULL,
    PRIMARY KEY (txid, ix_path)
);

CREATE TABLE example_samples (
    ts    timestamptz NOT NULL DEFAULT now(),
    total bigint      NOT NULL
);

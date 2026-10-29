//! HTTP client for arch-rust-indexer. The ONLY place that speaks the indexer's
//! wire format — everything past this module sees [`crate::input`] shapes.
//!
//! Depends on the program-scoped bulk-export endpoint:
//!   GET /api/v1/programs/{id}/transactions/export
//!       ?from_height=&cursor=&limit=  → { transactions: [IndexedTx], next_cursor }
//! An indexer without it answers 404; the tail logs and retries.

use crate::input::IndexedTx;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use std::time::Duration;

const ERROR_BODY_BYTES: usize = 200;

#[derive(Clone)]
pub struct ExplorerClient {
    /// Sends `X-API-Key` on every request when a key is configured: the
    /// explorer charges an unkeyed request, REST included, to its per-IP
    /// anonymous limit. Follows no redirect, because reqwest strips only its
    /// standard credential headers on a cross-host hop and would forward the
    /// key.
    http: reqwest::Client,
    /// The chain RPC: `read_account_data`'s fallback and
    /// `program_accounts_by_prefix`'s filtered read. Another host, so it
    /// never carries the indexer key.
    chain_rpc: reqwest::Client,
    /// `{base_url}/api/v1` or `{base_url}/api/v1/{network}` — every indexer
    /// REST path hangs off this.
    api_root: String,
}

#[derive(Debug, Deserialize)]
pub struct DeltasPage {
    pub deltas: Vec<TokenDelta>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TokenDelta {
    pub txid: String,
    pub block_height: i64,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub mint: String,
    pub decimals: i32,
    /// Signed atoms as decimal string.
    pub delta: String,
}

#[derive(Debug, Deserialize)]
pub struct Block {
    pub height: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct ExportPage {
    pub transactions: Vec<IndexedTx>,
    pub next_cursor: Option<String>,
    /// Height through which this page is complete — safe checkpoint value.
    pub complete_through_height: i64,
}

/// One token account holding a mint. Addresses are lowercase hex.
#[derive(Debug, PartialEq)]
pub struct TokenHolder {
    pub account: String,
    pub owner: Option<String>,
    pub balance: u64,
}

/// `token_holders` reads at most this many pages of 200.
const MAX_HOLDER_PAGES: u32 = 50;

/// One movement of a mint's tokens that took effect. Addresses are lowercase
/// hex; no `source` is a mint, no `destination` a burn.
#[derive(Debug, Clone, PartialEq)]
pub struct ShareTransfer {
    pub txid: String,
    /// `<outer>`, or `<outer>.<n>` for the n-th inner instruction under it.
    pub ix_path: String,
    pub block_height: i64,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub amount: u64,
}

/// `token_transfers` reads at most this many pages of 200, and warns past
/// four fifths of them.
const MAX_TRANSFER_PAGES: u32 = 500;

impl ExplorerClient {
    /// `network`: None targets the indexer's default (un-prefixed) routes —
    /// today's testnet deployment. Some("mainnet") targets the network-scoped
    /// `/api/v1/{network}/…` variants the indexer serves for every endpoint
    /// used here (export, token-balances, deltas, rpc, blocks).
    pub fn new(base_url: String, api_key: Option<String>, network: Option<String>) -> Self {
        // The connect timeout guards against a black-holed connection freezing
        // ingestion forever; the total timeout is a mid-stream-stall backstop,
        // generous enough for a full export page (lower page_limit if it bites).
        let builder = || {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(60))
        };
        let mut headers = HeaderMap::new();
        if let Some(key) = api_key.as_deref().map(str::trim).filter(|k| !k.is_empty()) {
            let mut key = HeaderValue::from_str(key)
                .expect("INDEXER_API_KEY must be a valid HTTP header value");
            key.set_sensitive(true);
            headers.insert("x-api-key", key);
        }
        let http = builder()
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers)
            .build()
            .expect("build indexer http client");
        let chain_rpc = builder().build().expect("build chain rpc http client");
        let base_url = base_url.trim_end_matches('/').to_string();
        let api_root = match network.as_deref().filter(|n| !n.is_empty()) {
            Some(net) => format!("{base_url}/api/v1/{net}"),
            None => format!("{base_url}/api/v1"),
        };
        Self {
            http,
            chain_rpc,
            api_root,
        }
    }

    pub async fn export_page(
        &self,
        program_id: &str,
        from_height: i64,
        cursor: Option<&str>,
        limit: u32,
    ) -> anyhow::Result<ExportPage> {
        let mut req = self
            .http
            .get(format!(
                "{}/programs/{}/transactions/export",
                self.api_root, program_id
            ))
            .query(&[
                ("from_height", from_height.to_string()),
                ("limit", limit.to_string()),
            ]);
        if let Some(c) = cursor {
            req = req.query(&[("cursor", c)]);
        }
        let resp = ensure_success(req.send().await?).await?;
        Ok(resp.json().await?)
    }

    /// The runtime log lines of one transaction, from the indexer's
    /// `GET /transactions/{txid}` (`transactions.logs`, public). The
    /// bridge for an export that predates the `logs` field: a decoder that
    /// `needs_logs` gets them per transaction until the export carries them.
    /// `None` when the indexer has no row (not an error; the runner records
    /// the gap). A transport failure is an error so the page is retried.
    pub async fn transaction_logs(&self, txid: &str) -> anyhow::Result<Option<Vec<String>>> {
        #[derive(Deserialize)]
        struct Resp {
            #[serde(default)]
            logs: Vec<String>,
        }
        let resp = self
            .http
            .get(format!("{}/transactions/{}", self.api_root, txid))
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp: Resp = ensure_success(resp).await?.json().await?;
        Ok(Some(resp.logs))
    }

    /// Atoms held by a single token account (e.g. an AMM vault), as a decimal
    /// string. None when the indexer has no row for the account yet.
    /// Endpoint matches arch-swap's vault reads: /accounts/{addr}/token-balances.
    pub async fn token_account_balance(&self, account: &str) -> anyhow::Result<Option<String>> {
        #[derive(Deserialize)]
        struct Entry {
            balance: String,
        }
        #[derive(Deserialize)]
        struct Resp {
            balances: Vec<Entry>,
        }
        let req = self.http.get(format!(
            "{}/accounts/{}/token-balances",
            self.api_root, account
        ));
        let resp: Resp = ensure_success(req.send().await?).await?.json().await?;
        Ok(resp.balances.into_iter().next().map(|e| e.balance))
    }

    /// Every token account holding `mint_hex`, from the indexer's
    /// `/tokens/{mint}/holders` (token balances read from account state),
    /// paged 200 at a time until `has_more` is false. More than 10 000
    /// holders is an error, not a truncated list.
    ///
    /// ponytail: offset paging over a list ordered by balance can skip or
    /// repeat a row when balances move between pages; the caller's supply
    /// check refuses such a list. A keyset or as-of holders endpoint in the
    /// indexer would make one read consistent.
    pub async fn token_holders(&self, mint_hex: &str) -> anyhow::Result<Vec<TokenHolder>> {
        #[derive(Deserialize)]
        struct Row {
            account_address: String,
            owner_address: Option<String>,
            balance: String,
        }
        #[derive(Deserialize)]
        struct Page {
            results: Vec<Row>,
            has_more: bool,
        }
        let mut holders = Vec::new();
        for page in 1..=MAX_HOLDER_PAGES {
            let req = self
                .http
                .get(format!("{}/tokens/{}/holders", self.api_root, mint_hex))
                .query(&[("limit", 200), ("page", page)]);
            let resp: Page = ensure_success(req.send().await?).await?.json().await?;
            for row in resp.results {
                holders.push(TokenHolder {
                    account: base58_to_hex(&row.account_address)?,
                    owner: row
                        .owner_address
                        .as_deref()
                        .map(base58_to_hex)
                        .transpose()?,
                    balance: row.balance.parse().map_err(|_| {
                        anyhow::anyhow!(
                            "holder {} balance {:?} is not a u64",
                            row.account_address,
                            row.balance
                        )
                    })?,
                });
            }
            if !resp.has_more {
                return Ok(holders);
            }
        }
        anyhow::bail!("token {mint_hex} has more than 10000 holders")
    }

    /// Every movement of `mint_hex` the indexer recorded (`token_transfers`:
    /// Transfer, MintTo, Burn and their checked forms, inner instructions
    /// included), block-ascending, paged 200 at a time until `has_more` is
    /// false. A failed or rolled-back transaction's rows are left out; a row
    /// with no block cannot be placed in time, so it is an error.
    ///
    /// ponytail: the whole list on every read, a request per 200 rows, and
    /// offset-paged with a tie-break that stops at the outer instruction, so a
    /// transaction's CPI transfers straddling a page boundary can swap places
    /// (the caller's reconciliation refuses a list that lost one). A keyset,
    /// mint-scoped export with a completeness height in the indexer, read from
    /// the last stored block, removes all three.
    pub async fn token_transfers(&self, mint_hex: &str) -> anyhow::Result<Vec<ShareTransfer>> {
        #[derive(Deserialize)]
        struct Row {
            txid: String,
            outer_index: i32,
            inner_index: Option<i32>,
            source_account_hex: Option<String>,
            destination_account_hex: Option<String>,
            amount: String,
            block_height: Option<i64>,
            is_failed: bool,
        }
        #[derive(Deserialize)]
        struct Page {
            results: Vec<Row>,
            has_more: bool,
        }
        let mut transfers = Vec::new();
        for page in 1..=MAX_TRANSFER_PAGES {
            let req = self
                .http
                .get(format!("{}/tokens/{}/transfers", self.api_root, mint_hex))
                .query(&[("sort", "block"), ("dir", "asc")])
                .query(&[("limit", 200), ("page", page)]);
            let resp: Page = ensure_success(req.send().await?).await?.json().await?;
            for row in resp.results.into_iter().filter(|r| !r.is_failed) {
                let ix_path = match row.inner_index {
                    Some(n) => format!("{}.{n}", row.outer_index),
                    None => row.outer_index.to_string(),
                };
                let block_height = row.block_height.ok_or_else(|| {
                    anyhow::anyhow!("transfer {} {ix_path} has no block", row.txid)
                })?;
                let amount = row.amount.parse().map_err(|_| {
                    anyhow::anyhow!("transfer {} amount {:?} is not a u64", row.txid, row.amount)
                })?;
                transfers.push(ShareTransfer {
                    txid: row.txid,
                    ix_path,
                    block_height,
                    source: row.source_account_hex.map(|a| a.to_lowercase()),
                    destination: row.destination_account_hex.map(|a| a.to_lowercase()),
                    amount,
                });
            }
            if !resp.has_more {
                return Ok(transfers);
            }
            if page == MAX_TRANSFER_PAGES * 4 / 5 {
                tracing::warn!(
                    mint = mint_hex,
                    page,
                    "token transfer list nears the read cap"
                );
            }
        }
        anyhow::bail!("token {mint_hex} has more than 100000 transfers")
    }

    /// Raw account bytes for `pubkey_hex`. Tries the indexer's JSON-RPC
    /// `read_account_info` first; if the indexer has no data for the account
    /// (its `accounts` table is sparse for program accounts), falls back to the
    /// chain RPC at `ARCH_RPC_URL` when set. None when neither has it. Used to
    /// sample on-chain program-account state (e.g. a CLAMM pool's liquidity).
    pub async fn read_account_data(&self, pubkey_hex: &str) -> anyhow::Result<Option<Vec<u8>>> {
        // `result` is `{ data: [..], .. }` or nests under `result.account`;
        // `data` is a JSON array of bytes.
        #[derive(Deserialize)]
        struct Inner {
            data: Option<Vec<u8>>,
        }
        #[derive(Deserialize)]
        struct AccountResult {
            data: Option<Vec<u8>>,
            account: Option<Inner>,
        }
        #[derive(Deserialize)]
        struct RpcResp {
            result: Option<AccountResult>,
        }
        fn extract(resp: RpcResp) -> Option<Vec<u8>> {
            resp.result
                .and_then(|r| r.data.or_else(|| r.account.and_then(|a| a.data)))
                .filter(|d| !d.is_empty())
        }

        // 1) Indexer RPC proxy. Params is a positional array (the indexer uses
        //    positional-or-named, not a bare value). pubkey_hex is plain hex.
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"read_account_info","params":["{pubkey_hex}"]}}"#
        );
        let req = self
            .http
            .post(format!("{}/rpc", self.api_root))
            .header("content-type", "application/json")
            .body(body);
        // Best-effort: a failure here (e.g. the indexer build returns 501 for
        // read_account_info) falls through to the chain RPC, not an error.
        if let Ok(r) = req.send().await.and_then(|r| r.error_for_status()) {
            if let Ok(resp) = r.json::<RpcResp>().await {
                if let Some(bytes) = extract(resp) {
                    return Ok(Some(bytes));
                }
            }
        }

        // 2) Chain RPC fallback. The validator is lenient about the pubkey form;
        //    try the byte-array then the hex-string (mirrors derive_clamm_pool_seed.py).
        let Some(rpc_url) = chain_rpc_url() else {
            return Ok(None);
        };
        let byte_csv: String = (0..pubkey_hex.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(pubkey_hex.get(i..i + 2)?, 16).ok())
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(",");
        for params in [format!("[{byte_csv}]"), format!("\"{pubkey_hex}\"")] {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"read_account_info","params":{params}}}"#
            );
            let sent = self
                .chain_rpc
                .post(&rpc_url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .and_then(|r| r.error_for_status());
            if let Ok(r) = sent {
                if let Ok(resp) = r.json::<RpcResp>().await {
                    if let Some(bytes) = extract(resp) {
                        return Ok(Some(bytes));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Accounts owned by `program_id_hex`, as (pubkey hex, data bytes).
    /// `Some(prefix)`: only accounts whose data begins with `prefix`
    /// (offset-0 DataContent filter) — the vault holder sample's read of a
    /// program's redemption entries (hourly per vault, and only while its
    /// escrow holds shares). Read from the chain RPC at `ARCH_RPC_URL` when
    /// set, else from the indexer's JSON-RPC proxy. The indexer answers from
    /// its accounts table, which is sparse for program accounts, and an
    /// account it lacks makes the list shorter, not an error: a redeemer's
    /// escrowed shares would read as a donation. So a chain failure is an
    /// error, never a retry on the indexer.
    /// `None`: every account of the program (the global loan-book scan) — an
    /// explicit variant, not an empty slice, so a caller cannot widen a
    /// filtered read into a whole-program scan by accident. Always the
    /// indexer, because a whole-program scan is one the validator should not
    /// absorb.
    pub async fn program_accounts_by_prefix(
        &self,
        program_id_hex: &str,
        prefix: Option<&[u8]>,
    ) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
        let filters = gpa_filters(prefix);
        if let (Some(_), Some(rpc_url)) = (prefix, chain_rpc_url()) {
            // The chain RPC refuses a hex program id; it takes the byte array.
            let program_id = hex::decode(program_id_hex)?;
            anyhow::ensure!(
                program_id.len() == 32,
                "program id {program_id_hex} is not 32 bytes"
            );
            let body = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "get_program_accounts",
                "params": [program_id, filters],
            });
            return program_accounts(self.chain_rpc.post(&rpc_url).json(&body)).await;
        }
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"get_program_accounts","params":["{program_id_hex}",{filters}]}}"#
        );
        let req = self
            .http
            .post(format!("{}/rpc", self.api_root))
            .header("content-type", "application/json")
            .body(body);
        program_accounts(req).await
    }

    /// All token balances held by an owner address: (mint_hex, atoms, decimals),
    /// plus whether the page was truncated.
    ///
    /// The bool is returned rather than only logged because the wallet leg is
    /// anchored on these balances: a truncated anchor silently understates the
    /// portfolio, and a consumer must be able to tell that from a correct one.
    pub async fn owner_token_balances(
        &self,
        owner: &str,
    ) -> anyhow::Result<(Vec<(String, String, i32)>, bool)> {
        #[derive(Deserialize)]
        struct Entry {
            mint_address_hex: String,
            balance: String,
            decimals: i32,
        }
        #[derive(Deserialize)]
        struct Resp {
            balances: Vec<Entry>,
            #[serde(default)]
            has_more: Option<bool>,
        }
        let req = self
            .http
            .get(format!(
                "{}/accounts/{}/token-balances",
                self.api_root, owner
            ))
            .query(&[("limit", "200")]);
        let resp: Resp = ensure_success(req.send().await?).await?.json().await?;
        let truncated = resp.has_more == Some(true);
        if truncated {
            // Truncated anchor: mints beyond the page reconstruct from history
            // alone (delta-walk from zero), which may misstate them.
            tracing::warn!(owner, "owner has >200 token balances; anchor truncated");
        }
        Ok((
            resp.balances
                .into_iter()
                .map(|e| (e.mint_address_hex.to_lowercase(), e.balance, e.decimals))
                .collect(),
            truncated,
        ))
    }

    /// One page of owner-resolved signed balance deltas, newest-first
    /// (the /token-transfers/deltas endpoint added for portfolio reconstruction).
    pub async fn token_deltas_page(
        &self,
        owner: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> anyhow::Result<DeltasPage> {
        let mut req = self
            .http
            .get(format!(
                "{}/accounts/{}/token-transfers/deltas",
                self.api_root, owner
            ))
            .query(&[("limit", limit.to_string())]);
        if let Some(c) = cursor {
            req = req.query(&[("cursor", c)]);
        }
        Ok(ensure_success(req.send().await?).await?.json().await?)
    }

    /// The chain tip as seen by the indexer, or with `at_or_below` the newest
    /// block at or below that height (`/blocks?before=` is exclusive).
    pub async fn latest_block(&self, at_or_below: Option<i64>) -> anyhow::Result<Block> {
        #[derive(Deserialize)]
        struct Page {
            blocks: Vec<Block>,
        }
        let mut req = self
            .http
            .get(format!("{}/blocks", self.api_root))
            .query(&[("limit", "1")]);
        if let Some(height) = at_or_below {
            req = req.query(&[("before", height + 1)]);
        }
        let page: Page = ensure_success(req.send().await?).await?.json().await?;
        let block = page
            .blocks
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("indexer returned no blocks"))?;
        // A scan mark claims everything before this block's time: an indexer
        // that ignored `before` would hand back its tip and overclaim.
        if let Some(height) = at_or_below {
            anyhow::ensure!(
                block.height <= height,
                "indexer returned block {} for at-or-below {height}",
                block.height
            );
        }
        Ok(block)
    }
}

/// `error_for_status`, keeping the start of the body: the explorer names the
/// refusing rule there (`rate_limit_exceeded`, `invalid_api_key`,
/// `rate_limit_unavailable`), which the status alone does not.
async fn ensure_success(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let url = resp.url().clone();
    let body = resp.bytes().await.unwrap_or_default();
    let body = String::from_utf8_lossy(&body[..body.len().min(ERROR_BODY_BYTES)]);
    anyhow::bail!("indexer returned {status} for {url}: {body}")
}

/// One get_program_accounts call. A transport failure, a non-2xx answer or
/// an rpc `error` is an error.
async fn program_accounts(req: reqwest::RequestBuilder) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    #[derive(Deserialize)]
    struct Account {
        data: Vec<u8>,
    }
    #[derive(Deserialize)]
    struct Entry {
        /// Validator-shaped: a u8 array (arch-swap `ProgramAccountEntry`).
        pubkey: Vec<u8>,
        account: Account,
    }
    #[derive(Deserialize)]
    struct RpcResp {
        result: Option<Vec<Entry>>,
        error: Option<serde_json::Value>,
    }
    let resp: RpcResp = ensure_success(req.send().await?).await?.json().await?;
    if let Some(err) = resp.error {
        anyhow::bail!("get_program_accounts rpc error: {err}");
    }
    Ok(resp
        .result
        .unwrap_or_default()
        .into_iter()
        .map(|e| (hex::encode(e.pubkey), e.account.data))
        .collect())
}

/// `ARCH_RPC_URL`, trimmed; unset or blank means no chain RPC.
fn chain_rpc_url() -> Option<String> {
    let url = std::env::var("ARCH_RPC_URL").ok()?;
    let url = url.trim().trim_end_matches('/');
    (!url.is_empty()).then(|| url.to_string())
}

/// A base58 address as the lowercase hex every table stores.
fn base58_to_hex(address: &str) -> anyhow::Result<String> {
    let bytes = bs58::decode(address).into_vec()?;
    anyhow::ensure!(bytes.len() == 32, "{address} is not a 32-byte address");
    Ok(hex::encode(bytes))
}

/// JSON filter list for get_program_accounts: an offset-0 DataContent prefix
/// match, or NO filter for `None`. A zero-length byte match is never sent —
/// the indexer's semantics for it are undefined, and "no filter" is the
/// unambiguous way to say "every account".
fn gpa_filters(prefix: Option<&[u8]>) -> serde_json::Value {
    match prefix {
        None => serde_json::json!([]),
        Some(p) => serde_json::json!([{
            "DataContent": { "offset": 0, "bytes": p }
        }]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpa_filters_omit_data_content_for_no_prefix() {
        // No prefix → no filters, never a zero-length DataContent match.
        assert_eq!(gpa_filters(None).to_string(), "[]");
        let f = gpa_filters(Some(&[0xAB, 0xCD]));
        assert_eq!(f[0]["DataContent"]["offset"], 0);
        assert_eq!(f[0]["DataContent"]["bytes"], serde_json::json!([171, 205]));
    }

    #[test]
    fn api_root_scopes_paths_by_network() {
        let plain = ExplorerClient::new("https://x.example/".into(), None, None);
        assert_eq!(plain.api_root, "https://x.example/api/v1");
        let scoped = ExplorerClient::new("https://x.example".into(), None, Some("mainnet".into()));
        assert_eq!(scoped.api_root, "https://x.example/api/v1/mainnet");
        // Empty network behaves like None (env set to "").
        let empty = ExplorerClient::new("https://x.example".into(), None, Some(String::new()));
        assert_eq!(empty.api_root, "https://x.example/api/v1");
    }

    /// Serves `requests` requests, one per connection, answering each with
    /// `respond(request)` (a 3xx sends its body as `Location`); yields every
    /// request, head and body, lower-cased.
    async fn fake_server(
        requests: usize,
        respond: fn(&str) -> (u16, String),
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut heads = Vec::new();
            for _ in 0..requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                // Read through the body: closing on unread bytes resets the
                // connection before the client reads the response.
                let mut request = Vec::new();
                let request = loop {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0, "connection closed mid-request");
                    request.extend_from_slice(&buf[..n]);
                    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let body_len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .map_or(0, |v| v.trim().parse().unwrap());
                    if request.len() >= end + 4 + body_len {
                        break String::from_utf8_lossy(&request).to_lowercase();
                    }
                };
                let (status, body) = respond(&request);
                let location = if (300..400).contains(&status) {
                    format!("Location: {body}\r\n")
                } else {
                    String::new()
                };
                let response = format!(
                    "HTTP/1.1 {status} Fake\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                heads.push(request);
            }
            heads
        });
        (url, server)
    }

    #[tokio::test]
    async fn a_configured_key_rides_on_rest_and_rpc_requests() {
        for (key, sent) in [(Some(" k-123\n"), true), (Some(""), false), (None, false)] {
            let (url, server) = fake_server(2, |head| {
                let body = if head.starts_with("post ") {
                    r#"{"jsonrpc":"2.0","id":1,"result":[]}"#
                } else {
                    r#"{"transactions":[],"next_cursor":null,"complete_through_height":7}"#
                };
                (200, body.into())
            })
            .await;
            let client = ExplorerClient::new(url, key.map(String::from), Some("mainnet".into()));
            client.export_page("ab", 0, None, 10).await.unwrap();
            client.program_accounts_by_prefix("ab", None).await.unwrap();
            let heads = server.await.unwrap();
            assert!(heads[0].starts_with("get /api/v1/mainnet/programs/ab/transactions/export?"));
            assert!(heads[1].starts_with("post /api/v1/mainnet/rpc "));
            for head in &heads {
                let keyed = head.lines().any(|l| l == "x-api-key: k-123");
                assert_eq!(keyed, sent, "{key:?}: {head}");
                assert_eq!(head.contains("x-api-key"), sent, "{key:?}: {head}");
            }
        }
    }

    /// One test for both chain reads: `ARCH_RPC_URL` is process-wide.
    #[tokio::test]
    async fn the_chain_rpc_fallback_never_carries_the_indexer_key() {
        let (url, server) = fake_server(4, |request| {
            let gpa = request.contains("get_program_accounts");
            if request.starts_with("post /api/v1/rpc ") && gpa {
                (
                    200,
                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601}}"#.into(),
                )
            } else if request.starts_with("post /api/v1/rpc ") {
                (501, r#"{"error":"not_implemented"}"#.into())
            } else if gpa {
                let pubkey = serde_json::json!([9u8; 32].to_vec());
                let body =
                    format!(r#"{{"result":[{{"pubkey":{pubkey},"account":{{"data":[5,6]}}}}]}}"#);
                (200, body)
            } else {
                (200, r#"{"result":{"data":[1,2]}}"#.into())
            }
        })
        .await;
        std::env::set_var("ARCH_RPC_URL", format!("{url}/chain"));
        let client = ExplorerClient::new(url, Some("k-123".into()), None);
        assert_eq!(
            client.read_account_data("ab").await.unwrap(),
            Some(vec![1, 2])
        );
        // A prefixed read goes straight to the chain, which the indexer's
        // possibly partial list cannot stand in for.
        let program = hex::encode([7u8; 32]);
        assert_eq!(
            client
                .program_accounts_by_prefix(&program, Some(&[1, 2]))
                .await
                .unwrap(),
            vec![(hex::encode([9u8; 32]), vec![5, 6])]
        );
        // A whole-program scan stays on the indexer, failure or not.
        let err = client
            .program_accounts_by_prefix(&program, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rpc error"), "{err}");
        let heads = server.await.unwrap();
        assert!(heads[0].starts_with("post /api/v1/rpc ") && heads[0].contains("x-api-key: k-123"));
        assert!(heads[1].starts_with("post /chain ") && !heads[1].contains("x-api-key"));
        assert!(heads[2].starts_with("post /chain ") && !heads[2].contains("x-api-key"));
        let params = format!(
            r#""params":[[{}],[{{"datacontent":{{"bytes":[1,2],"offset":0}}}}]]"#,
            ["7"; 32].join(",")
        );
        assert!(heads[2].contains(&params), "{}", heads[2]);
        assert!(heads[3].starts_with("post /api/v1/rpc ") && heads[3].contains(&program));
    }

    #[tokio::test]
    async fn token_holders_page_until_the_last_and_read_base58_as_hex() {
        let (url, server) = fake_server(2, |request| {
            let row = |account: u8, owner: Option<u8>, balance: &str| {
                serde_json::json!({
                    "account_address": bs58::encode([account; 32]).into_string(),
                    "balance": balance,
                    "decimals": 9,
                    "owner_address": owner.map(|o| bs58::encode([o; 32]).into_string()),
                })
            };
            let page = if request.contains("&page=1 ") {
                serde_json::json!({"has_more": true, "limit": 200, "page": 1, "total": null,
                    "results": [row(1, Some(2), "18446744073709551615")]})
            } else {
                serde_json::json!({"has_more": false, "limit": 200, "page": 2, "total": null,
                    "results": [row(3, None, "5")]})
            };
            (200, page.to_string())
        })
        .await;
        let client = ExplorerClient::new(url, None, Some("mainnet".into()));
        let holders = client.token_holders("ab").await.unwrap();
        assert_eq!(
            holders,
            vec![
                TokenHolder {
                    account: hex::encode([1u8; 32]),
                    owner: Some(hex::encode([2u8; 32])),
                    balance: u64::MAX,
                },
                TokenHolder {
                    account: hex::encode([3u8; 32]),
                    owner: None,
                    balance: 5,
                },
            ]
        );
        let heads = server.await.unwrap();
        for (i, head) in heads.iter().enumerate() {
            let want = format!(
                "get /api/v1/mainnet/tokens/ab/holders?limit=200&page={} ",
                i + 1
            );
            assert!(head.starts_with(&want), "{head}");
        }
    }

    #[tokio::test]
    async fn token_transfers_page_block_ascending_and_leave_failed_ones_out() {
        let (url, server) = fake_server(2, |request| {
            let row = |txid: &str, inner: Option<i32>, src: Option<&str>, failed: bool| {
                serde_json::json!({
                    "txid": txid, "outer_index": 1, "inner_index": inner,
                    "source_account_hex": src, "destination_account_hex": "CD",
                    "amount": "18446744073709551615", "block_height": 7, "is_failed": failed,
                    "created_at": "2026-10-19T19:28:29.784141Z",
                })
            };
            let page = if request.contains("&page=1 ") {
                serde_json::json!({"has_more": true, "results": [
                    row("t1", Some(2), None, false), row("t2", None, Some("ab"), true)]})
            } else {
                serde_json::json!({"has_more": false, "results": [row("t3", None, Some("AB"), false)]})
            };
            (200, page.to_string())
        })
        .await;
        let client = ExplorerClient::new(url, None, Some("mainnet".into()));
        let moved = |txid: &str, ix_path: &str, source: Option<&str>| ShareTransfer {
            txid: txid.into(),
            ix_path: ix_path.into(),
            block_height: 7,
            source: source.map(Into::into),
            destination: Some("cd".into()),
            amount: u64::MAX,
        };
        assert_eq!(
            client.token_transfers("ab").await.unwrap(),
            [moved("t1", "1.2", None), moved("t3", "1", Some("ab"))]
        );
        let heads = server.await.unwrap();
        for (i, head) in heads.iter().enumerate() {
            let want = format!(
                "get /api/v1/mainnet/tokens/ab/transfers?sort=block&dir=asc&limit=200&page={} ",
                i + 1
            );
            assert!(head.starts_with(&want), "{head}");
        }
    }

    #[tokio::test]
    async fn a_transfer_with_no_block_is_an_error() {
        let (url, server) = fake_server(1, |_| {
            let row = serde_json::json!({
                "txid": "t1", "outer_index": 0, "inner_index": null,
                "source_account_hex": null, "destination_account_hex": "cd",
                "amount": "1", "block_height": null, "is_failed": false,
            });
            (
                200,
                serde_json::json!({"has_more": false, "results": [row]}).to_string(),
            )
        })
        .await;
        let client = ExplorerClient::new(url, None, Some("mainnet".into()));
        let err = client.token_transfers("ab").await.unwrap_err();
        assert_eq!(err.to_string(), "transfer t1 0 has no block");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_refusal_carries_its_status_and_the_start_of_its_body() {
        const BODY_PREFIX: &str = r#"{"error":"rate_limit_exceeded","pad":""#;
        let (url, server) =
            fake_server(1, |_| (429, format!("{BODY_PREFIX}{}", "x".repeat(300)))).await;
        let client = ExplorerClient::new(url, Some("k-123".into()), None);
        let err = client.latest_block(None).await.unwrap_err().to_string();
        server.await.unwrap();
        assert!(err.contains("429 Too Many Requests"), "{err}");
        let body = format!("{BODY_PREFIX}{}", "x".repeat(300));
        assert!(err.ends_with(&body[..ERROR_BODY_BYTES]), "{err}");
        assert!(!err.contains("k-123"), "{err}");
    }

    #[tokio::test]
    async fn a_block_at_or_below_a_height_is_the_newest_before_the_next() {
        let (url, server) = fake_server(1, |_| {
            let body = r#"{"total_count":91,"blocks":[{"height":90,"hash":"h","timestamp":"2026-10-14T00:01:02Z"}]}"#;
            (200, body.into())
        })
        .await;
        let client = ExplorerClient::new(url, None, None);
        let block = client.latest_block(Some(90)).await.unwrap();
        assert_eq!(block.height, 90);
        assert_eq!(block.timestamp.to_rfc3339(), "2026-10-14T00:01:02+00:00");
        let heads = server.await.unwrap();
        assert!(
            heads[0].starts_with("get /api/v1/blocks?limit=1&before=91 "),
            "{}",
            heads[0]
        );
    }

    #[tokio::test]
    async fn a_redirect_is_refused_instead_of_carrying_the_key_to_another_host() {
        let (url, server) = fake_server(1, |_| (302, "http://localhost:9/elsewhere".into())).await;
        let client = ExplorerClient::new(url, Some("k-123".into()), None);
        let err = client.latest_block(None).await.unwrap_err().to_string();
        server.await.unwrap();
        assert!(err.contains("302 Found"), "{err}");
    }
}

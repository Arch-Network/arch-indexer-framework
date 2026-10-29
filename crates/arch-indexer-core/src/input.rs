use chrono::{DateTime, Utc};
use serde::Deserialize;

/// Normalized decoder input: the shape of one transaction in arch-rust-indexer's
/// bulk export (`GET /api/v1/programs/{id}/transactions/export`), which
/// `arch_indexer::explorer::ExplorerClient` deserializes. Decoders never see raw
/// indexer JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct IndexedTx {
    pub txid: String,
    pub block_height: i64,
    pub ts: DateTime<Utc>,
    /// Top-level instructions; CPIs nest in `children`.
    pub instructions: Vec<InstructionNode>,
    /// All token transfers in the tx, tagged `<outer>` when the transfer is
    /// an outer instruction and `<outer>.<n>` when it is a CPI, which is not
    /// its tree path below the first level (see `transfers_under`).
    pub token_transfers: Vec<TokenTransfer>,
    /// Execution status as the export serves it. Absent only in fixtures
    /// captured before the export carried it; see `effects_happened`.
    #[serde(default)]
    pub status: Option<TxStatus>,
    /// Independent of `status`: a Processed transaction can still have been
    /// rolled back by consensus.
    #[serde(default)]
    pub rolled_back: bool,
    /// The runtime's log lines, in order, as the node recorded them. Programs
    /// on Satellite emit events with `emit!`, which is a `Program data:
    /// <base64>` log line rather than an instruction, so a decoder for such a
    /// program reads these (see [`IndexedTx::program_logs`]). Empty when the
    /// export predates the field; the ingest runner then fetches them for a
    /// decoder that `needs_logs`.
    #[serde(default)]
    pub logs: Vec<String>,
}

/// The node serializes every log line cut to this many bytes
/// (arch-network `sdk/src/types/processed_transaction.rs`,
/// `LOG_MESSAGES_BYTES_LIMIT`). A `Program data:` line of exactly this length
/// has lost its tail, and so has its base64 payload.
pub const LOG_LINE_LIMIT: usize = 255;

/// A `Program data:` line attributed to the instruction frame that wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramData {
    /// The frame's dotted path, in the same convention as the instruction
    /// tree (`"1"`, `"1.0"`), from the `invoke [depth]` / `success` lines.
    pub ix_path: String,
    /// Hex program id of the frame.
    pub program_id: String,
    /// Position among this frame's `Program data:` lines, from 0.
    pub ordinal: usize,
    /// The decoded payload. When `truncated`, only the prefix the node kept:
    /// the base64 was cut at the line limit and decoded up to its last whole
    /// quantum, so up to two trailing bytes of what survived are lost too.
    pub payload: Vec<u8>,
    pub truncated: bool,
}

/// A `Program log:` message attributed to its frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramMessage {
    pub ix_path: String,
    pub program_id: String,
    /// The text after `Program log: `.
    pub message: String,
}

/// The program output of a transaction, by instruction frame.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgramLogs {
    pub data: Vec<ProgramData>,
    pub messages: Vec<ProgramMessage>,
    /// The runtime stopped recording (`Log truncated`, the 10 kB budget):
    /// nothing after that point exists, so any frame at or after it is
    /// incomplete. A decoder must fail closed on the whole transaction.
    pub truncated_tail: bool,
    /// The invoke/success lines did not nest as a tree (a success for a
    /// program that is not the open frame, an invoke at an impossible
    /// depth). Attribution is then unreliable; fail closed.
    pub consistent: bool,
}

impl IndexedTx {
    /// Walk the log lines, tracking the frame stack the runtime prints
    /// (`Program <id> invoke [depth]` opens a frame, `Program <id> success`
    /// or `failed` closes it), and attribute every `Program data:` and
    /// `Program log:` line to the frame that emitted it. Frame paths follow
    /// the instruction tree's convention: the n-th depth-1 invoke is outer
    /// instruction `n`, its m-th depth-2 invoke is `n.m`, and so on — the
    /// runtime logs every invocation, builtins included, so the two agree.
    /// Program ids are base58 in the logs and hex here.
    pub fn program_logs(&self) -> ProgramLogs {
        struct Frame {
            path: String,
            program: String,
            children: usize,
            data: usize,
        }
        let mut out = ProgramLogs {
            consistent: true,
            ..Default::default()
        };
        let mut stack: Vec<Frame> = Vec::new();
        let mut outer = 0usize;
        for line in &self.logs {
            if line == "Log truncated" {
                out.truncated_tail = true;
                break;
            }
            if let Some(rest) = line.strip_prefix("Program data: ") {
                let Some(top) = stack.last_mut() else {
                    out.consistent = false;
                    continue;
                };
                // `sol_log_data` joins several chunks with spaces; `emit!`
                // passes exactly one, so anything after a space is not ours.
                let b64 = rest.split(' ').next().unwrap_or("");
                let truncated = line.len() >= LOG_LINE_LIMIT;
                let usable = if truncated {
                    &b64[..b64.len() / 4 * 4]
                } else {
                    b64
                };
                let Some(payload) = base64_decode(usable) else {
                    out.consistent = false;
                    continue;
                };
                out.data.push(ProgramData {
                    ix_path: top.path.clone(),
                    program_id: top.program.clone(),
                    ordinal: top.data,
                    payload,
                    truncated,
                });
                top.data += 1;
                continue;
            }
            if let Some(message) = line.strip_prefix("Program log: ") {
                if let Some(top) = stack.last() {
                    out.messages.push(ProgramMessage {
                        ix_path: top.path.clone(),
                        program_id: top.program.clone(),
                        message: message.to_string(),
                    });
                }
                continue;
            }
            let Some(rest) = line.strip_prefix("Program ") else {
                continue;
            };
            let mut words = rest.split(' ');
            let (Some(id), Some(verb)) = (words.next(), words.next()) else {
                continue;
            };
            match verb {
                "invoke" => {
                    let depth: usize = words
                        .next()
                        .and_then(|d| d.strip_prefix('['))
                        .and_then(|d| d.strip_suffix(']'))
                        .and_then(|d| d.parse().ok())
                        .unwrap_or(0);
                    if depth == 0 || stack.len() != depth - 1 {
                        out.consistent = false;
                        continue;
                    }
                    let path = match stack.last_mut() {
                        None => {
                            let p = outer.to_string();
                            outer += 1;
                            p
                        }
                        Some(parent) => {
                            let p = format!("{}.{}", parent.path, parent.children);
                            parent.children += 1;
                            p
                        }
                    };
                    stack.push(Frame {
                        path,
                        program: program_hex(id),
                        children: 0,
                        data: 0,
                    });
                }
                "success" | "failed" | "failed:" => match stack.pop() {
                    Some(frame) if frame.program == program_hex(id) => {}
                    _ => out.consistent = false,
                },
                _ => {} // "consumed …", "return: …" and anything newer
            }
        }
        out
    }
}

/// Base58 (what the runtime logs) to lowercase hex (what the tree carries);
/// a string that is not a 32-byte key is kept as is, so it matches nothing.
fn program_hex(id: &str) -> String {
    match bs58::decode(id).into_vec() {
        Ok(bytes) if bytes.len() == 32 => hex::encode(bytes),
        _ => id.to_string(),
    }
}

/// Strict standard base64 (RFC 4648, `=` padding, no whitespace), which is
/// what the runtime's `Program data:` line carries. `None` on any other
/// input — a truncated line is handled by the caller, never guessed here.
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a') as u32 + 26,
            b'0'..=b'9' => (c - b'0') as u32 + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for (i, chunk) in bytes.chunks(4).enumerate() {
        let last = i + 1 == bytes.len() / 4;
        let pad = chunk.iter().rev().take_while(|c| **c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut acc = 0u32;
        for &c in &chunk[..4 - pad] {
            acc = (acc << 6) | val(c)?;
        }
        acc <<= 6 * pad as u32;
        let b = acc.to_be_bytes();
        out.extend_from_slice(&b[1..4 - pad]);
    }
    Some(out)
}

/// `{"type":"Processed"}` or `{"type":"Failed","message":"…"}`.
#[derive(Debug, Clone, Deserialize)]
pub struct TxStatus {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstructionNode {
    pub program_id: String,
    /// Dotted position path: "2" (outer index 2), "2.0" (its first CPI), …
    pub ix_path: String,
    /// Raw instruction data, hex-encoded by the indexer.
    pub data_hex: String,
    /// Account pubkeys (hex) in instruction order.
    pub accounts: Vec<String>,
    #[serde(default)]
    pub children: Vec<InstructionNode>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenTransfer {
    pub ix_path: String,
    /// None for mint legs (tokens created into `destination`).
    pub source: Option<String>,
    /// None for burn legs (tokens destroyed from `source`).
    pub destination: Option<String>,
    pub mint: String,
    /// Atoms as decimal string (u64/u128-safe).
    pub amount: String,
    pub decimals: i32,
}

impl IndexedTx {
    /// Whether the transaction's effects exist on the ledger. The runtime
    /// records the instruction trace on the error path too, so a failed
    /// transaction is exported with every self-CPI event it emitted before
    /// failing; decoding one reports state that never existed. The export's
    /// contract is to allowlist `Processed` (never denylist `Failed`) and to
    /// skip `rolled_back`, so a missing status fails closed.
    pub fn effects_happened(&self) -> bool {
        self.status.as_ref().is_some_and(|s| s.kind == "Processed") && !self.rolled_back
    }

    pub fn touches_program(&self, program_id: &str) -> bool {
        fn walk(nodes: &[InstructionNode], pid: &str) -> bool {
            nodes
                .iter()
                .any(|n| n.program_id == pid || walk(&n.children, pid))
        }
        walk(&self.instructions, program_id)
    }

    /// Depth-first iterator over all instruction nodes.
    pub fn walk(&self) -> Vec<&InstructionNode> {
        preorder(&self.instructions)
    }

    /// Token transfers that happened inside the subtree of `ix_path`.
    pub fn transfers_under<'a>(&'a self, ix_path: &str) -> Vec<&'a TokenTransfer> {
        let prefix = format!("{ix_path}.");
        self.token_transfers
            .iter()
            .filter(|t| self.transfer_path(t).starts_with(&prefix))
            .collect()
    }

    /// The tree path of the instruction that made `t`. The export tags a CPI
    /// transfer `<outer>.<n>`, `n` its position in the outer instruction's
    /// inner-instruction list, which is the depth-first order of that
    /// instruction's subtree: the tree path only one level down.
    fn transfer_path<'a>(&'a self, t: &'a TokenTransfer) -> &'a str {
        t.ix_path
            .split_once('.')
            .and_then(|(outer, n)| {
                let outer = self.instructions.iter().find(|i| i.ix_path == outer)?;
                preorder(&outer.children)
                    .get(n.parse::<usize>().ok()?)
                    .copied()
            })
            .map_or(t.ix_path.as_str(), |node| node.ix_path.as_str())
    }
}

fn preorder(nodes: &[InstructionNode]) -> Vec<&InstructionNode> {
    fn collect<'a>(nodes: &'a [InstructionNode], out: &mut Vec<&'a InstructionNode>) {
        for n in nodes {
            out.push(n);
            collect(&n.children, out);
        }
    }
    let mut out = Vec::new();
    collect(nodes, &mut out);
    out
}

impl InstructionNode {
    pub fn data(&self) -> Vec<u8> {
        hex::decode(&self.data_hex).unwrap_or_else(|e| {
            tracing::warn!(ix = %self.ix_path, %e, "instruction data_hex failed to hex-decode; treating as empty");
            Vec::new()
        })
    }
}

/// Little-endian readers for borsh-style instruction args.
pub struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    pub fn skip(&mut self, n: usize) -> anyhow::Result<()> {
        self.take(n).map(|_| ())
    }
    pub fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            anyhow::bail!("instruction data too short: need {} at {}", n, self.pos);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> anyhow::Result<u128> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn i128(&mut self) -> anyhow::Result<i128> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> anyhow::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i32(&mut self) -> anyhow::Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    pub fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn bool(&mut self) -> anyhow::Result<bool> {
        Ok(self.u8()? != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::{base64_decode, IndexedTx, LOG_LINE_LIMIT};

    fn tx(json: &str) -> IndexedTx {
        serde_json::from_str(json).expect("parses")
    }

    const BASE: &str = r#""txid":"t","block_height":1,"ts":"2026-09-10T13:35:30Z","instructions":[],"token_transfers":[]"#;

    const LEND: &str = "GTwufykqdDcVruQifmRWQgHmM6PNX7kYpxnUTk2Xm15j";
    const LEND_HEX: &str = "e5c67d6103d851ebc66018d8524dc3fcb3810ad4e8204d4564705d5702c78c32";
    const TOKEN: &str = "TokenT4em53UrV4gSvZ3nCS2mZeHaqTLapwt6iZt6Mk";
    const BUDGET: &str = "ComputeBudget111111111111111111111111111111";

    fn with_logs(lines: &[String]) -> IndexedTx {
        let mut t = tx(&format!("{{{BASE}}}"));
        t.logs = lines.to_vec();
        t
    }

    #[test]
    fn base64_is_strict_rfc4648() {
        assert_eq!(base64_decode("YWJj").unwrap(), b"abc");
        assert_eq!(base64_decode("YWI=").unwrap(), b"ab");
        assert_eq!(base64_decode("YQ==").unwrap(), b"a");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert!(base64_decode("YWJ").is_none(), "length not a multiple of 4");
        assert!(base64_decode("YQ==YWJj").is_none(), "padding mid-stream");
        assert!(base64_decode("YW J=").is_none(), "whitespace");
        assert!(base64_decode("YWJ-").is_none(), "url alphabet");
    }

    /// The frame paths must be the instruction tree's: outer index from
    /// depth-1 invokes, child index from depth-2 invokes under that parent,
    /// builtins included. A liquidation-style tx: compute budget at 0, the
    /// program at 1 with a token CPI at 1.0 — the data line after the CPI
    /// returns belongs to frame 1, not 1.0.
    #[test]
    fn program_data_is_attributed_to_the_emitting_frame() {
        let t = with_logs(&[
            format!("Program {BUDGET} invoke [1]"),
            "Invoked ComputeBudget Program, Heap size: 32768".to_string(),
            format!("Program {BUDGET} success"),
            format!("Program {LEND} invoke [1]"),
            "Program log: Instruction: LendingAccountDeposit".to_string(),
            "Program data: YWJj".to_string(),
            format!("Program {TOKEN} invoke [2]"),
            "Program log: Instruction: Transfer".to_string(),
            "Program data: eHl6".to_string(),
            format!("Program {TOKEN} consumed 1857 of 180045 compute units"),
            format!("Program {TOKEN} success"),
            "Program data: ZGVm".to_string(),
            format!("Program {LEND} consumed 23276 of 197630 compute units"),
            format!("Program {LEND} success"),
        ]);
        let logs = t.program_logs();
        assert!(logs.consistent);
        assert!(!logs.truncated_tail);
        let seen: Vec<(&str, &str, usize, &[u8], bool)> = logs
            .data
            .iter()
            .map(|d| {
                (
                    d.ix_path.as_str(),
                    d.program_id.as_str(),
                    d.ordinal,
                    d.payload.as_slice(),
                    d.truncated,
                )
            })
            .collect();
        let token_hex = hex::encode(bs58::decode(TOKEN).into_vec().unwrap());
        assert_eq!(
            seen,
            vec![
                ("1", LEND_HEX, 0, b"abc".as_slice(), false),
                ("1.0", token_hex.as_str(), 0, b"xyz".as_slice(), false),
                ("1", LEND_HEX, 1, b"def".as_slice(), false),
            ]
        );
        assert_eq!(logs.messages.len(), 2);
        assert_eq!(logs.messages[0].ix_path, "1");
        assert_eq!(
            logs.messages[0].message,
            "Instruction: LendingAccountDeposit"
        );
        assert_eq!(logs.messages[1].ix_path, "1.0");
    }

    /// A line at the node's 255-byte limit is a cut event: the surviving
    /// prefix decodes (to its last whole quantum) and is flagged, never
    /// silently accepted as a whole payload or dropped.
    #[test]
    fn a_line_at_the_limit_is_a_truncated_payload() {
        let b64 = "QUJD".repeat(80); // 320 chars of "ABC"
        let line = format!("Program data: {b64}");
        let cut = &line[..LOG_LINE_LIMIT];
        assert_eq!(cut.len(), LOG_LINE_LIMIT);
        let t = with_logs(&[
            format!("Program {LEND} invoke [1]"),
            cut.to_string(),
            format!("Program {LEND} success"),
        ]);
        let logs = t.program_logs();
        assert!(logs.consistent);
        let d = &logs.data[0];
        assert!(d.truncated);
        // 255 − 14 = 241 base64 chars → 240 usable → 180 bytes of "ABC…".
        assert_eq!(d.payload.len(), 180);
        assert!(d.payload.chunks(3).all(|c| c == b"ABC"));

        // One char shorter is a complete line and decodes whole.
        let whole = format!("Program data: {}", &b64[..240]);
        assert!(whole.len() < LOG_LINE_LIMIT);
        let t = with_logs(&[
            format!("Program {LEND} invoke [1]"),
            whole,
            format!("Program {LEND} success"),
        ]);
        assert!(!t.program_logs().data[0].truncated);
    }

    #[test]
    fn budget_exhaustion_and_bad_nesting_fail_closed() {
        let t = with_logs(&[
            format!("Program {LEND} invoke [1]"),
            "Program data: YWJj".to_string(),
            "Log truncated".to_string(),
            "Program data: ZGVm".to_string(),
        ]);
        let logs = t.program_logs();
        assert!(logs.truncated_tail);
        assert_eq!(
            logs.data.len(),
            1,
            "nothing after the truncation marker is read"
        );

        let t = with_logs(&[
            format!("Program {LEND} invoke [1]"),
            format!("Program {TOKEN} success"),
        ]);
        assert!(
            !t.program_logs().consistent,
            "a success for a program that is not the open frame"
        );

        let t = with_logs(&[format!("Program {TOKEN} invoke [2]")]);
        assert!(!t.program_logs().consistent, "depth 2 with no open frame");

        let t = with_logs(&["Program data: YWJj".to_string()]);
        assert!(!t.program_logs().consistent, "data outside any frame");

        let t = with_logs(&[
            format!("Program {LEND} invoke [1]"),
            "Program data: not*base64".to_string(),
            format!("Program {LEND} success"),
        ]);
        assert!(
            !t.program_logs().consistent,
            "a malformed short line is not a payload"
        );
    }

    #[test]
    fn only_a_processed_unrolled_transaction_has_effects() {
        assert!(tx(&format!(
            r#"{{{BASE},"status":{{"type":"Processed"}},"rolled_back":false}}"#
        ))
        .effects_happened());
        assert!(!tx(&format!(
            r#"{{{BASE},"status":{{"type":"Failed","message":"x"}},"rolled_back":false}}"#
        ))
        .effects_happened());
        assert!(!tx(&format!(
            r#"{{{BASE},"status":{{"type":"Processed"}},"rolled_back":true}}"#
        ))
        .effects_happened());
        // Pre-status fixtures still parse, and fail closed at the gate.
        let legacy = tx(&format!("{{{BASE}}}"));
        assert!(legacy.status.is_none());
        assert!(!legacy.effects_happened());
    }
}

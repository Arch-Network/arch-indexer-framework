//! Keyset paging for event logs ordered by `(ts, txid, ix_path)`.

use super::ApiError;
use axum::http::StatusCode;
use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug, serde::Deserialize)]
pub struct EventsParams {
    pub limit: Option<i64>,
    /// The previous page's `next`; opaque to the caller.
    pub before: Option<String>,
}

pub const EVENTS_DEFAULT_LIMIT: i64 = 50;
pub const EVENTS_MAX_LIMIT: i64 = 200;

/// A page's bounds from its query: the size clamped to what the route serves,
/// and the position the previous page ended at.
pub fn page_of(p: &EventsParams) -> Result<(i64, Option<EventCursor>), ApiError> {
    let limit = p
        .limit
        .unwrap_or(EVENTS_DEFAULT_LIMIT)
        .clamp(1, EVENTS_MAX_LIMIT);
    let cursor = match p.before.as_deref() {
        None => None,
        Some(s) => Some(EventCursor::decode(s).ok_or((
            StatusCode::BAD_REQUEST,
            "`before` must be the `next` of a previous page".to_string(),
        ))?),
    };
    Ok((limit, cursor))
}

/// Keyset position: the last row's `(ts, txid, ix_path)`, which is the page
/// order and ends in the table's primary key, so a page never repeats or skips
/// a row however many events share a timestamp.
#[derive(Debug, PartialEq)]
pub struct EventCursor {
    pub ts: DateTime<Utc>,
    pub txid: String,
    pub ix_path: String,
}

impl EventCursor {
    pub fn encode(&self) -> String {
        // Microseconds: timestamptz's own precision, so the cursor round-trips exactly.
        format!(
            "{}|{}|{}",
            self.ts.to_rfc3339_opts(SecondsFormat::Micros, true),
            self.txid,
            self.ix_path
        )
    }

    pub fn decode(s: &str) -> Option<Self> {
        let mut parts = s.splitn(3, '|');
        let ts = DateTime::parse_from_rfc3339(parts.next()?)
            .ok()?
            .with_timezone(&Utc);
        let txid = parts.next()?.to_string();
        let ix_path = parts.next()?.to_string();
        // Logs that order by the path cast to int[] would turn a value that
        // cast rejects into a 500 carrying the caller's text into the error log.
        if !ix_path.split('.').all(|n| n.parse::<i32>().is_ok()) {
            return None;
        }
        Some(Self { ts, txid, ix_path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_cursor_round_trips_at_the_database_precision() {
        let cursor = EventCursor {
            ts: DateTime::parse_from_rfc3339("2026-09-22T19:03:00.123456Z")
                .unwrap()
                .with_timezone(&Utc),
            txid: "ab".repeat(32),
            ix_path: "0.1".into(),
        };
        assert_eq!(EventCursor::decode(&cursor.encode()), Some(cursor));
        assert_eq!(EventCursor::decode("not a cursor"), None);
        assert_eq!(EventCursor::decode("2026-09-22T19:03:00Z|txid"), None);
    }

    #[test]
    fn a_cursor_is_refused_unless_its_ix_path_is_an_instruction_path() {
        let at =
            |ix_path: &str| EventCursor::decode(&format!("2026-09-22T19:03:00Z|txid|{ix_path}"));
        assert!(at("0").is_some());
        assert!(at("12.0.3").is_some());
        for tampered in [
            "",
            "abc",
            "1..2",
            "1.",
            "99999999999",
            "1.y\n2026-09-30T00:00:00Z ERROR forged",
        ] {
            assert_eq!(at(tampered), None, "{tampered:?}");
        }
    }

    #[test]
    fn page_size_is_clamped_and_a_bad_cursor_refused() {
        let page = |limit, before: Option<&str>| {
            page_of(&EventsParams {
                limit,
                before: before.map(String::from),
            })
        };
        assert_eq!(page(None, None).unwrap().0, EVENTS_DEFAULT_LIMIT);
        assert_eq!(page(Some(0), None).unwrap().0, 1);
        assert_eq!(page(Some(10_000), None).unwrap().0, EVENTS_MAX_LIMIT);
        assert_eq!(
            page(None, Some("junk")).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }
}

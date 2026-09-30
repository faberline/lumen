//! The page cursor: the opaque token a page hands back, parsed into an offset,
//! a sort keyset or a score keyset.

use crate::index::domain::query::sort::SortValue;

// ---------------------------------------------------------------------------
// Cursor helpers — opaque base64 tokens in two generations:
//   v1 (legacy)  {"offset": N}                    — offset skip, O(offset) deep
//   v2 keyset    {"v":2,"m":"sort","k":bits,"d":docid}
//                {"v":2,"m":"score","k":score_bits,"t":external_id}
// Keyset cursors carry the LAST hit's position so the next page SEEKS to it
// (sorted walks: O(log n) range/binary-search start; score ranking: filter +
// top-`limit` heap instead of top-(offset+limit)) — deep pagination cost no
// longer grows with depth. v1 cursors keep working (legacy skip path).
// ---------------------------------------------------------------------------

/// A parsed pagination cursor.
#[derive(Debug)]
pub(crate) enum PageCursor {
    /// Legacy offset skip.
    Offset(u64),
    /// Continue a single-number-field sorted walk after (sort-value bits, docid).
    SortKeyset { bits: u64, docid: u32 },
    /// Continue a keyword or composite sorted walk after (sort-key tuple, docid).
    SortValuesKeyset { values: Vec<SortValue>, docid: u32 },
    /// Continue a score-ranked page after (score bits, external_id).
    ScoreKeyset { score_bits: u32, eid: String },
}

fn encode_cursor(json: String) -> String {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
    STANDARD_NO_PAD.encode(json)
}

pub(in crate::index) fn make_cursor(offset: usize) -> String {
    encode_cursor(format!("{{\"offset\":{offset}}}"))
}

pub(crate) fn make_sort_cursor(bits: u64, docid: u32) -> String {
    encode_cursor(format!(
        "{{\"v\":2,\"m\":\"sort\",\"k\":{bits},\"d\":{docid}}}"
    ))
}

pub(crate) fn make_sort_values_cursor(values: &[SortValue], docid: u32) -> String {
    let keys: Vec<serde_json::Value> = values
        .iter()
        .map(|value| match value {
            SortValue::Number(bits) => serde_json::json!({"n": bits}),
            SortValue::Keyword(term) => serde_json::json!({"s": term}),
        })
        .collect();
    let payload = serde_json::json!({"v": 2, "m": "sortv", "k": keys, "d": docid});
    encode_cursor(payload.to_string())
}

pub(crate) fn make_score_cursor(score: f32, eid: &str) -> String {
    let payload = serde_json::json!({"v": 2, "m": "score", "k": score.to_bits(), "t": eid});
    encode_cursor(payload.to_string())
}

pub(crate) fn parse_page_cursor(s: &str) -> Option<PageCursor> {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
    let raw = STANDARD_NO_PAD.decode(s).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    if let Some(offset) = v.get("offset").and_then(|o| o.as_u64()) {
        return Some(PageCursor::Offset(offset));
    }
    if v.get("v")?.as_u64()? != 2 {
        return None;
    }
    match v.get("m")?.as_str()? {
        "sort" => Some(PageCursor::SortKeyset {
            bits: v.get("k")?.as_u64()?,
            docid: v.get("d")?.as_u64()? as u32,
        }),
        "sortv" => {
            let values = v
                .get("k")?
                .as_array()?
                .iter()
                .map(|value| {
                    if let Some(bits) = value.get("n").and_then(|n| n.as_u64()) {
                        return Some(SortValue::Number(bits));
                    }
                    value
                        .get("s")
                        .and_then(|s| s.as_str())
                        .map(|s| SortValue::Keyword(s.to_string()))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(PageCursor::SortValuesKeyset {
                values,
                docid: v.get("d")?.as_u64()? as u32,
            })
        }
        "score" => Some(PageCursor::ScoreKeyset {
            score_bits: v.get("k")?.as_u64()? as u32,
            eid: v.get("t")?.as_str()?.to_string(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

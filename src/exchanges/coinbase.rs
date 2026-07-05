//! Coinbase Exchange `level2_batch` adapter.
//!
//! Endpoint: wss://ws-feed.exchange.coinbase.com  (public, no auth)
//! Flow:     subscribe to `level2_batch` -> one `snapshot` message, then a
//!           stream of `l2update` messages (batched to ~50ms). The plain
//!           `level2` channel is auth-only now; `level2_batch` is the public one.
//!
//! Unlike Binance, the channel carries no per-message sequence number, so
//! genuine drops cannot be detected from the payload alone. We synthesize a
//! monotonic counter (reset on each snapshot) so updates stay contiguous for
//! the shared [`OrderBook`] machinery; a fresh `snapshot` on reconnect resets
//! it cleanly. `l2update` does carry a `time` field, used for the latency
//! metric.

use super::parse_levels;
use crate::feed::Exchange;
use crate::orderbook::{BookEvent, Level};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Coinbase {
    /// Product id, e.g. "BTC-USD".
    symbol: String,
    /// Synthetic monotonic sequence, reset to 0 on each snapshot.
    seq: AtomicU64,
}

impl Coinbase {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_uppercase(),
            seq: AtomicU64::new(0),
        }
    }
}

#[derive(Serialize)]
struct Subscribe<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    product_ids: Vec<&'a str>,
    channels: Vec<&'a str>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum Incoming {
    #[serde(rename = "snapshot")]
    Snapshot {
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
    },
    #[serde(rename = "l2update")]
    L2Update {
        /// Exchange event time (RFC3339). Optional so an update without it still
        /// parses — we just report no latency for it.
        #[serde(default)]
        time: Option<String>,
        /// Each change is [side, price, size] with side "buy" or "sell".
        changes: Vec<[String; 3]>,
    },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(other)]
    Other,
}

#[async_trait]
impl Exchange for Coinbase {
    fn name(&self) -> &str {
        "coinbase"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn ws_url(&self) -> String {
        "wss://ws-feed.exchange.coinbase.com".to_string()
    }

    fn subscribe_messages(&self) -> Vec<String> {
        // `level2_batch` is the public (no-auth) depth channel on the Exchange
        // feed; the plain `level2` channel now requires authentication and
        // replies "Failed to subscribe" without it. Both deliver identical
        // `snapshot` + `l2update` messages — batch just throttles them to ~50ms.
        let sub = Subscribe {
            kind: "subscribe",
            product_ids: vec![&self.symbol],
            channels: vec!["level2_batch"],
        };
        vec![serde_json::to_string(&sub).expect("serialize subscribe")]
    }

    fn needs_rest_snapshot(&self) -> bool {
        false
    }

    async fn fetch_snapshot(&self) -> Result<BookEvent> {
        // Snapshot arrives over the socket; never called.
        Err(anyhow!("coinbase snapshot comes over the websocket"))
    }

    fn parse_message(&self, raw: &str) -> Result<Option<BookEvent>> {
        let msg: Incoming = match serde_json::from_str(raw) {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        match msg {
            Incoming::Snapshot { bids, asks } => {
                self.seq.store(0, Ordering::SeqCst);
                Ok(Some(BookEvent::Snapshot {
                    bids: parse_levels(&bids)?,
                    asks: parse_levels(&asks)?,
                    sequence: 0,
                }))
            }
            Incoming::L2Update { time, changes } => {
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                for [side, price, size] in &changes {
                    let level = Level {
                        price: Decimal::from_str(price)
                            .with_context(|| format!("bad price {price:?}"))?,
                        qty: Decimal::from_str(size)
                            .with_context(|| format!("bad size {size:?}"))?,
                    };
                    match side.as_str() {
                        "buy" => bids.push(level),
                        "sell" => asks.push(level),
                        other => return Err(anyhow!("unknown side {other:?}")),
                    }
                }
                let n = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
                let event_time_ms = time.as_deref().and_then(rfc3339_to_epoch_ms);
                Ok(Some(BookEvent::Delta {
                    bids,
                    asks,
                    first: n,
                    last: n,
                    event_time_ms,
                    checksum: None,
                }))
            }
            Incoming::Error { message } => {
                log::error!("[coinbase] feed error: {message}");
                Ok(None)
            }
            Incoming::Other => Ok(None),
        }
    }
}

/// Parse an RFC3339 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.fff...]Z`, the form
/// Coinbase puts in `l2update.time`) into milliseconds since the Unix epoch.
/// Zero-dependency on purpose: not worth a full datetime crate for one field.
/// Fractional seconds beyond milliseconds are truncated; malformed input yields
/// `None`.
fn rfc3339_to_epoch_ms(s: &str) -> Option<u64> {
    // Split "<date>T<time>Z" into date and time halves.
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;

    let mut dparts = date.split('-');
    let year: i64 = dparts.next()?.parse().ok()?;
    let month: i64 = dparts.next()?.parse().ok()?;
    let day: i64 = dparts.next()?.parse().ok()?;
    if dparts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Time is "HH:MM:SS" or "HH:MM:SS.fff...".
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    let mut tparts = hms.split(':');
    let hour: i64 = tparts.next()?.parse().ok()?;
    let min: i64 = tparts.next()?.parse().ok()?;
    let sec: i64 = tparts.next()?.parse().ok()?;
    if tparts.next().is_some() || hour > 23 || min > 59 || sec > 60 {
        return None;
    }

    // Milliseconds: first 3 fractional digits, right-padded (".5" -> 500ms).
    let millis: i64 = if frac.is_empty() {
        0
    } else {
        if !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut ms = frac.as_bytes().to_vec();
        ms.resize(3, b'0');
        std::str::from_utf8(&ms[..3]).ok()?.parse().ok()?
    };

    let days = days_from_civil(year, month, day);
    let total_ms = ((days * 86_400 + hour * 3_600 + min * 60 + sec) * 1_000) + millis;
    u64::try_from(total_ms).ok()
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date.
/// Howard Hinnant's `days_from_civil` algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_epoch_known_instants() {
        assert_eq!(rfc3339_to_epoch_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_to_epoch_ms("1970-01-01T00:00:01.500Z"), Some(1500));
        assert_eq!(
            rfc3339_to_epoch_ms("2019-08-14T20:42:27.265Z"),
            Some(1565815347265)
        );
    }

    #[test]
    fn rfc3339_truncates_micros_to_millis() {
        // 6 fractional digits (microseconds) -> keep the first 3 (milliseconds).
        assert_eq!(
            rfc3339_to_epoch_ms("2024-06-27T12:34:56.789012Z"),
            Some(1719491696789)
        );
    }

    #[test]
    fn rfc3339_handles_leap_day() {
        assert_eq!(
            rfc3339_to_epoch_ms("2024-02-29T00:00:00Z"),
            Some(1709164800000)
        );
    }

    #[test]
    fn rfc3339_rejects_garbage() {
        assert_eq!(rfc3339_to_epoch_ms("not-a-time"), None);
        assert_eq!(rfc3339_to_epoch_ms(""), None);
        assert_eq!(rfc3339_to_epoch_ms("2024-13-01T00:00:00Z"), None);
    }

    #[test]
    fn l2update_carries_event_time() {
        let c = Coinbase::new("BTC-USD");
        let raw = r#"{"type":"l2update","product_id":"BTC-USD","time":"2019-08-14T20:42:27.265Z","changes":[["buy","61000.00","0.5"]]}"#;
        let ev = c
            .parse_message(raw)
            .unwrap()
            .expect("should parse an l2update");
        match ev {
            BookEvent::Delta {
                event_time_ms,
                bids,
                ..
            } => {
                assert_eq!(event_time_ms, Some(1565815347265));
                assert_eq!(bids.len(), 1);
            }
            _ => panic!("expected a delta"),
        }
    }

    #[test]
    fn l2update_without_time_has_no_event_time() {
        let c = Coinbase::new("BTC-USD");
        let raw = r#"{"type":"l2update","changes":[["sell","61001.00","1.0"]]}"#;
        let ev = c.parse_message(raw).unwrap().expect("should parse");
        match ev {
            BookEvent::Delta { event_time_ms, .. } => assert_eq!(event_time_ms, None),
            _ => panic!("expected a delta"),
        }
    }
}

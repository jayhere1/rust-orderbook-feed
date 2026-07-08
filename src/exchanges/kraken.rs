//! Kraken Exchange v2 `book` adapter with CRC32 checksum validation.
//!
//! Endpoint: wss://ws.kraken.com/v2  (public, no auth)
//! Flow:     subscribe to `instrument` (for this symbol's price/qty precision)
//!           and `book` `depth=10` -> one `snapshot`, then `update` messages.
//!
//! Kraken's book carries no sequence number; its per-message CRC32 `checksum`
//! (over the top-10 of each side) is the integrity mechanism. We synthesize a
//! monotonic counter (like Coinbase) so updates stay contiguous for the shared
//! [`OrderBook`], and after each update we recompute the checksum and compare —
//! a mismatch is surfaced as an error so the driver resyncs. Kraken sends
//! updates that can add levels beyond the subscribed depth and does not send
//! removals for levels leaving it, so the driver truncates to `depth` after
//! applying (see [`crate::orderbook::OrderBook::retain_top`]).
//!
//! The checksum string is built per Kraken's spec: top-10 asks (low->high) then
//! top-10 bids (high->low); each level contributes the price then the quantity,
//! formatted to the symbol's precision with the decimal point removed and
//! leading zeros stripped.

use super::rfc3339_to_epoch_ms;
use crate::checksum::crc32;
use crate::feed::{Exchange, ParsedEvent};
use crate::orderbook::{BookEvent, Level, OrderBook};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

pub struct Kraken {
    /// Pair ids, e.g. "BTC/USD".
    symbols: Vec<String>,
    /// Per-symbol synthetic monotonic sequence, reset to 0 on that symbol's
    /// snapshot (Kraken's book carries no sequence number).
    seq: Mutex<HashMap<String, u64>>,
    /// Per-symbol `(price_precision, qty_precision)` learned from the
    /// `instrument` channel; needed to reconstruct the exact checksum string.
    precision: Mutex<HashMap<String, (u32, u32)>>,
}

impl Kraken {
    pub fn new<S: AsRef<str>>(symbols: &[S]) -> Self {
        Self {
            symbols: symbols.iter().map(|s| s.as_ref().to_uppercase()).collect(),
            seq: Mutex::new(HashMap::new()),
            precision: Mutex::new(HashMap::new()),
        }
    }

    /// Next synthetic sequence for `symbol`; `reset` restarts it at 0 (snapshot).
    fn next_seq(&self, symbol: &str, reset: bool) -> u64 {
        let mut seqs = self.seq.lock().unwrap();
        let entry = seqs.entry(symbol.to_string()).or_insert(0);
        if reset {
            *entry = 0;
        } else {
            *entry += 1;
        }
        *entry
    }
}

/// Just the routing fields; `data`'s shape differs per channel, so it is parsed
/// in a second pass once the channel is known.
#[derive(Deserialize)]
struct Envelope {
    channel: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct BookWrap {
    data: Vec<BookData>,
}

#[derive(Deserialize)]
struct BookData {
    symbol: String,
    #[serde(default)]
    bids: Vec<KLevel>,
    #[serde(default)]
    asks: Vec<KLevel>,
    checksum: u32,
    /// Exchange event time (RFC3339); present on `book` updates.
    #[serde(default)]
    timestamp: Option<String>,
}

/// Price and qty arrive as JSON numbers; kept as `serde_json::Number` so they
/// can be turned into exact `Decimal`s via their shortest round-trip string
/// rather than through a lossy `f64`.
#[derive(Deserialize)]
struct KLevel {
    price: serde_json::Number,
    qty: serde_json::Number,
}

#[derive(Deserialize)]
struct InstrumentWrap {
    data: InstrumentData,
}

#[derive(Deserialize)]
struct InstrumentData {
    #[serde(default)]
    pairs: Vec<PairInfo>,
}

#[derive(Deserialize)]
struct PairInfo {
    symbol: String,
    price_precision: u32,
    qty_precision: u32,
}

fn to_levels(raw: &[KLevel]) -> Result<Vec<Level>> {
    raw.iter()
        .map(|l| {
            let price = Decimal::from_str(&l.price.to_string())
                .with_context(|| format!("bad price {}", l.price))?;
            let qty = Decimal::from_str(&l.qty.to_string())
                .with_context(|| format!("bad qty {}", l.qty))?;
            Ok(Level { price, qty })
        })
        .collect()
}

/// One level's contribution to the checksum string: value formatted to
/// `precision` decimals, decimal point removed, leading zeros stripped.
fn kraken_token(value: Decimal, precision: u32) -> String {
    let formatted = format!("{:.*}", precision as usize, value);
    let digits: String = formatted.chars().filter(|c| *c != '.').collect();
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

#[async_trait]
impl Exchange for Kraken {
    fn name(&self) -> &str {
        "kraken"
    }

    fn symbols(&self) -> &[String] {
        &self.symbols
    }

    fn ws_url(&self) -> String {
        "wss://ws.kraken.com/v2".to_string()
    }

    fn subscribe_messages(&self) -> Vec<String> {
        let instrument = serde_json::json!({
            "method": "subscribe",
            "params": { "channel": "instrument" }
        });
        let book = serde_json::json!({
            "method": "subscribe",
            "params": { "channel": "book", "symbol": self.symbols, "depth": 10 }
        });
        vec![instrument.to_string(), book.to_string()]
    }

    fn needs_rest_snapshot(&self) -> bool {
        false
    }

    async fn fetch_snapshot(&self) -> Result<BookEvent> {
        Err(anyhow!("kraken snapshot arrives over the websocket"))
    }

    fn parse_message(&self, raw: &str) -> Result<Option<ParsedEvent>> {
        let env: Envelope = match serde_json::from_str(raw) {
            Ok(e) => e,
            Err(_) => return Ok(None),
        };
        match env.channel.as_deref() {
            Some("instrument") => {
                if let Ok(wrap) = serde_json::from_str::<InstrumentWrap>(raw) {
                    let mut precision = self.precision.lock().unwrap();
                    for p in wrap.data.pairs {
                        if self.symbols.contains(&p.symbol) {
                            precision.insert(p.symbol, (p.price_precision, p.qty_precision));
                        }
                    }
                }
                Ok(None)
            }
            Some("book") => {
                let wrap: BookWrap = serde_json::from_str(raw)?;
                let Some(data) = wrap.data.into_iter().next() else {
                    return Ok(None);
                };
                let symbol = data.symbol;
                let bids = to_levels(&data.bids)?;
                let asks = to_levels(&data.asks)?;
                match env.kind.as_deref() {
                    Some("snapshot") => {
                        self.next_seq(&symbol, true);
                        Ok(Some(ParsedEvent {
                            symbol,
                            event: BookEvent::Snapshot {
                                bids,
                                asks,
                                sequence: 0,
                            },
                        }))
                    }
                    Some("update") => {
                        let n = self.next_seq(&symbol, false);
                        Ok(Some(ParsedEvent {
                            symbol,
                            event: BookEvent::Delta {
                                bids,
                                asks,
                                first: n,
                                last: n,
                                event_time_ms: data
                                    .timestamp
                                    .as_deref()
                                    .and_then(rfc3339_to_epoch_ms),
                                checksum: Some(data.checksum),
                            },
                        }))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    fn book_depth_limit(&self) -> Option<usize> {
        Some(10)
    }

    fn verify_checksum(&self, symbol: &str, book: &OrderBook, checksum: u32) -> Result<()> {
        let Some((price_prec, qty_prec)) = self.precision.lock().unwrap().get(symbol).copied()
        else {
            // Precision not learned yet (instrument message not seen); can't
            // verify, so don't force a spurious resync.
            return Ok(());
        };
        let mut s = String::new();
        for (price, qty) in book.top_asks(10) {
            s.push_str(&kraken_token(price, price_prec));
            s.push_str(&kraken_token(qty, qty_prec));
        }
        for (price, qty) in book.top_bids(10) {
            s.push_str(&kraken_token(price, price_prec));
            s.push_str(&kraken_token(qty, qty_prec));
        }
        let computed = crc32(s.as_bytes());
        if computed == checksum {
            Ok(())
        } else {
            Err(anyhow!(
                "book checksum mismatch: computed {computed}, expected {checksum}"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    const INSTRUMENT: &str = include_str!("../../tests/fixtures/kraken_btcusd_instrument.json");
    const BOOK: &str = include_str!("../../tests/fixtures/kraken_btcusd_book.jsonl");

    #[test]
    fn update_extracts_event_time_and_symbol() {
        let kraken = Kraken::new(&["BTC/USD"]);
        let raw = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[{"price":62623.4,"qty":0.07892262}],"checksum":643939250,"timestamp":"2019-08-14T20:42:27.265Z"}]}"#;
        let parsed = kraken.parse_message(raw).unwrap().expect("update parses");
        assert_eq!(parsed.symbol, "BTC/USD");
        match parsed.event {
            BookEvent::Delta {
                event_time_ms,
                checksum,
                ..
            } => {
                assert_eq!(event_time_ms, Some(1565815347265));
                assert_eq!(checksum, Some(643939250));
            }
            _ => panic!("expected a delta"),
        }
    }

    #[test]
    fn token_strips_decimal_and_leading_zeros() {
        // Kraken's documented example, plus values from the real fixture.
        assert_eq!(kraken_token(dec("45285.2"), 1), "452852");
        assert_eq!(kraken_token(dec("0.00100000"), 8), "100000");
        assert_eq!(kraken_token(dec("62622.9"), 1), "626229");
        assert_eq!(kraken_token(dec("0.03654491"), 8), "3654491");
    }

    fn seed_from_snapshot(kraken: &Kraken) -> OrderBook {
        // Precision first, then the snapshot.
        assert!(kraken.parse_message(INSTRUMENT.trim()).unwrap().is_none());
        let snap_line = BOOK.lines().next().unwrap();
        let mut book = OrderBook::new();
        match kraken.parse_message(snap_line).unwrap().unwrap().event {
            BookEvent::Snapshot {
                bids,
                asks,
                sequence,
            } => book.apply_snapshot(&bids, &asks, sequence),
            _ => panic!("first message should be a snapshot"),
        }
        book.retain_top(10);
        book
    }

    #[test]
    fn snapshot_checksum_matches_and_wrong_one_is_rejected() {
        let kraken = Kraken::new(&["BTC/USD"]);
        let book = seed_from_snapshot(&kraken);
        // The real checksum from the captured snapshot.
        assert!(kraken.verify_checksum("BTC/USD", &book, 814493173).is_ok());
        assert!(kraken.verify_checksum("BTC/USD", &book, 12345).is_err());
    }

    #[test]
    fn recorded_session_checksums_match_kraken() {
        let kraken = Kraken::new(&["BTC/USD"]);
        assert!(kraken.parse_message(INSTRUMENT.trim()).unwrap().is_none());

        let mut book = OrderBook::new();
        let mut verified = 0usize;
        for line in BOOK.lines().filter(|l| !l.trim().is_empty()) {
            let parsed = kraken
                .parse_message(line)
                .unwrap()
                .expect("book message parses");
            let symbol = parsed.symbol.clone();
            match parsed.event {
                BookEvent::Snapshot {
                    bids,
                    asks,
                    sequence,
                } => {
                    book.apply_snapshot(&bids, &asks, sequence);
                    book.retain_top(10);
                }
                BookEvent::Delta {
                    bids,
                    asks,
                    first,
                    last,
                    checksum,
                    ..
                } => {
                    book.apply_delta(&bids, &asks, first, last).unwrap();
                    book.retain_top(10);
                    let cksum = checksum.expect("kraken update carries a checksum");
                    kraken
                        .verify_checksum(&symbol, &book, cksum)
                        .expect("our checksum must match kraken's on real data");
                    verified += 1;
                }
            }
        }
        assert!(verified >= 5, "should verify several real update checksums");
    }
}

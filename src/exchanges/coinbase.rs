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

use super::{parse_levels, rfc3339_to_epoch_ms};
use crate::feed::{Exchange, ParsedEvent};
use crate::orderbook::{BookEvent, Level};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

pub struct Coinbase {
    /// Product ids, e.g. "BTC-USD".
    symbols: Vec<String>,
    /// Per-symbol synthetic monotonic sequence, reset to 0 on that symbol's
    /// snapshot. The channel carries no sequence, so we manufacture one per book.
    seq: Mutex<HashMap<String, u64>>,
}

impl Coinbase {
    pub fn new<S: AsRef<str>>(symbols: &[S]) -> Self {
        Self {
            symbols: symbols.iter().map(|s| s.as_ref().to_uppercase()).collect(),
            seq: Mutex::new(HashMap::new()),
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
        product_id: String,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
    },
    #[serde(rename = "l2update")]
    L2Update {
        product_id: String,
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

    fn symbols(&self) -> &[String] {
        &self.symbols
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
            product_ids: self.symbols.iter().map(String::as_str).collect(),
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

    fn parse_message(&self, raw: &str) -> Result<Option<ParsedEvent>> {
        let msg: Incoming = match serde_json::from_str(raw) {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        match msg {
            Incoming::Snapshot {
                product_id,
                bids,
                asks,
            } => {
                self.next_seq(&product_id, true);
                Ok(Some(ParsedEvent {
                    symbol: product_id,
                    event: BookEvent::Snapshot {
                        bids: parse_levels(&bids)?,
                        asks: parse_levels(&asks)?,
                        sequence: 0,
                    },
                }))
            }
            Incoming::L2Update {
                product_id,
                time,
                changes,
            } => {
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
                let n = self.next_seq(&product_id, false);
                let event_time_ms = time.as_deref().and_then(rfc3339_to_epoch_ms);
                Ok(Some(ParsedEvent {
                    symbol: product_id,
                    event: BookEvent::Delta {
                        bids,
                        asks,
                        first: n,
                        last: n,
                        event_time_ms,
                        checksum: None,
                    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2update_carries_event_time_and_symbol() {
        let c = Coinbase::new(&["BTC-USD"]);
        let raw = r#"{"type":"l2update","product_id":"BTC-USD","time":"2019-08-14T20:42:27.265Z","changes":[["buy","61000.00","0.5"]]}"#;
        let parsed = c
            .parse_message(raw)
            .unwrap()
            .expect("should parse an l2update");
        assert_eq!(parsed.symbol, "BTC-USD");
        match parsed.event {
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
        let c = Coinbase::new(&["BTC-USD"]);
        let raw =
            r#"{"type":"l2update","product_id":"BTC-USD","changes":[["sell","61001.00","1.0"]]}"#;
        let parsed = c.parse_message(raw).unwrap().expect("should parse");
        match parsed.event {
            BookEvent::Delta { event_time_ms, .. } => assert_eq!(event_time_ms, None),
            _ => panic!("expected a delta"),
        }
    }

    #[test]
    fn per_symbol_sequences_are_independent() {
        let c = Coinbase::new(&["BTC-USD", "ETH-USD"]);
        let snap =
            |p: &str| format!(r#"{{"type":"snapshot","product_id":"{p}","bids":[],"asks":[]}}"#);
        let upd = |p: &str| {
            format!(r#"{{"type":"l2update","product_id":"{p}","changes":[["buy","1","1"]]}}"#)
        };
        // Seed both, then interleave updates: each symbol counts from 1.
        c.parse_message(&snap("BTC-USD")).unwrap();
        c.parse_message(&snap("ETH-USD")).unwrap();
        let first = |raw: &str| match c.parse_message(raw).unwrap().unwrap().event {
            BookEvent::Delta { first, .. } => first,
            _ => panic!("delta"),
        };
        assert_eq!(first(&upd("BTC-USD")), 1);
        assert_eq!(first(&upd("ETH-USD")), 1); // independent, not 2
        assert_eq!(first(&upd("BTC-USD")), 2);
    }
}

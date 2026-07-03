//! Coinbase Exchange `level2` adapter.
//!
//! Endpoint: wss://ws-feed.exchange.coinbase.com  (public, no auth)
//! Flow:     subscribe to `level2` -> one `snapshot` message, then a stream
//!           of `l2update` messages.
//!
//! Unlike Binance, the level2 channel carries no per-message sequence number,
//! so genuine drops cannot be detected from the payload alone. We synthesize a
//! monotonic counter (reset on each snapshot) so updates stay contiguous for
//! the shared [`OrderBook`] machinery; a fresh `snapshot` on reconnect resets
//! it cleanly.

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
            Incoming::L2Update { changes } => {
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
                Ok(Some(BookEvent::Delta {
                    bids,
                    asks,
                    first: n,
                    last: n,
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

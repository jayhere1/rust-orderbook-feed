//! Binance spot diff-depth adapter.
//!
//! Stream:   wss://stream.binance.com:9443/ws/<symbol>@depth@100ms
//! Snapshot: GET https://api.binance.com/api/v3/depth?symbol=<SYMBOL>&limit=1000
//!
//! Local-book sync follows Binance's documented procedure: buffer stream
//! deltas, fetch the REST snapshot, drop deltas fully older than the snapshot,
//! then apply the first delta whose range straddles `lastUpdateId + 1`. That
//! contiguity check lives in [`crate::orderbook::OrderBook::apply_delta`].

use super::parse_levels;
use crate::feed::Exchange;
use crate::orderbook::BookEvent;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;

pub struct Binance {
    /// Upper-case symbol for REST, e.g. "BTCUSDT".
    symbol: String,
    /// Lower-case symbol for the stream path, e.g. "btcusdt".
    stream_symbol: String,
}

impl Binance {
    pub fn new(symbol: &str) -> Self {
        let symbol = symbol.to_uppercase();
        let stream_symbol = symbol.to_lowercase();
        Self {
            symbol,
            stream_symbol,
        }
    }
}

#[derive(Deserialize)]
struct RestSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct DepthEvent {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[async_trait]
impl Exchange for Binance {
    fn name(&self) -> &str {
        "binance"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn ws_url(&self) -> String {
        format!(
            "wss://stream.binance.com:9443/ws/{}@depth@100ms",
            self.stream_symbol
        )
    }

    fn subscribe_messages(&self) -> Vec<String> {
        // Subscription is encoded in the URL path; nothing to send.
        Vec::new()
    }

    fn needs_rest_snapshot(&self) -> bool {
        true
    }

    async fn fetch_snapshot(&self) -> Result<BookEvent> {
        let url = format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
            self.symbol
        );
        let mut res = surf::get(&url).await.map_err(|e| anyhow!(e))?;
        if !res.status().is_success() {
            return Err(anyhow!("snapshot HTTP {}", res.status()));
        }
        let snap: RestSnapshot = res.body_json().await.map_err(|e| anyhow!(e))?;
        Ok(BookEvent::Snapshot {
            bids: parse_levels(&snap.bids)?,
            asks: parse_levels(&snap.asks)?,
            sequence: snap.last_update_id,
        })
    }

    fn parse_message(&self, raw: &str) -> Result<Option<BookEvent>> {
        // Only depth-update frames carry U/u; anything else is ignored.
        let ev: DepthEvent = match serde_json::from_str(raw) {
            Ok(ev) => ev,
            Err(_) => return Ok(None),
        };
        Ok(Some(BookEvent::Delta {
            bids: parse_levels(&ev.bids)?,
            asks: parse_levels(&ev.asks)?,
            first: ev.first_update_id,
            last: ev.final_update_id,
            event_time_ms: Some(ev.event_time),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::BookEvent;

    #[test]
    fn parse_extracts_range_and_event_time() {
        let b = Binance::new("BTCUSDT");
        let raw = r#"{"e":"depthUpdate","E":1719491696789,"s":"BTCUSDT","U":100,"u":105,"b":[["61000.00","1.5"]],"a":[["61001.00","2.0"]]}"#;
        let ev = b.parse_message(raw).unwrap().expect("should parse a delta");
        match ev {
            BookEvent::Delta {
                first,
                last,
                event_time_ms,
                bids,
                asks,
            } => {
                assert_eq!((first, last), (100, 105));
                assert_eq!(event_time_ms, Some(1719491696789));
                assert_eq!((bids.len(), asks.len()), (1, 1));
            }
            _ => panic!("expected a delta"),
        }
    }
}

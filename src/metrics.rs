//! Lightweight throughput/latency accounting and periodic top-of-book output.

use crate::orderbook::OrderBook;
use std::time::{Duration, Instant};

pub struct Metrics {
    exchange: String,
    symbol: String,
    total_updates: u64,
    total_levels: u64,
    window_updates: u64,
    window_start: Instant,
    window_latency: Latency,
    last_print: Instant,
    print_every: Duration,
}

impl Metrics {
    pub fn new(exchange: &str, symbol: &str) -> Self {
        let now = Instant::now();
        Self {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            total_updates: 0,
            total_levels: 0,
            window_updates: 0,
            window_start: now,
            window_latency: Latency::default(),
            last_print: now,
            print_every: Duration::from_secs(1),
        }
    }

    /// Record an applied delta touching `levels` price points. `latency_ms` is
    /// the event-to-receive latency when the feed carries an event timestamp.
    pub fn record_update(&mut self, levels: usize, latency_ms: Option<u64>) {
        self.total_updates += 1;
        self.total_levels += levels as u64;
        self.window_updates += 1;
        if let Some(l) = latency_ms {
            self.window_latency.record(l);
        }
    }

    /// Print a one-line snapshot at most once per `print_every`.
    pub fn maybe_print(&mut self, book: &OrderBook, depth: usize) {
        if self.last_print.elapsed() < self.print_every {
            return;
        }
        let elapsed = self.window_start.elapsed().as_secs_f64().max(1e-6);
        let rate = self.window_updates as f64 / elapsed;

        let (bid_levels, ask_levels) = book.depth();
        let bid = book
            .best_bid()
            .map(|(p, q)| format!("{p} x {q}"))
            .unwrap_or_else(|| "--".into());
        let ask = book
            .best_ask()
            .map(|(p, q)| format!("{p} x {q}"))
            .unwrap_or_else(|| "--".into());
        let spread = book
            .spread()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "--".into());
        let lat = match self.window_latency.summary() {
            Some((avg, max)) => format!("lat {avg}/{max} ms (avg/max)"),
            None => "lat --".to_string(),
        };

        println!(
            "[{}:{}] bid {bid} | ask {ask} | spread {spread} | book {bid_levels}/{ask_levels} | {rate:.0} upd/s | {lat} | {} total",
            self.exchange, self.symbol, self.total_updates
        );

        if depth > 1 {
            let bids = book.top_bids(depth);
            let asks = book.top_asks(depth);
            for i in 0..depth {
                let b = bids
                    .get(i)
                    .map(|(p, q)| format!("{p:>14} {q:>14}"))
                    .unwrap_or_else(|| " ".repeat(29));
                let a = asks
                    .get(i)
                    .map(|(p, q)| format!("{p:<14} {q:<14}"))
                    .unwrap_or_default();
                println!("    {b}  |  {a}");
            }
        }

        self.last_print = Instant::now();
        self.window_updates = 0;
        self.window_start = Instant::now();
        self.window_latency.reset();
    }
}

/// Rolling per-window latency aggregation: event-time (exchange) to
/// receive-time (local). Tracks enough to report average and worst-case.
#[derive(Default)]
struct Latency {
    sum_ms: u64,
    count: u64,
    max_ms: u64,
}

impl Latency {
    fn record(&mut self, ms: u64) {
        self.sum_ms += ms;
        self.count += 1;
        self.max_ms = self.max_ms.max(ms);
    }

    /// `(avg_ms, max_ms)` over the current window, or `None` if no samples
    /// were recorded (e.g. a feed that carries no event timestamp).
    fn summary(&self) -> Option<(u64, u64)> {
        // checked_div yields None on an empty window (count == 0).
        self.sum_ms
            .checked_div(self.count)
            .map(|avg| (avg, self.max_ms))
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_empty_is_none() {
        let l = Latency::default();
        assert_eq!(l.summary(), None);
    }

    #[test]
    fn latency_reports_avg_and_max() {
        let mut l = Latency::default();
        l.record(4);
        l.record(6);
        l.record(38);
        // avg = (4 + 6 + 38) / 3 = 16, max = 38
        assert_eq!(l.summary(), Some((16, 38)));
    }

    #[test]
    fn latency_reset_clears_window() {
        let mut l = Latency::default();
        l.record(10);
        l.reset();
        assert_eq!(l.summary(), None);
    }

    #[test]
    fn record_update_feeds_latency_window() {
        let mut m = Metrics::new("binance", "BTCUSDT");
        m.record_update(2, Some(10));
        m.record_update(2, Some(20));
        assert_eq!(m.window_latency.summary(), Some((15, 20)));
    }

    #[test]
    fn record_update_without_timestamp_has_no_latency() {
        let mut m = Metrics::new("binance", "BTCUSDT");
        m.record_update(2, None);
        assert_eq!(m.window_latency.summary(), None);
    }
}

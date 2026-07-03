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
    last_print: Instant,
    print_every: Duration,
    healthy: bool,
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
            last_print: now,
            print_every: Duration::from_secs(1),
            healthy: false,
        }
    }

    /// Record an applied delta touching `levels` price points.
    pub fn record_update(&mut self, levels: usize) {
        self.total_updates += 1;
        self.total_levels += levels as u64;
        self.window_updates += 1;
    }

    /// Mark the session as having survived long enough to be considered healthy
    /// (used to reset reconnect backoff).
    pub fn mark_healthy(&mut self) {
        self.healthy = true;
    }

    pub fn session_was_healthy(&mut self) -> bool {
        let was = self.healthy;
        self.healthy = false;
        was
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

        println!(
            "[{}:{}] bid {bid} | ask {ask} | spread {spread} | book {bid_levels}/{ask_levels} | {rate:.0} upd/s | {} total",
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
    }
}

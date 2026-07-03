//! In-memory order book maintained from snapshot + incremental deltas.

use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// One side of the book. Bids and asks are both stored as price -> quantity.
/// A quantity of zero means the level should be removed.
pub type Levels = BTreeMap<Decimal, Decimal>;

/// A single price/quantity update. `qty == 0` deletes the level.
#[derive(Debug, Clone)]
pub struct Level {
    pub price: Decimal,
    pub qty: Decimal,
}

/// A parsed book event coming off a feed: either a full snapshot that
/// replaces book state, or an incremental delta that mutates it.
#[derive(Debug, Clone)]
pub enum BookEvent {
    /// Full replacement of the book. `sequence` is the last update id it covers.
    Snapshot {
        bids: Vec<Level>,
        asks: Vec<Level>,
        sequence: u64,
    },
    /// Incremental update. `first`/`last` are the update-id range this delta
    /// spans (Binance uses U/u; Coinbase carries a single sequence, so
    /// first == last there). `event_time_ms` is the exchange-stamped event time
    /// in epoch milliseconds when the feed provides one (`None` otherwise); it
    /// is used only for the latency metric, not for book correctness.
    Delta {
        bids: Vec<Level>,
        asks: Vec<Level>,
        first: u64,
        last: u64,
        event_time_ms: Option<u64>,
    },
}

/// Error returned when an incoming delta does not line up with the book's
/// current sequence, meaning we dropped one or more messages.
#[derive(Debug)]
pub struct SequenceGap {
    pub expected: u64,
    pub got: u64,
}

impl std::fmt::Display for SequenceGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sequence gap: expected update id {}, got {}",
            self.expected, self.got
        )
    }
}

impl std::error::Error for SequenceGap {}

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: Levels,
    asks: Levels,
    /// Last update id currently reflected in the book.
    last_update_id: u64,
    initialized: bool,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Last update id currently reflected in the book. Part of the book's
    /// public surface and asserted on in the unit tests; the binary itself
    /// never reads it, hence the allow.
    #[allow(dead_code)]
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    /// Replace the entire book from a snapshot.
    pub fn apply_snapshot(&mut self, bids: &[Level], asks: &[Level], sequence: u64) {
        self.bids.clear();
        self.asks.clear();
        for l in bids {
            set_level(&mut self.bids, l);
        }
        for l in asks {
            set_level(&mut self.asks, l);
        }
        self.last_update_id = sequence;
        self.initialized = true;
    }

    /// Apply an incremental delta.
    ///
    /// `first`/`last` bound the update-id range carried by the delta. The book
    /// accepts it only if it is contiguous with what we already have:
    /// `first <= last_update_id + 1 <= last`. A delta that starts beyond the
    /// next expected id means we missed messages and returns a [`SequenceGap`]
    /// so the caller can resync from a fresh snapshot. Stale deltas that fall
    /// entirely below the current id are ignored.
    pub fn apply_delta(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        first: u64,
        last: u64,
    ) -> Result<bool, SequenceGap> {
        if !self.initialized {
            return Err(SequenceGap {
                expected: 0,
                got: first,
            });
        }

        // Entirely stale (already applied) — drop silently.
        if last <= self.last_update_id {
            return Ok(false);
        }

        let expected = self.last_update_id + 1;
        // A gap exists if this delta begins after the id we were expecting.
        if first > expected {
            return Err(SequenceGap {
                expected,
                got: first,
            });
        }

        for l in bids {
            set_level(&mut self.bids, l);
        }
        for l in asks {
            set_level(&mut self.asks, l);
        }
        self.last_update_id = last;
        Ok(true)
    }

    /// Best bid (highest price).
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    /// Best ask (lowest price).
    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    /// Spread between best ask and best bid, if both sides are populated.
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => Some(a - b),
            _ => None,
        }
    }

    /// Top `n` bids, highest price first.
    pub fn top_bids(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, q)| (*p, *q))
            .collect()
    }

    /// Top `n` asks, lowest price first.
    pub fn top_asks(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect()
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }
}

/// Insert/update a level, or remove it when quantity is zero.
fn set_level(side: &mut Levels, l: &Level) {
    if l.qty.is_zero() {
        side.remove(&l.price);
    } else {
        side.insert(l.price, l.qty);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn lvl(p: &str, q: &str) -> Level {
        Level {
            price: d(p),
            qty: d(q),
        }
    }

    #[test]
    fn snapshot_sets_top_of_book() {
        let mut b = OrderBook::new();
        b.apply_snapshot(
            &[lvl("100", "1"), lvl("99", "2")],
            &[lvl("101", "1"), lvl("102", "3")],
            10,
        );
        assert_eq!(b.best_bid(), Some((d("100"), d("1"))));
        assert_eq!(b.best_ask(), Some((d("101"), d("1"))));
        assert_eq!(b.spread(), Some(d("1")));
        assert_eq!(b.last_update_id(), 10);
    }

    #[test]
    fn delta_updates_and_removes_levels() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&[lvl("100", "1")], &[lvl("101", "1")], 5);
        // Update qty at 100, add new bid at 99, remove ask at 101.
        let applied = b
            .apply_delta(&[lvl("100", "4"), lvl("99", "2")], &[lvl("101", "0")], 6, 6)
            .unwrap();
        assert!(applied);
        assert_eq!(b.best_bid(), Some((d("100"), d("4"))));
        assert_eq!(b.best_ask(), None); // 101 was removed
        assert_eq!(b.depth(), (2, 0));
        assert_eq!(b.last_update_id(), 6);
    }

    #[test]
    fn stale_delta_is_ignored() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&[lvl("100", "1")], &[lvl("101", "1")], 100);
        let applied = b.apply_delta(&[lvl("100", "9")], &[], 90, 99).unwrap();
        assert!(!applied);
        assert_eq!(b.best_bid(), Some((d("100"), d("1"))));
        assert_eq!(b.last_update_id(), 100);
    }

    #[test]
    fn overlapping_delta_is_applied() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&[lvl("100", "1")], &[lvl("101", "1")], 100);
        // Binance-style: first <= last_update_id+1 <= last
        let applied = b.apply_delta(&[lvl("100", "7")], &[], 98, 105).unwrap();
        assert!(applied);
        assert_eq!(b.best_bid(), Some((d("100"), d("7"))));
        assert_eq!(b.last_update_id(), 105);
    }

    #[test]
    fn gap_is_detected() {
        let mut b = OrderBook::new();
        b.apply_snapshot(&[lvl("100", "1")], &[lvl("101", "1")], 100);
        // Expected 101, but delta starts at 103 -> gap.
        let err = b
            .apply_delta(&[lvl("100", "7")], &[], 103, 104)
            .unwrap_err();
        assert_eq!(err.expected, 101);
        assert_eq!(err.got, 103);
    }

    #[test]
    fn delta_before_init_errors() {
        let mut b = OrderBook::new();
        assert!(b.apply_delta(&[lvl("100", "1")], &[], 1, 1).is_err());
    }
}

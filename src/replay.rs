//! Integration tests that replay recorded *real* exchange sessions through the
//! exact parse + order-book machinery the live driver uses (`parse_snapshot`,
//! `parse_message`, `OrderBook::apply_delta`). This exercises the whole
//! snapshot → delta → gap path on genuine exchange data, deterministically and
//! offline — no network, no flakiness. Fixtures live in `tests/fixtures/` and
//! were captured with the coherent local-book procedure (see the capture note
//! in the RUNBOOK); the Binance snapshot's `lastUpdateId` falls inside the
//! delta range so a straddling delta exists, just like production.

use crate::exchanges::binance::Binance;
use crate::exchanges::coinbase::Coinbase;
use crate::feed::{Exchange, ParsedEvent};
use crate::orderbook::{BookEvent, OrderBook};

const BINANCE_SNAPSHOT: &str = include_str!("../tests/fixtures/binance_btcusdt_snapshot.json");
const BINANCE_DELTAS: &str = include_str!("../tests/fixtures/binance_btcusdt_deltas.jsonl");
const COINBASE_SESSION: &str = include_str!("../tests/fixtures/coinbase_btcusd.jsonl");

/// Seed a book from the recorded Binance REST snapshot.
fn seed_binance() -> OrderBook {
    let mut book = OrderBook::new();
    match Binance::parse_snapshot(BINANCE_SNAPSHOT).unwrap() {
        BookEvent::Snapshot {
            bids,
            asks,
            sequence,
        } => book.apply_snapshot(&bids, &asks, sequence),
        _ => panic!("fixture snapshot did not parse as a Snapshot"),
    }
    book
}

fn nonempty_lines(s: &str) -> impl Iterator<Item = &str> {
    s.lines().filter(|l| !l.trim().is_empty())
}

#[test]
fn binance_recorded_session_stays_contiguous() {
    let binance = Binance::new(&["BTCUSDT"]);
    let mut book = seed_binance();

    let mut applied = 0usize;
    let mut prev_id = book.last_update_id();
    for line in nonempty_lines(BINANCE_DELTAS) {
        let Some(ParsedEvent {
            event:
                BookEvent::Delta {
                    bids,
                    asks,
                    first,
                    last,
                    ..
                },
            ..
        }) = binance.parse_message(line).unwrap()
        else {
            continue;
        };
        match book.apply_delta(&bids, &asks, first, last) {
            // pre-snapshot deltas are stale and silently dropped
            Ok(false) => {}
            Ok(true) => {
                assert!(last >= prev_id, "update ids must be non-decreasing");
                prev_id = last;
                applied += 1;
            }
            Err(gap) => panic!("real coherent data must not gap: {gap}"),
        }
    }

    assert!(applied > 0, "at least one recorded delta should apply");
    assert_eq!(book.last_update_id(), prev_id);
    let (bid, _) = book.best_bid().expect("book has a bid side");
    let (ask, _) = book.best_ask().expect("book has an ask side");
    assert!(bid < ask, "best bid {bid} must be below best ask {ask}");
}

#[test]
fn binance_dropped_frame_is_detected_as_gap() {
    let binance = Binance::new(&["BTCUSDT"]);
    let mut book = seed_binance();

    // Replay normally until the book is live (2 deltas applied), then drop the
    // next contiguous frame to simulate a lost message. The frame after the
    // hole must surface as a SequenceGap.
    let mut applied = 0usize;
    let mut dropped_one = false;
    let mut hit_gap = false;
    for line in nonempty_lines(BINANCE_DELTAS) {
        let Some(ParsedEvent {
            event:
                BookEvent::Delta {
                    bids,
                    asks,
                    first,
                    last,
                    ..
                },
            ..
        }) = binance.parse_message(line).unwrap()
        else {
            continue;
        };
        if applied >= 2 && !dropped_one {
            dropped_one = true; // "lose" this frame
            continue;
        }
        match book.apply_delta(&bids, &asks, first, last) {
            Ok(true) => applied += 1,
            Ok(false) => {}
            Err(_) => {
                hit_gap = true;
                break;
            }
        }
    }

    assert!(dropped_one, "test should have dropped a contiguous frame");
    assert!(
        hit_gap,
        "a dropped contiguous frame must surface as a SequenceGap"
    );
}

#[test]
fn coinbase_recorded_session_stays_contiguous() {
    let coinbase = Coinbase::new(&["BTC-USD"]);
    let mut book = OrderBook::new();

    let mut seeded = false;
    let mut applied = 0usize;
    for line in nonempty_lines(COINBASE_SESSION) {
        let Some(parsed) = coinbase.parse_message(line).unwrap() else {
            continue;
        };
        match parsed.event {
            BookEvent::Snapshot {
                bids,
                asks,
                sequence,
            } => {
                book.apply_snapshot(&bids, &asks, sequence);
                seeded = true;
            }
            BookEvent::Delta {
                bids,
                asks,
                first,
                last,
                ..
            } => {
                assert!(seeded, "snapshot must arrive before any l2update");
                match book.apply_delta(&bids, &asks, first, last) {
                    Ok(true) => applied += 1,
                    Ok(false) => {}
                    Err(gap) => panic!("coinbase synthetic sequence must not gap: {gap}"),
                }
            }
        }
    }

    assert!(seeded, "session should contain a snapshot");
    assert!(applied > 0, "at least one l2update should apply");
    let (bid, _) = book.best_bid().expect("book has a bid side");
    let (ask, _) = book.best_ask().expect("book has an ask side");
    assert!(bid < ask, "best bid {bid} must be below best ask {ask}");
}

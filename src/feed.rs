//! Generic feed driver: connects a WebSocket, maintains one [`OrderBook`] per
//! symbol from snapshots + incremental deltas, detects sequence gaps / checksum
//! mismatches, and reconnects with backoff. Exchange-specific behaviour lives
//! behind the [`Exchange`] trait. A single connection can carry several symbols
//! (Coinbase / Kraken); each parsed message says which symbol it belongs to.

use crate::metrics::Metrics;
use crate::orderbook::{BookEvent, OrderBook};
use anyhow::{anyhow, Result};
use async_std::task;
use async_trait::async_trait;
use async_tungstenite::async_std::connect_async;
use async_tungstenite::tungstenite::Message;
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A parsed book event tagged with the symbol it applies to, so the driver can
/// route it to the right per-symbol book.
pub struct ParsedEvent {
    pub symbol: String,
    pub event: BookEvent,
}

/// Exchange-specific glue. Everything here is pure/parsing plus one async
/// snapshot fetch; the connection loop itself is shared in [`run`].
#[async_trait]
pub trait Exchange: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Market symbols this feed tracks, as the exchange expects them
    /// (already normalized).
    fn symbols(&self) -> &[String];

    /// WebSocket URL to connect to.
    fn ws_url(&self) -> String;

    /// Text frames to send immediately after connecting (subscriptions).
    fn subscribe_messages(&self) -> Vec<String>;

    /// Whether a REST snapshot must be fetched to seed the book (Binance).
    /// When false, the snapshot is expected to arrive over the socket
    /// (Coinbase / Kraken).
    fn needs_rest_snapshot(&self) -> bool;

    /// Fetch the REST order-book snapshot. Only called when
    /// [`needs_rest_snapshot`](Exchange::needs_rest_snapshot) is true (a
    /// single-symbol feed).
    async fn fetch_snapshot(&self) -> Result<BookEvent>;

    /// Parse one raw text frame into a symbol-tagged book event, or `None` to
    /// ignore it (control messages, subscription acks, heartbeats, ...).
    fn parse_message(&self, raw: &str) -> Result<Option<ParsedEvent>>;

    /// If the feed maintains only a fixed depth (e.g. Kraken `book` `depth=10`),
    /// the number of levels to keep per side after each update. The driver
    /// truncates the book to this depth. `None` means keep the full book.
    fn book_depth_limit(&self) -> Option<usize> {
        None
    }

    /// Validate the maintained `book` for `symbol` against an exchange-provided
    /// `checksum`. Called after an update that carried one; an `Err` is treated
    /// like a sequence gap (drop the book and resync). Default is a no-op for
    /// feeds that don't provide checksums.
    fn verify_checksum(&self, _symbol: &str, _book: &OrderBook, _checksum: u32) -> Result<()> {
        Ok(())
    }
}

/// Run the feed forever, reconnecting with capped exponential backoff.
pub async fn run(exchange: &dyn Exchange, print_depth: usize) -> Result<()> {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(30);

    loop {
        let started = Instant::now();
        match run_once(exchange, print_depth).await {
            Ok(()) => log::warn!("[{}] stream closed cleanly; reconnecting", exchange.name()),
            Err(e) => log::warn!("[{}] session ended: {e:#}; reconnecting", exchange.name()),
        }
        // A session that stayed up long enough is "healthy" — reset the backoff.
        if started.elapsed() > Duration::from_secs(10) {
            backoff = Duration::from_millis(500);
        }
        log::info!("[{}] reconnecting in {:?}", exchange.name(), backoff);
        task::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// A single connect → sync → consume cycle. Returns `Ok` on clean close, or an
/// error (a detected gap or checksum mismatch) that the caller treats as a
/// signal to resync from scratch.
async fn run_once(exchange: &dyn Exchange, print_depth: usize) -> Result<()> {
    let url = exchange.ws_url();
    log::info!("[{}] connecting to {url}", exchange.name());
    let (mut ws, _resp) = connect_async(&url).await?;

    for msg in exchange.subscribe_messages() {
        ws.send(Message::Text(msg)).await?;
    }

    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut metrics: HashMap<String, Metrics> = HashMap::new();

    if exchange.needs_rest_snapshot() {
        // Single-symbol (Binance): seed its one book from the REST snapshot.
        let symbol = exchange
            .symbols()
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("no symbol configured"))?;
        let book = buffer_then_snapshot(exchange, &mut ws).await?;
        books.insert(symbol, book);
    }

    // Steady-state consume loop.
    while let Some(frame) = ws.next().await {
        match frame? {
            Message::Text(txt) => {
                // Stamp arrival before parsing so the latency figure reflects
                // wire time, not our JSON work.
                let recv_ms = now_ms();
                if let Some(parsed) = exchange.parse_message(&txt)? {
                    apply_event(
                        exchange,
                        parsed,
                        &mut books,
                        &mut metrics,
                        print_depth,
                        recv_ms,
                    )?;
                }
            }
            Message::Ping(payload) => {
                // Keep the connection alive; split-free stream so send directly.
                ws.send(Message::Pong(payload)).await?;
            }
            Message::Close(frame) => {
                log::info!("[{}] server closed connection: {frame:?}", exchange.name());
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Binance path: buffer deltas arriving on the socket while fetching the REST
/// snapshot concurrently, apply the snapshot, then replay buffered deltas.
/// Single-symbol; returns the seeded book.
async fn buffer_then_snapshot<S>(exchange: &dyn Exchange, ws: &mut S) -> Result<OrderBook>
where
    S: futures::Stream<Item = Result<Message, async_tungstenite::tungstenite::Error>>
        + futures::Sink<Message, Error = async_tungstenite::tungstenite::Error>
        + Unpin,
{
    let mut buffered: Vec<BookEvent> = Vec::new();
    let mut snap_fut = exchange.fetch_snapshot().fuse();

    let snapshot = loop {
        select! {
            snap = snap_fut => break snap?,
            frame = ws.next().fuse() => {
                match frame {
                    Some(Ok(Message::Text(txt))) => {
                        if let Some(parsed) = exchange.parse_message(&txt)? {
                            buffered.push(parsed.event);
                        }
                    }
                    Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await?; }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(anyhow!("socket closed before snapshot")),
                }
            }
        }
    };

    let (bids, asks, sequence) = match snapshot {
        BookEvent::Snapshot {
            bids,
            asks,
            sequence,
        } => (bids, asks, sequence),
        BookEvent::Delta { .. } => return Err(anyhow!("expected snapshot, got delta")),
    };
    let mut book = OrderBook::new();
    book.apply_snapshot(&bids, &asks, sequence);
    log::info!(
        "[{}] snapshot applied @ update id {} ({} buffered deltas to replay)",
        exchange.name(),
        sequence,
        buffered.len()
    );

    for ev in buffered {
        if let BookEvent::Delta {
            bids,
            asks,
            first,
            last,
            ..
        } = ev
        {
            // Stale deltas (u <= lastUpdateId) are dropped inside apply_delta.
            book.apply_delta(&bids, &asks, first, last)
                .map_err(|g| anyhow!("gap while replaying snapshot buffer: {g}"))?;
        }
    }
    Ok(book)
}

/// Apply one parsed event to the right per-symbol book, updating that symbol's
/// metrics/output.
fn apply_event(
    exchange: &dyn Exchange,
    parsed: ParsedEvent,
    books: &mut HashMap<String, OrderBook>,
    metrics: &mut HashMap<String, Metrics>,
    print_depth: usize,
    recv_ms: u64,
) -> Result<()> {
    let ParsedEvent { symbol, event } = parsed;

    match event {
        BookEvent::Snapshot {
            bids,
            asks,
            sequence,
        } => {
            let book = books.entry(symbol.clone()).or_default();
            book.apply_snapshot(&bids, &asks, sequence);
            if let Some(limit) = exchange.book_depth_limit() {
                book.retain_top(limit);
            }
            log::info!(
                "[{}:{symbol}] snapshot applied @ {sequence}",
                exchange.name()
            );
        }
        BookEvent::Delta {
            bids,
            asks,
            first,
            last,
            event_time_ms,
            checksum,
        } => {
            // A delta for a symbol we haven't seeded yet (snapshot not arrived).
            let Some(book) = books.get_mut(&symbol) else {
                return Ok(());
            };
            let n = bids.len() + asks.len();
            match book.apply_delta(&bids, &asks, first, last) {
                Ok(true) => {
                    // Depth-limited feeds don't send removals for levels leaving
                    // the top-N, so truncate before validating the checksum.
                    if let Some(limit) = exchange.book_depth_limit() {
                        book.retain_top(limit);
                    }
                    // saturating: if the exchange clock reads ahead of ours the
                    // difference is treated as zero rather than wrapping.
                    let latency = event_time_ms.map(|e| recv_ms.saturating_sub(e));
                    metrics
                        .entry(symbol.clone())
                        .or_insert_with(|| Metrics::new(exchange.name(), &symbol))
                        .record_update(n, latency);
                    // A checksum mismatch means our book diverged; surface it as
                    // an error so the driver resyncs, exactly like a gap.
                    if let Some(cksum) = checksum {
                        exchange.verify_checksum(&symbol, book, cksum)?;
                    }
                }
                Ok(false) => {} // stale, ignored
                Err(gap) => return Err(anyhow!("{symbol}: {gap}")),
            }
        }
    }

    if let (Some(book), Some(m)) = (books.get(&symbol), metrics.get_mut(&symbol)) {
        m.maybe_print(book, print_depth);
    }
    Ok(())
}

/// Current wall-clock time in epoch milliseconds (best effort; 0 if the clock
/// is before the Unix epoch, which cannot happen in practice).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

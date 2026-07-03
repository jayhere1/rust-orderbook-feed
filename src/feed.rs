//! Generic feed driver: connects a WebSocket, syncs an [`OrderBook`] from a
//! snapshot, applies incremental deltas, detects sequence gaps, and reconnects
//! with backoff. Exchange-specific behaviour lives behind the [`Exchange`] trait.

use crate::metrics::Metrics;
use crate::orderbook::{BookEvent, OrderBook};
use anyhow::{anyhow, Result};
use async_std::task;
use async_trait::async_trait;
use async_tungstenite::async_std::connect_async;
use async_tungstenite::tungstenite::Message;
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::time::{Duration, Instant};

/// Exchange-specific glue. Everything here is pure/parsing plus one async
/// snapshot fetch; the connection loop itself is shared in [`run`].
#[async_trait]
pub trait Exchange: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Market symbol as the exchange expects it (already normalized).
    fn symbol(&self) -> &str;

    /// WebSocket URL to connect to.
    fn ws_url(&self) -> String;

    /// Text frames to send immediately after connecting (subscriptions).
    fn subscribe_messages(&self) -> Vec<String>;

    /// Whether a REST snapshot must be fetched to seed the book (Binance).
    /// When false, the snapshot is expected to arrive over the socket (Coinbase).
    fn needs_rest_snapshot(&self) -> bool;

    /// Fetch the REST order-book snapshot. Only called when
    /// [`needs_rest_snapshot`](Exchange::needs_rest_snapshot) is true.
    async fn fetch_snapshot(&self) -> Result<BookEvent>;

    /// Parse one raw text frame into a book event, or `None` to ignore it
    /// (control messages, subscription acks, heartbeats, ...).
    fn parse_message(&self, raw: &str) -> Result<Option<BookEvent>>;
}

/// Run the feed forever, reconnecting with capped exponential backoff.
pub async fn run(exchange: &dyn Exchange, print_depth: usize) -> Result<()> {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(30);
    let mut metrics = Metrics::new(exchange.name(), exchange.symbol());

    loop {
        match run_once(exchange, print_depth, &mut metrics).await {
            Ok(()) => {
                log::warn!("[{}] stream closed cleanly; reconnecting", exchange.name());
            }
            Err(e) => {
                log::warn!("[{}] session ended: {e:#}; reconnecting", exchange.name());
            }
        }
        log::info!("[{}] reconnecting in {:?}", exchange.name(), backoff);
        task::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
        // A successful, long-lived session resets backoff inside run_once via metrics.
        if metrics.session_was_healthy() {
            backoff = Duration::from_millis(500);
        }
    }
}

/// A single connect → sync → consume cycle. Returns `Ok` on clean close, or an
/// error (including a detected sequence gap) that the caller treats as a
/// signal to resync from scratch.
async fn run_once(
    exchange: &dyn Exchange,
    print_depth: usize,
    metrics: &mut Metrics,
) -> Result<()> {
    let url = exchange.ws_url();
    log::info!("[{}] connecting to {url}", exchange.name());
    let (mut ws, _resp) = connect_async(&url).await?;

    for msg in exchange.subscribe_messages() {
        ws.send(Message::Text(msg)).await?;
    }

    let mut book = OrderBook::new();
    let session_start = Instant::now();

    if exchange.needs_rest_snapshot() {
        buffer_then_snapshot(exchange, &mut ws, &mut book).await?;
    }

    // Steady-state consume loop.
    while let Some(frame) = ws.next().await {
        let frame = frame?;
        match frame {
            Message::Text(txt) => {
                // Stamp arrival before parsing so the latency figure reflects
                // wire time, not our JSON work.
                let recv_ms = now_ms();
                handle_frame(exchange, &txt, &mut book, metrics, print_depth, recv_ms)?;
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

        if session_start.elapsed() > Duration::from_secs(10) {
            metrics.mark_healthy();
        }
    }
    Ok(())
}

/// Binance path: buffer deltas arriving on the socket while fetching the REST
/// snapshot concurrently, apply the snapshot, then replay buffered deltas.
async fn buffer_then_snapshot<S>(
    exchange: &dyn Exchange,
    ws: &mut S,
    book: &mut OrderBook,
) -> Result<()>
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
                        if let Some(ev) = exchange.parse_message(&txt)? {
                            buffered.push(ev);
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
    Ok(())
}

/// Parse and apply a single steady-state frame, updating metrics/output.
fn handle_frame(
    exchange: &dyn Exchange,
    txt: &str,
    book: &mut OrderBook,
    metrics: &mut Metrics,
    print_depth: usize,
    recv_ms: u64,
) -> Result<()> {
    let event = match exchange.parse_message(txt)? {
        Some(ev) => ev,
        None => return Ok(()),
    };

    match event {
        BookEvent::Snapshot {
            bids,
            asks,
            sequence,
        } => {
            book.apply_snapshot(&bids, &asks, sequence);
            log::info!("[{}] snapshot applied @ {sequence}", exchange.name());
        }
        BookEvent::Delta {
            bids,
            asks,
            first,
            last,
            event_time_ms,
        } => {
            let n = bids.len() + asks.len();
            match book.apply_delta(&bids, &asks, first, last) {
                Ok(true) => {
                    // saturating: if the exchange clock reads ahead of ours the
                    // difference is treated as zero rather than wrapping.
                    let latency = event_time_ms.map(|e| recv_ms.saturating_sub(e));
                    metrics.record_update(n, latency);
                }
                Ok(false) => {} // stale, ignored
                Err(gap) => return Err(anyhow!("{gap}")),
            }
        }
    }

    metrics.maybe_print(book, print_depth);
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

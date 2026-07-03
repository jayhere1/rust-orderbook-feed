# orderbook-feed

A Rust WebSocket client that consumes a live crypto exchange feed (Binance or
Coinbase public streams), parses order-book deltas, and maintains a correct
in-memory order book with sequence/gap handling, automatic reconnect, and basic
throughput metrics.

Built on `async-std` + `async-tungstenite` (rustls TLS), `surf` for the REST
snapshot, `serde` for JSON, and `rust_decimal` for exact price/quantity math.

Verified against both live feeds (see [`RUNBOOK.md`](RUNBOOK.md) §10): `cargo
test` is green, and both adapters seed a snapshot and hold a correct book under a
steady delta stream.

## Run

```sh
# Binance BTCUSDT (default), print top 5 levels each tick
cargo run --release

# Coinbase BTC-USD
cargo run --release -- --exchange coinbase --symbol BTC-USD

# Ethereum on Binance, top-of-book only
cargo run --release -- --exchange binance --symbol ETHUSDT --depth 1

# more logging
RUST_LOG=debug cargo run
```

CLI flags: `--exchange {binance|coinbase}`, `--symbol <SYMBOL>`,
`--depth <N>` (book levels printed per tick). Defaults: Binance / `BTCUSDT` /
depth 5. Coinbase's default symbol is `BTC-USD`.

Sample output (real, top-3 depth, Binance BTCUSDT):

```
[binance] snapshot applied @ update id 96898756487 (4 buffered deltas to replay)
[binance:BTCUSDT] bid 61757.99 x 1.66603 | ask 61758.00 x 1.43825 | spread 0.01 | book 1049/999 | 10 upd/s | 191 total
    61757.99000000     1.66603000  |  61758.00000000 1.43825000
    61757.98000000     0.00107000  |  61758.01000000 0.00089000
    61757.97000000     0.00017000  |  61758.02000000 0.00017000
```

## How the book stays correct

**Binance** (`src/exchanges/binance.rs`) follows the exchange's documented local
book procedure: connect to `<symbol>@depth@100ms`, buffer incoming diff events,
fetch a REST depth snapshot (`/api/v3/depth?limit=1000`), drop deltas fully
older than the snapshot, then apply the first delta whose update-id range spans
`lastUpdateId + 1`. Every later delta must be contiguous (`U <= last+1 <= u`) or
the book is discarded and resynced.

**Coinbase** (`src/exchanges/coinbase.rs`) subscribes to the public
`level2_batch` channel on `wss://ws-feed.exchange.coinbase.com`, applies the
initial `snapshot` message, then folds in `l2update` changes. (`level2_batch` is
the no-auth channel; the plain `level2` channel now requires authentication and
replies `Failed to subscribe` without it — same message shapes, just batched to
~50ms.) That channel carries no per-message sequence number, so a synthetic
monotonic counter (reset on each snapshot) keeps updates contiguous for the
shared book machinery.

The contiguity/gap logic itself lives once in
`OrderBook::apply_delta` (`src/orderbook.rs`): it applies overlapping deltas,
silently drops stale ones, and returns a `SequenceGap` when a message was
missed. The generic driver in `src/feed.rs` treats a gap as a reason to drop the
connection and resync from a fresh snapshot, and reconnects with capped
exponential backoff.

## Layout

| File | Responsibility |
|------|----------------|
| `src/main.rs` | CLI parsing, logging, wiring |
| `src/orderbook.rs` | `OrderBook`, delta application, gap detection (+ unit tests) |
| `src/feed.rs` | `Exchange` trait + generic connect/sync/consume/reconnect loop |
| `src/metrics.rs` | throughput accounting and periodic top-of-book printing |
| `src/exchanges/binance.rs` | Binance diff-depth + REST snapshot adapter |
| `src/exchanges/coinbase.rs` | Coinbase `level2_batch` adapter |

## Test

```sh
cargo test
```

Unit tests in `src/orderbook.rs` cover snapshot application, level
update/removal, stale-delta dropping, overlapping-delta application, and gap
detection.

## Roadmap

Natural next steps, roughly in order of value:

- **Latency metric** — compare the exchange event timestamp (`E` on Binance,
  `time` on Coinbase) against local receive time.
- **Checksum validation** — verify the maintained book against exchange-provided
  book checksums where offered.
- **Multiple symbols per process** — Binance combined streams; several Coinbase
  `product_ids` on one subscription.
- **Recorded-feed integration test** — replay a captured session as a fixture so
  the full sync/gap path is covered without a network dependency.
- **Persistence / fan-out** — expose top-of-book over a local socket or persist
  it for downstream consumers.

# orderbook-feed

A Rust WebSocket client that consumes a live crypto exchange feed (Binance,
Coinbase, or Kraken public streams), parses order-book deltas, and maintains a
correct in-memory order book with sequence/gap handling, CRC32 checksum
validation (Kraken), automatic reconnect, and throughput + feed-latency metrics.

Built on `async-std` + `async-tungstenite` (rustls TLS), `surf` for the REST
snapshot, `serde` for JSON, and `rust_decimal` for exact price/quantity math.

Verified against all three live feeds (see [`RUNBOOK.md`](RUNBOOK.md) §10): `cargo
test` is green, and each adapter seeds a snapshot and holds a correct book under a
steady delta stream — with Kraken's per-update checksum matching ours on live
data.

## Run

```sh
# Binance BTCUSDT (default), print top 5 levels each tick
cargo run --release

# Coinbase BTC-USD
cargo run --release -- --exchange coinbase --symbol BTC-USD

# Kraken BTC/USD (checksum-validated depth-10 book)
cargo run --release -- --exchange kraken --symbol BTC/USD

# Ethereum on Binance, top-of-book only
cargo run --release -- --exchange binance --symbol ETHUSDT --depth 1

# more logging
RUST_LOG=debug cargo run
```

CLI flags: `--exchange {binance|coinbase|kraken}`, `--symbol <SYMBOL>`,
`--depth <N>` (book levels printed per tick). Defaults: Binance / `BTCUSDT` /
depth 5. Default symbols: `BTC-USD` (coinbase), `BTC/USD` (kraken).

Sample output (real, top-3 depth, Binance BTCUSDT):

```
[binance] snapshot applied @ update id 96898756487 (4 buffered deltas to replay)
[binance:BTCUSDT] bid 61966.14 x 0.49 | ask 61966.15 x 2.40 | spread 0.01 | book 1079/998 | 10 upd/s | lat 113/146 ms (avg/max) | 96 total
    61966.14000000     0.49257000  |  61966.15000000 2.40323000
    61966.13000000     0.00107000  |  61966.16000000 0.00089000
    61966.12000000     0.00017000  |  61966.17000000 0.00017000
```

`lat a/b ms (avg/max)` is the exchange-to-local latency of applied updates over
the same 1-second window as `upd/s`: for each update, `local_receive_time −
exchange_event_time` (`E` on Binance, `time` on Coinbase, `timestamp` on
Kraken). It bundles network
transit and any clock offset between the two machines, so without NTP-synced
clocks the absolute value is indicative rather than exact — the **max** and how
it moves are the useful signal. A feed that carries no event timestamp shows
`lat --`.

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

**Kraken** (`src/exchanges/kraken.rs`) subscribes to the v2 `book` channel
(`depth=10`) plus the `instrument` channel (for the symbol's price/qty
precision) on `wss://ws.kraken.com/v2`. Kraken's book has no sequence number —
instead every message carries a **CRC32 `checksum`** over the top-10 of each
side, and that is the integrity mechanism. After applying each update (and
truncating back to depth 10 — Kraken doesn't send removals for levels leaving
the top-10), the driver recomputes the checksum from the maintained book and
compares; a mismatch is treated exactly like a gap → drop and resync. The CRC32
and the exact string construction (price then qty per level, formatted to
precision, decimal removed, leading zeros stripped) live in `src/checksum.rs`
and `src/exchanges/kraken.rs`, and are tested byte-for-byte against a recorded
real Kraken session.

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
| `src/exchanges/kraken.rs` | Kraken v2 `book` adapter + checksum verification |
| `src/checksum.rs` | zero-dependency CRC32 (IEEE) |
| `src/replay.rs` | replay integration tests over recorded real sessions |
| `tests/fixtures/` | small recorded Binance/Coinbase/Kraken sessions |

## Test

```sh
cargo test
```

Two layers:

- **Unit tests** (`src/orderbook.rs`, `src/metrics.rs`, the adapters) cover
  snapshot application, level update/removal, stale-delta dropping,
  overlapping-delta application, gap detection, latency aggregation, RFC3339
  parsing, and per-exchange event-time extraction.
- **Replay integration tests** (`src/replay.rs`) feed *recorded real* Binance and
  Coinbase sessions (`tests/fixtures/`) back through the exact
  `parse` + `apply_delta` machinery the live driver uses — asserting the book
  stays contiguous on genuine data, that a deliberately dropped frame surfaces
  as a `SequenceGap`, and that top-of-book ends uncrossed. No network, no flake.
- **Checksum tests** (`src/exchanges/kraken.rs`) rebuild the book from a recorded
  real Kraken session and assert our computed CRC32 matches Kraken's on the
  snapshot and every update (plus the CRC32 standard check vector).

## Roadmap

Natural next steps, roughly in order of value:

- **Multiple symbols per process** — Binance combined streams; several Coinbase
  `product_ids` / Kraken symbols on one subscription.
- **Persistence / fan-out** — expose top-of-book over a local socket or persist
  it for downstream consumers.

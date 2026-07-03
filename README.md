# orderbook-feed

A Rust WebSocket client that consumes a live crypto exchange feed (Binance or
Coinbase public streams), parses order-book deltas, and maintains a correct
in-memory order book with sequence/gap handling, automatic reconnect, and
throughput + feed-latency metrics.

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
[binance:BTCUSDT] bid 61966.14 x 0.49 | ask 61966.15 x 2.40 | spread 0.01 | book 1079/998 | 10 upd/s | lat 113/146 ms (avg/max) | 96 total
    61966.14000000     0.49257000  |  61966.15000000 2.40323000
    61966.13000000     0.00107000  |  61966.16000000 0.00089000
    61966.12000000     0.00017000  |  61966.17000000 0.00017000
```

`lat a/b ms (avg/max)` is the exchange-to-local latency of applied updates over
the same 1-second window as `upd/s`: for each update, `local_receive_time −
exchange_event_time` (`E` on Binance, `time` on Coinbase). It bundles network
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

- **Checksum validation** — verify the maintained book against exchange-provided
  book checksums where offered.
- **Multiple symbols per process** — Binance combined streams; several Coinbase
  `product_ids` on one subscription.
- **Recorded-feed integration test** — replay a captured session as a fixture so
  the full sync/gap path is covered without a network dependency.
- **Persistence / fan-out** — expose top-of-book over a local socket or persist
  it for downstream consumers.

# Runbook — orderbook-feed

Operational guide for running and troubleshooting the live order-book client.

## 1. Prerequisites

- Rust stable (edition 2021; `rustc >= 1.75` for native async-fn-in-trait, though
  we use `async-trait` so older toolchains also work).
- Outbound network access to:
  - `stream.binance.com:9443` (WSS) and `api.binance.com:443` (REST snapshot)
  - `ws-feed.exchange.coinbase.com:443` (WSS)
  - `ws.kraken.com:443` (WSS, v2)
- No API keys required — all three feeds used here are public.

## 2. Build

```sh
cargo build            # debug
cargo build --release  # optimized (LTO on)
cargo test             # order-book unit tests
```

## 3. Run

```sh
# Binance BTCUSDT, print top 5 levels per tick (defaults)
cargo run --release

# Coinbase BTC-USD
cargo run --release -- --exchange coinbase --symbol BTC-USD

# Kraken BTC/USD (checksum-validated depth-10 book)
cargo run --release -- --exchange kraken --symbol BTC/USD

# Top-of-book only, Ethereum on Binance
cargo run --release -- --exchange binance --symbol ETHUSDT --depth 1

# Verbose logging
RUST_LOG=debug cargo run --release
```

Flags: `--exchange {binance|coinbase|kraken}`, `--symbol <SYMBOL>`, `--depth <N>`.
Log level via `RUST_LOG` (`error|warn|info|debug|trace`, default `info`).

## 4. Reading the output

Steady-state line (once per second):

```
[binance:BTCUSDT] bid 61234.10 x 0.5 | ask 61234.11 x 1.2 | spread 0.01 | book 1000/1000 | 94 upd/s | lat 113/146 ms (avg/max) | 5123 total
```

- `bid` / `ask` — best price × size on each side
- `spread` — best ask − best bid
- `book` — number of bid levels / ask levels currently held
- `upd/s` — applied deltas per second (rolling 1s window)
- `lat a/b ms (avg/max)` — exchange-to-local latency of applied updates over the
  rolling 1s window: `local_receive_time − exchange_event_time` (Binance `E`,
  Coinbase `time`). Bundles network transit **and** any clock offset between the
  hosts, so treat the absolute number as indicative unless the clocks are
  NTP-synced; the max and its drift are the real signal. Shows `lat --` for a
  feed with no event timestamp.
- `total` — applied deltas since process start

Startup log to expect (Binance):

```
starting Binance feed for BTCUSDT (printing top 5 levels)
[binance] connecting to wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
[binance] snapshot applied @ update id <N> (<K> buffered deltas to replay)
```

## 5. Normal lifecycle

1. Connect WS. Binance: subscription is in the URL. Coinbase: a `subscribe`
   frame for the `level2_batch` channel is sent. Kraken: `subscribe` frames for
   the `instrument` and `book` (`depth=10`) channels are sent.
2. Seed the book. Binance: buffer stream deltas, fetch REST snapshot, replay.
   Coinbase/Kraken: apply the `snapshot` message that arrives first.
3. Apply deltas continuously; print metrics once per second. Kraken: after each
   update, truncate to depth 10 and verify the CRC32 checksum.
4. Server pings are answered with pongs automatically.

## 6. Failure modes & expected behavior

| Symptom in logs | Cause | Automatic response | Operator action |
|---|---|---|---|
| `sequence gap: expected X, got Y` then reconnect | Dropped WS message(s) | Drop book, reconnect, resync from fresh snapshot | None — self-heals. Frequent gaps ⇒ investigate network |
| `session ended: ...; reconnecting` | Socket dropped / server close / TLS error | Reconnect with capped exponential backoff (0.5s → 30s) | None unless it loops indefinitely |
| `snapshot HTTP 429` | Binance REST rate limit on `/api/v3/depth` | Errors the session, reconnects with backoff | Reduce restart frequency; wait out the limit |
| `snapshot HTTP 4xx` (e.g. 400) | Bad/unknown symbol | Loops reconnecting | Fix `--symbol` (Binance wants `BTCUSDT`, no dash) |
| `[coinbase] feed error: <msg>` | Coinbase rejected the subscription | Message ignored; book never seeds | Fix `--symbol` (Coinbase wants `BTC-USD`, with dash) |
| `book checksum mismatch: computed X, expected Y` then reconnect | Kraken book diverged (dropped/misordered update) | Drop book, reconnect, resync | None — self-heals. Persistent ⇒ investigate network |
| Kraken book never seeds / stays empty | Bad `--symbol` (Kraken rejects the subscription) | No book events | Fix `--symbol` (Kraken wants `BTC/USD`, with slash) |
| `socket closed before snapshot` | WS died mid-sync (Binance) | Reconnect + retry sync | None — self-heals |
| Backoff keeps growing, never healthy | Persistent connectivity/DNS/TLS failure | Keeps retrying up to 30s intervals | Check egress firewall to the hosts in §1 |

Backoff resets to 0.5s after any session that stays healthy for >10s.

## 7. Symbol formats (gotcha)

- Binance: concatenated, uppercase, no separator — `BTCUSDT`, `ETHUSDT`.
  (Input is case-normalized automatically.)
- Coinbase: base-quote with a dash — `BTC-USD`, `ETH-USD`.
- Kraken: base/quote with a slash — `BTC/USD`, `ETH/USD`.

Using the wrong format for the chosen exchange is the most common startup error.

## 8. Shutdown

`Ctrl-C`. State is in-memory only; nothing is persisted, so there is no cleanup.

## 9. Known limitations

- Coinbase `level2_batch` carries no sequence number, so genuine mid-stream drops
  on that feed cannot be detected from the payload. Integrity per feed: Binance
  via update-id contiguity, Kraken via the CRC32 checksum, Coinbase best-effort
  (synthetic counter only). (See §"How the book stays correct" in `README.md`.)
- No persistence, no cross-run recovery, single symbol per process.

## 10. Verification status

Verified on macOS (Rust 1.95, edition 2021):

- `cargo build` / `cargo build --release` — clean, no warnings.
- `cargo clippy --all-targets` — clean.
- `cargo fmt --check` — clean.
- `cargo test` — 28/28 tests pass: unit tests (order book, latency aggregation,
  RFC3339 parsing, per-exchange event-time extraction, CRC32, Kraken checksum)
  plus replay + checksum integration tests over recorded real sessions (see §11).
- Live smoke test, Binance `BTCUSDT` — snapshot seeded (buffered deltas
  replayed), then steady ~10 upd/s with a 0.01 spread and ~1000/1000 book;
  latency ~`lat 113/146 ms`.
- Live smoke test, Coinbase `BTC-USD` — snapshot seeded, then steady ~17 upd/s
  over the full book (~14k/28k levels); latency ~`lat 53/113 ms`.
- Live smoke test, Kraken `BTC/USD` — snapshot seeded, then a steady stream at a
  fixed 10/10 book with **0 checksum mismatches / 0 resyncs** (our CRC32 matched
  Kraken's on every live update); latency ~`lat 14/62 ms` from the update
  `timestamp`.

Notes:
- The Coinbase adapter subscribes to `level2_batch` (the public, no-auth depth
  channel). The plain `level2` channel now requires authentication and returns
  `Failed to subscribe` without it.
- All three feeds carry an event timestamp (Binance `E`, Coinbase `time`, Kraken
  `timestamp`), so latency is reported for all of them.
- Kraken's book has no sequence number; the CRC32 `checksum` is its integrity
  mechanism, verified after every update (mismatch → resync).

## 11. Replay & checksum fixtures

Recorded real sessions in `tests/fixtures/` are replayed through the live parse +
order-book code (`cargo test`, no network): `src/replay.rs` covers the
snapshot/delta/gap path (Binance, Coinbase), and `src/exchanges/kraken.rs`
rebuilds the Kraken book and asserts our CRC32 matches Kraken's on every frame.

To refresh the fixtures, capture a *coherent* session:

- **Binance** — the REST snapshot must be fetched mid-stream so its
  `lastUpdateId` lands inside the captured delta range (a straddling delta must
  exist), exactly as the live sync does. Buffer a few `@depth@100ms` deltas,
  fetch `/api/v3/depth?...&limit=50`, then buffer a few more; save the raw frames.
- **Coinbase** — subscribe `level2_batch`, keep the `snapshot` message (trimmed
  to the top ~40 levels/side to stay small) and a run of `l2update` frames.
- **Kraken** — subscribe `instrument` and `book` `depth=10`; save the BTC/USD
  instrument entry (for precision) as a minimal `instrument` message plus the
  `book` snapshot and a run of updates (each carries its own `checksum`).

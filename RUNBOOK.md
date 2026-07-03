# Runbook — orderbook-feed

Operational guide for running and troubleshooting the live order-book client.

## 1. Prerequisites

- Rust stable (edition 2021; `rustc >= 1.75` for native async-fn-in-trait, though
  we use `async-trait` so older toolchains also work).
- Outbound network access to:
  - `stream.binance.com:9443` (WSS) and `api.binance.com:443` (REST snapshot)
  - `ws-feed.exchange.coinbase.com:443` (WSS)
- No API keys required — both feeds used here are public.

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

# Top-of-book only, Ethereum on Binance
cargo run --release -- --exchange binance --symbol ETHUSDT --depth 1

# Verbose logging
RUST_LOG=debug cargo run --release
```

Flags: `--exchange {binance|coinbase}`, `--symbol <SYMBOL>`, `--depth <N>`.
Log level via `RUST_LOG` (`error|warn|info|debug|trace`, default `info`).

## 4. Reading the output

Steady-state line (once per second):

```
[binance:BTCUSDT] bid 61234.10 x 0.5 | ask 61234.11 x 1.2 | spread 0.01 | book 1000/1000 | 94 upd/s | 5123 total
```

- `bid` / `ask` — best price × size on each side
- `spread` — best ask − best bid
- `book` — number of bid levels / ask levels currently held
- `upd/s` — applied deltas per second (rolling 1s window)
- `total` — applied deltas since process start

Startup log to expect (Binance):

```
starting Binance feed for BTCUSDT (printing top 5 levels)
[binance] connecting to wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
[binance] snapshot applied @ update id <N> (<K> buffered deltas to replay)
```

## 5. Normal lifecycle

1. Connect WS. Binance: subscription is in the URL. Coinbase: a `subscribe`
   frame for the `level2` channel is sent.
2. Seed the book. Binance: buffer stream deltas, fetch REST snapshot, replay.
   Coinbase: apply the `snapshot` message that arrives first.
3. Apply deltas continuously; print metrics once per second.
4. Server pings are answered with pongs automatically.

## 6. Failure modes & expected behavior

| Symptom in logs | Cause | Automatic response | Operator action |
|---|---|---|---|
| `sequence gap: expected X, got Y` then reconnect | Dropped WS message(s) | Drop book, reconnect, resync from fresh snapshot | None — self-heals. Frequent gaps ⇒ investigate network |
| `session ended: ...; reconnecting` | Socket dropped / server close / TLS error | Reconnect with capped exponential backoff (0.5s → 30s) | None unless it loops indefinitely |
| `snapshot HTTP 429` | Binance REST rate limit on `/api/v3/depth` | Errors the session, reconnects with backoff | Reduce restart frequency; wait out the limit |
| `snapshot HTTP 4xx` (e.g. 400) | Bad/unknown symbol | Loops reconnecting | Fix `--symbol` (Binance wants `BTCUSDT`, no dash) |
| `[coinbase] feed error: <msg>` | Coinbase rejected the subscription | Message ignored; book never seeds | Fix `--symbol` (Coinbase wants `BTC-USD`, with dash) |
| `socket closed before snapshot` | WS died mid-sync (Binance) | Reconnect + retry sync | None — self-heals |
| Backoff keeps growing, never healthy | Persistent connectivity/DNS/TLS failure | Keeps retrying up to 30s intervals | Check egress firewall to the hosts in §1 |

Backoff resets to 0.5s after any session that stays healthy for >10s.

## 7. Symbol formats (gotcha)

- Binance: concatenated, uppercase, no separator — `BTCUSDT`, `ETHUSDT`.
  (Input is case-normalized automatically.)
- Coinbase: base-quote with a dash — `BTC-USD`, `ETH-USD`.

Using the wrong format for the chosen exchange is the most common startup error.

## 8. Shutdown

`Ctrl-C`. State is in-memory only; nothing is persisted, so there is no cleanup.

## 9. Known limitations

- Coinbase `level2` carries no sequence number, so genuine mid-stream drops on
  that feed cannot be detected from the payload; gap detection is real only for
  Binance. (See §"How the book stays correct" in `README.md`.)
- No persistence, no cross-run recovery, single symbol per process.
- No checksum validation against exchange-provided book checksums (Coinbase
  offers one on some channels; not used here).

## 10. Verification status

Verified on macOS (Rust 1.95, edition 2021):

- `cargo build` / `cargo build --release` — clean, no warnings.
- `cargo clippy --all-targets` — clean.
- `cargo fmt --check` — clean.
- `cargo test` — 6/6 order-book unit tests pass.
- Live smoke test, Binance `BTCUSDT` — snapshot seeded (buffered deltas
  replayed), then steady ~10 upd/s with a 0.01 spread and ~1000/1000 book.
- Live smoke test, Coinbase `BTC-USD` — snapshot seeded, then steady ~17 upd/s
  over the full book (~14k/28k levels).

Note: the Coinbase adapter subscribes to `level2_batch` (the public, no-auth
depth channel). The plain `level2` channel now requires authentication and
returns `Failed to subscribe` without it.

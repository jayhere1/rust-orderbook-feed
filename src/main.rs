//! orderbook-feed: connect to a public crypto exchange WebSocket feed,
//! parse order-book deltas, and maintain a live in-memory order book.

mod checksum;
mod exchanges;
mod feed;
mod metrics;
mod orderbook;
#[cfg(test)]
mod replay;

use crate::exchanges::{binance::Binance, coinbase::Coinbase, kraken::Kraken};
use crate::feed::Exchange;
use anyhow::Result;
use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum ExchangeArg {
    Binance,
    Coinbase,
    Kraken,
}

/// Live order-book client for Binance / Coinbase / Kraken public feeds.
#[derive(Parser, Debug)]
#[command(name = "orderbook-feed", version, about)]
struct Args {
    /// Which exchange feed to consume.
    #[arg(short, long, value_enum, default_value_t = ExchangeArg::Binance)]
    exchange: ExchangeArg,

    /// Market symbol. Defaults: BTCUSDT (binance), BTC-USD (coinbase),
    /// BTC/USD (kraken).
    #[arg(short, long)]
    symbol: Option<String>,

    /// Number of book levels to print on each update tick (1 = top of book).
    #[arg(short, long, default_value_t = 5)]
    depth: usize,
}

impl ExchangeArg {
    fn default_symbol(self) -> &'static str {
        match self {
            ExchangeArg::Binance => "BTCUSDT",
            ExchangeArg::Coinbase => "BTC-USD",
            ExchangeArg::Kraken => "BTC/USD",
        }
    }
}

#[async_std::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let symbol = args
        .symbol
        .clone()
        .unwrap_or_else(|| args.exchange.default_symbol().to_string());

    let exchange: Box<dyn Exchange> = match args.exchange {
        ExchangeArg::Binance => Box::new(Binance::new(&symbol)),
        ExchangeArg::Coinbase => Box::new(Coinbase::new(&symbol)),
        ExchangeArg::Kraken => Box::new(Kraken::new(&symbol)),
    };

    log::info!(
        "starting {:?} feed for {symbol} (printing top {} levels)",
        args.exchange,
        args.depth
    );

    feed::run(exchange.as_ref(), args.depth).await
}

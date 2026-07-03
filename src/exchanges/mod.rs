//! Exchange adapters implementing the [`crate::feed::Exchange`] trait.

pub mod binance;
pub mod coinbase;

use crate::orderbook::Level;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

/// Convert an exchange's `[price, qty]` string pairs into typed [`Level`]s,
/// parsing each field as an exact decimal.
pub(crate) fn parse_levels(raw: &[[String; 2]]) -> Result<Vec<Level>> {
    raw.iter()
        .map(|pair| {
            let price =
                Decimal::from_str(&pair[0]).with_context(|| format!("bad price {:?}", pair[0]))?;
            let qty =
                Decimal::from_str(&pair[1]).with_context(|| format!("bad qty {:?}", pair[1]))?;
            Ok(Level { price, qty })
        })
        .collect()
}

//! Exchange adapters implementing the [`crate::feed::Exchange`] trait.

pub mod binance;
pub mod coinbase;
pub mod kraken;

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

/// Parse an RFC3339 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.fff...]Z`, the form
/// Coinbase puts in `l2update.time` and Kraken in a `book` update `timestamp`)
/// into milliseconds since the Unix epoch. Zero-dependency on purpose: not worth
/// a full datetime crate for one field. Fractional seconds beyond milliseconds
/// are truncated; malformed input yields `None`.
pub(crate) fn rfc3339_to_epoch_ms(s: &str) -> Option<u64> {
    // Split "<date>T<time>Z" into date and time halves.
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;

    let mut dparts = date.split('-');
    let year: i64 = dparts.next()?.parse().ok()?;
    let month: i64 = dparts.next()?.parse().ok()?;
    let day: i64 = dparts.next()?.parse().ok()?;
    if dparts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Time is "HH:MM:SS" or "HH:MM:SS.fff...".
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    let mut tparts = hms.split(':');
    let hour: i64 = tparts.next()?.parse().ok()?;
    let min: i64 = tparts.next()?.parse().ok()?;
    let sec: i64 = tparts.next()?.parse().ok()?;
    if tparts.next().is_some() || hour > 23 || min > 59 || sec > 60 {
        return None;
    }

    // Milliseconds: first 3 fractional digits, right-padded (".5" -> 500ms).
    let millis: i64 = if frac.is_empty() {
        0
    } else {
        if !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut ms = frac.as_bytes().to_vec();
        ms.resize(3, b'0');
        std::str::from_utf8(&ms[..3]).ok()?.parse().ok()?
    };

    let days = days_from_civil(year, month, day);
    let total_ms = ((days * 86_400 + hour * 3_600 + min * 60 + sec) * 1_000) + millis;
    u64::try_from(total_ms).ok()
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date.
/// Howard Hinnant's `days_from_civil` algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_epoch_known_instants() {
        assert_eq!(rfc3339_to_epoch_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_to_epoch_ms("1970-01-01T00:00:01.500Z"), Some(1500));
        assert_eq!(
            rfc3339_to_epoch_ms("2019-08-14T20:42:27.265Z"),
            Some(1565815347265)
        );
    }

    #[test]
    fn rfc3339_truncates_micros_to_millis() {
        // 6 fractional digits (microseconds) -> keep the first 3 (milliseconds).
        assert_eq!(
            rfc3339_to_epoch_ms("2024-06-27T12:34:56.789012Z"),
            Some(1719491696789)
        );
    }

    #[test]
    fn rfc3339_handles_leap_day() {
        assert_eq!(
            rfc3339_to_epoch_ms("2024-02-29T00:00:00Z"),
            Some(1709164800000)
        );
    }

    #[test]
    fn rfc3339_rejects_garbage() {
        assert_eq!(rfc3339_to_epoch_ms("not-a-time"), None);
        assert_eq!(rfc3339_to_epoch_ms(""), None);
        assert_eq!(rfc3339_to_epoch_ms("2024-13-01T00:00:00Z"), None);
    }
}

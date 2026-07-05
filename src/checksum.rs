//! Zero-dependency CRC32 (IEEE 802.3, the zlib/reflected variant) used to
//! validate Kraken's order-book checksum.

/// CRC32 (reflected, polynomial 0xEDB88320, init/final 0xFFFFFFFF) of `bytes`.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            // Branch-free: mask is all-ones when the low bit is set.
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_standard_check_vector() {
        // The canonical CRC32 test vector.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn crc32_empty_is_zero() {
        assert_eq!(crc32(b""), 0);
    }
}

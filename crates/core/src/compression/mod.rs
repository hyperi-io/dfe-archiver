// Project:   dfe-archiver
// File:      crates/core/src/compression/mod.rs
// Purpose:   Compression codec abstraction
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::{Error, Result};

/// Compression codec trait
pub trait Compressor {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Get file extension for this codec
    fn extension(&self) -> &'static str;

    /// Get codec name
    fn name(&self) -> &'static str;
}

/// No compression (passthrough)
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn extension(&self) -> &'static str {
        ""
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// Zstd compression
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    /// Create new Zstd compressor with given level (1-22, default 3)
    #[must_use]
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(1, 22),
        }
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self::new(3)
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(data, self.level)
            .map_err(|e| Error::Compression(format!("zstd compress failed: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::decode_all(data)
            .map_err(|e| Error::Compression(format!("zstd decompress failed: {e}")))
    }

    fn extension(&self) -> &'static str {
        "zst"
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}

/// LZ4 compression, one LZ4 frame per call, so a file of appended calls is a
/// standard concatenated-frame `.lz4` stream.
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;

        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder
            .write_all(data)
            .map_err(|e| Error::Compression(format!("lz4 compress failed: {e}")))?;
        encoder
            .finish()
            .map_err(|e| Error::Compression(format!("lz4 compress finish failed: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Read;

        // The decoder ends its stream at each frame's end, and a file holds one frame a flush.
        let mut rest = data;
        let mut decompressed = Vec::new();
        while !rest.is_empty() {
            lz4_flex::frame::FrameDecoder::new(&mut rest)
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::Compression(format!("lz4 decompress failed: {e}")))?;
        }
        Ok(decompressed)
    }

    fn extension(&self) -> &'static str {
        "lz4"
    }

    fn name(&self) -> &'static str {
        "lz4"
    }
}

/// Snappy compression in the snappy framing format, one framed stream per
/// call, so a file of appended calls is still one readable framed stream.
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;

        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder
            .write_all(data)
            .map_err(|e| Error::Compression(format!("snappy compress failed: {e}")))?;
        encoder
            .into_inner()
            .map_err(|e| Error::Compression(format!("snappy compress finish failed: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Read;

        let mut decompressed = Vec::new();
        snap::read::FrameDecoder::new(data)
            .read_to_end(&mut decompressed)
            .map_err(|e| Error::Compression(format!("snappy decompress failed: {e}")))?;
        Ok(decompressed)
    }

    fn extension(&self) -> &'static str {
        "snappy"
    }

    fn name(&self) -> &'static str {
        "snappy"
    }
}

/// Gzip compression
pub struct GzipCompressor {
    level: u32,
}

impl GzipCompressor {
    /// Create new Gzip compressor with given level (0-9, default 6)
    #[must_use]
    pub fn new(level: u32) -> Self {
        Self {
            level: level.min(9),
        }
    }
}

impl Default for GzipCompressor {
    fn default() -> Self {
        Self::new(6)
    }
}

impl Compressor for GzipCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.level));
        encoder
            .write_all(data)
            .map_err(|e| Error::Compression(format!("gzip compress failed: {e}")))?;
        encoder
            .finish()
            .map_err(|e| Error::Compression(format!("gzip compress finish failed: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::MultiGzDecoder;
        use std::io::Read;

        // Every flush appends a gzip member, and a file holds many.
        let mut decoder = MultiGzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| Error::Compression(format!("gzip decompress failed: {e}")))?;
        Ok(decompressed)
    }

    fn extension(&self) -> &'static str {
        "gz"
    }

    fn name(&self) -> &'static str {
        "gzip"
    }
}

/// Create compressor from codec name
pub fn create_compressor(codec: &str, level: i32) -> Result<Box<dyn Compressor + Send + Sync>> {
    match codec.to_lowercase().as_str() {
        "none" => Ok(Box::new(NoCompressor)),
        "zstd" => Ok(Box::new(ZstdCompressor::new(level))),
        "lz4" => Ok(Box::new(Lz4Compressor)),
        "snappy" => Ok(Box::new(SnappyCompressor)),
        "gzip" | "gz" => Ok(Box::new(GzipCompressor::new(level.unsigned_abs()))),
        _ => Err(Error::Compression(format!("unknown codec: {codec}"))),
    }
}

/// The compressor a [`crate::config::CompressionConfig`] asks for.
///
/// `enabled: false` means no compression whatever `codec` says. Every caller has
/// to go through here rather than reading `codec` directly: doing that made the
/// flag dead config, and because the default codec is `zstd`, `enabled: false`
/// produced zstd archives with nothing reporting the setting as discarded. An
/// unknown codec is still an error even when compression is off, so a typo
/// surfaces at startup rather than the day the flag is flipped on.
///
/// # Errors
///
/// [`Error::Compression`] when `codec` is not a known codec name.
pub fn compressor_for(
    config: &crate::config::CompressionConfig,
) -> Result<Box<dyn Compressor + Send + Sync>> {
    let selected = create_compressor(&config.codec, config.level)?;
    if config.enabled {
        Ok(selected)
    } else {
        Ok(Box::new(NoCompressor))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = b"hello world this is test data that should compress well when repeated hello world this is test data that should compress well when repeated";

    /// `enabled: false` must win over the codec. The default codec is `zstd`, so
    /// reading the codec alone meant `enabled: false` still produced compressed
    /// archives -- and nothing logged, metered or validated said the flag was
    /// ignored.
    #[test]
    fn compressor_for_disabled_is_none_regardless_of_codec() {
        for codec in ["zstd", "lz4", "snappy", "gzip"] {
            let config = crate::config::CompressionConfig {
                codec: codec.to_string(),
                level: 3,
                enabled: false,
            };
            let compressor = compressor_for(&config).expect("known codec");
            assert_eq!(
                compressor.name(),
                "none",
                "enabled: false with codec {codec} still compressed"
            );
            assert_eq!(
                compressor.compress(TEST_DATA).expect("compress"),
                TEST_DATA,
                "the disabled compressor must pass bytes through untouched"
            );
        }
    }

    #[test]
    fn compressor_for_enabled_uses_the_codec() {
        let config = crate::config::CompressionConfig {
            codec: "zstd".to_string(),
            level: 3,
            enabled: true,
        };
        assert_eq!(compressor_for(&config).expect("known codec").name(), "zstd");
    }

    /// An unknown codec is rejected even with compression off, so a typo shows
    /// up at startup rather than the day someone flips the flag on.
    #[test]
    fn compressor_for_rejects_an_unknown_codec_when_disabled() {
        let config = crate::config::CompressionConfig {
            codec: "zstdd".to_string(),
            level: 3,
            enabled: false,
        };
        assert!(compressor_for(&config).is_err());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let compressor = ZstdCompressor::default();
        let compressed = compressor.compress(TEST_DATA).expect("compress");
        let decompressed = compressor.decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, TEST_DATA);
        assert!(compressed.len() < TEST_DATA.len());
    }

    #[test]
    fn test_lz4_roundtrip() {
        let compressor = Lz4Compressor;
        let compressed = compressor.compress(TEST_DATA).expect("compress");
        let decompressed = compressor.decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_snappy_roundtrip() {
        let compressor = SnappyCompressor;
        let compressed = compressor.compress(TEST_DATA).expect("compress");
        let decompressed = compressor.decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_gzip_roundtrip() {
        let compressor = GzipCompressor::default();
        let compressed = compressor.compress(TEST_DATA).expect("compress");
        let decompressed = compressor.decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_no_compressor() {
        let compressor = NoCompressor;
        let compressed = compressor.compress(TEST_DATA).expect("compress");
        assert_eq!(compressed, TEST_DATA);
    }

    #[test]
    fn test_create_compressor() {
        assert!(create_compressor("zstd", 3).is_ok());
        assert!(create_compressor("lz4", 0).is_ok());
        assert!(create_compressor("snappy", 0).is_ok());
        assert!(create_compressor("gzip", 6).is_ok());
        assert!(create_compressor("none", 0).is_ok());
        assert!(create_compressor("invalid", 0).is_err());
    }

    #[test]
    fn test_compress_empty_input() {
        for codec in &["zstd", "lz4", "snappy", "gzip", "none"] {
            let c = create_compressor(codec, 0).expect(codec);
            let compressed = c
                .compress(b"")
                .unwrap_or_else(|e| panic!("{codec} compress empty: {e}"));
            let decompressed = c
                .decompress(&compressed)
                .unwrap_or_else(|e| panic!("{codec} decompress empty: {e}"));
            assert!(decompressed.is_empty(), "{codec} should roundtrip empty");
        }
    }

    #[test]
    fn test_compress_large_input() {
        let large = vec![b'X'; 2 * 1024 * 1024]; // 2MB
        for codec in &["zstd", "lz4", "snappy", "gzip"] {
            let c = create_compressor(codec, 0).expect(codec);
            let compressed = c
                .compress(&large)
                .unwrap_or_else(|e| panic!("{codec} compress large: {e}"));
            let decompressed = c
                .decompress(&compressed)
                .unwrap_or_else(|e| panic!("{codec} decompress large: {e}"));
            assert_eq!(decompressed.len(), large.len(), "{codec} roundtrip large");
        }
    }

    #[test]
    fn test_decompress_corrupted_data() {
        let garbage = b"this is not compressed data at all";
        for codec in &["zstd", "lz4", "snappy", "gzip"] {
            let c = create_compressor(codec, 0).expect(codec);
            let result = c.decompress(garbage);
            assert!(result.is_err(), "{codec} should error on corrupted data");
        }
    }

    /// A file is one compressed call per flush appended end to end, and every
    /// codec reads the whole of it back, not just the first flush.
    #[test]
    fn every_codec_reads_back_a_file_of_appended_flushes() {
        let flushes: [&[u8]; 4] = [
            b"{\"id\":0}\n",
            b"",
            b"{\"id\":1}\n{\"id\":2}\n",
            b"{\"id\":3}\n",
        ];
        for codec in ["zstd", "lz4", "snappy", "gzip", "none"] {
            let c = create_compressor(codec, 3).expect(codec);
            let file: Vec<u8> = flushes
                .iter()
                .flat_map(|flush| c.compress(flush).expect("compress"))
                .collect();
            assert_eq!(
                c.decompress(&file)
                    .unwrap_or_else(|e| panic!("{codec}: {e}")),
                flushes.concat(),
                "{codec} reads every flush"
            );
        }
    }

    #[test]
    fn test_compressor_extensions() {
        assert_eq!(
            create_compressor("zstd", 0).expect("zstd").extension(),
            "zst"
        );
        assert_eq!(create_compressor("lz4", 0).expect("lz4").extension(), "lz4");
        assert_eq!(
            create_compressor("snappy", 0).expect("snappy").extension(),
            "snappy"
        );
        assert_eq!(
            create_compressor("gzip", 0).expect("gzip").extension(),
            "gz"
        );
        assert_eq!(create_compressor("none", 0).expect("none").extension(), "");
    }
}

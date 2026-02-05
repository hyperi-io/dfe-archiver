// Project:   dfe-archiver
// File:      src/compression/mod.rs
// Purpose:   Compression codec abstraction
// Language:  Rust
//
// License:      FSL-1.1-ALv2
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

/// LZ4 compression
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(|e| Error::Compression(format!("lz4 decompress failed: {e}")))
    }

    fn extension(&self) -> &'static str {
        "lz4"
    }

    fn name(&self) -> &'static str {
        "lz4"
    }
}

/// Snappy compression
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = snap::raw::Encoder::new();
        encoder
            .compress_vec(data)
            .map_err(|e| Error::Compression(format!("snappy compress failed: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress_vec(data)
            .map_err(|e| Error::Compression(format!("snappy decompress failed: {e}")))
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
        use flate2::write::GzEncoder;
        use flate2::Compression;
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
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
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
        "gzip" | "gz" => Ok(Box::new(GzipCompressor::new(level as u32))),
        _ => Err(Error::Compression(format!("unknown codec: {codec}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = b"hello world this is test data that should compress well when repeated hello world this is test data that should compress well when repeated";

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
}

// Project:   dfe-archiver
// File:      benches/compression.rs
// Purpose:   Compression codec comparison benchmarks
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dfe_archiver::compression::create_compressor;
use std::hint::black_box;

/// Generate realistic JSON log data
fn generate_log_data(size_kb: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_kb * 1024);
    let template = r#"{"timestamp":"2026-02-03T12:00:00.000Z","level":"INFO","logger":"app.service","message":"Processing request","context":{"request_id":"abc-123","user_id":"user-456","method":"POST","path":"/api/v1/data","duration_ms":42}}"#;

    while data.len() < size_kb * 1024 {
        data.extend_from_slice(template.as_bytes());
        data.push(b'\n');
    }

    data.truncate(size_kb * 1024);
    data
}

fn bench_compression_ratios(c: &mut Criterion) {
    let sizes = [64, 256, 1024, 4096]; // KB
    let codecs = ["zstd", "lz4", "snappy", "gzip"];

    for size in sizes {
        let data = generate_log_data(size);

        let mut group = c.benchmark_group(format!("compress_{}kb", size));
        group.throughput(Throughput::Bytes(data.len() as u64));

        for codec in codecs {
            let compressor = create_compressor(codec, 3).expect(codec);

            group.bench_with_input(BenchmarkId::new(codec, size), &data, |b, data| {
                b.iter(|| compressor.compress(black_box(data)))
            });
        }

        group.finish();
    }
}

fn bench_decompression(c: &mut Criterion) {
    let data = generate_log_data(1024); // 1MB
    let codecs = ["zstd", "lz4", "snappy", "gzip"];

    // Pre-compress data
    let compressed: Vec<(&str, Vec<u8>)> = codecs
        .iter()
        .map(|&codec| {
            let compressor = create_compressor(codec, 3).expect(codec);
            let compressed = compressor.compress(&data).expect("compress");
            (codec, compressed)
        })
        .collect();

    let mut group = c.benchmark_group("decompress_1mb");

    for (codec, compressed_data) in &compressed {
        let compressor = create_compressor(codec, 3).expect(codec);

        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(*codec, "1mb"),
            compressed_data,
            |b, compressed| b.iter(|| compressor.decompress(black_box(compressed))),
        );
    }

    group.finish();

    // Print compression ratios
    println!("\nCompression ratios (1MB JSON log data):");
    for (codec, compressed_data) in &compressed {
        let ratio = data.len() as f64 / compressed_data.len() as f64;
        let percentage = (1.0 - compressed_data.len() as f64 / data.len() as f64) * 100.0;
        println!(
            "  {}: {} -> {} bytes ({:.1}x, {:.1}% reduction)",
            codec,
            data.len(),
            compressed_data.len(),
            ratio,
            percentage
        );
    }
}

criterion_group!(benches, bench_compression_ratios, bench_decompression);
criterion_main!(benches);

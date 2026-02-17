// Project:   dfe-archiver
// File:      benches/throughput.rs
// Purpose:   Throughput benchmarks
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::RoutingConfig;
use dfe_archiver::routing::Router;
use std::hint::black_box;

/// Generate test JSON data
fn generate_test_data(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "org_id": format!("org-{}", i % 10),
                "event_type": "benchmark",
                "timestamp": "2026-02-03T12:00:00Z",
                "data": {
                    "field1": "value1",
                    "field2": 123,
                    "nested": {
                        "key": "value"
                    }
                }
            })
            .to_string()
            .into_bytes()
        })
        .collect()
}

fn bench_compression(c: &mut Criterion) {
    let data = generate_test_data(1000);
    let combined: Vec<u8> = data.iter().flat_map(|d| d.iter().copied()).collect();

    let mut group = c.benchmark_group("compression");
    group.throughput(Throughput::Bytes(combined.len() as u64));

    // Zstd compression
    let zstd = create_compressor("zstd", 3).expect("zstd");
    group.bench_function("zstd_level3", |b| {
        b.iter(|| zstd.compress(black_box(&combined)))
    });

    // LZ4 compression
    let lz4 = create_compressor("lz4", 0).expect("lz4");
    group.bench_function("lz4", |b| b.iter(|| lz4.compress(black_box(&combined))));

    // Snappy compression
    let snappy = create_compressor("snappy", 0).expect("snappy");
    group.bench_function("snappy", |b| {
        b.iter(|| snappy.compress(black_box(&combined)))
    });

    // Gzip compression
    let gzip = create_compressor("gzip", 6).expect("gzip");
    group.bench_function("gzip_level6", |b| {
        b.iter(|| gzip.compress(black_box(&combined)))
    });

    group.finish();
}

fn bench_routing(c: &mut Criterion) {
    use dfe_archiver::kafka::KafkaMessage;

    let data = generate_test_data(1000);
    let messages: Vec<KafkaMessage> = data
        .into_iter()
        .enumerate()
        .map(|(i, payload)| KafkaMessage::for_test(payload, "benchmark-topic", 0, i as i64))
        .collect();

    let mut group = c.benchmark_group("routing");
    group.throughput(Throughput::Elements(messages.len() as u64));

    // Topic-based routing
    let topic_router = Router::new(RoutingConfig {
        mode: "topic".to_string(),
        ..Default::default()
    });

    group.bench_function("by_topic", |b| {
        b.iter(|| {
            for msg in &messages {
                black_box(topic_router.route(msg).expect("route"));
            }
        })
    });

    // Expression-based routing
    let expr_router = Router::new(RoutingConfig {
        mode: "expression".to_string(),
        expression_fields: vec!["org_id".to_string(), "event_type".to_string()],
        default_segment: "unknown".to_string(),
    });

    group.bench_function("by_expression", |b| {
        b.iter(|| {
            for msg in &messages {
                black_box(expr_router.route(msg).expect("route"));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_compression, bench_routing);
criterion_main!(benches);

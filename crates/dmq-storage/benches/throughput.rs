use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use dmq_storage::partition_log::PartitionLog;

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_log_append");
    group.throughput(Throughput::Bytes(64));
    group.bench_function("append_64b", |b| {
        let mut log = PartitionLog::new();
        let payload = vec![0u8; 64];
        b.iter(|| {
            log.append(black_box(&payload));
        });
    });
    group.finish();
}

criterion_group!(benches, bench_append);
criterion_main!(benches);

//! Benchmarks for evaluating various ODB storage systems for blob storage.
use criterion::{
    criterion_group, criterion_main, BatchSize, Bencher, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use spacetimedb_sats::flat::offset_map::OffsetMap;
use spacetimedb_sats::flat::raw_page::{BufferOffset, PageOffset};
use spacetimedb_sats::flat::table::RowHash;
use std::time::{Duration, Instant};

type RngMut<'r> = &'r mut ThreadRng;

fn gen_buffer_offset(rng: RngMut<'_>) -> BufferOffset {
    let page = rng.gen::<usize>();
    let page_offset = PageOffset(rng.gen::<u16>());
    BufferOffset::new(page, page_offset)
}

fn gen_row_hash(rng: RngMut<'_>, max_range: u64) -> RowHash {
    RowHash(rng.gen_range(0..max_range))
}

fn gen_hash_and_offsets(rng: RngMut<'_>, max: u64, count: usize) -> impl '_ + Iterator<Item = (RowHash, BufferOffset)> {
    (0..count).map(move |_| (gen_row_hash(rng, max), gen_buffer_offset(rng)))
}

fn bench_insert(c: &mut Criterion) {
    let mut bench_group = c.benchmark_group("offset_map_insert");
    bench_group.throughput(Throughput::Elements(1));
    let bench_insert_inner = |bench: &mut Bencher<'_, _>, collision_ratio: &f64| {
        bench.iter_custom(|iters| {
            let mut rng = thread_rng();
            let num_inserts_per_map = 1000;
            let preload_amt = 10_000;
            let n = (preload_amt + num_inserts_per_map) as f64;
            let max_range = -1.0 / (-1.0 + f64::powf(1.0 - *collision_ratio, 1.0 / (-1.0 + n)));
            let max_range = if max_range.is_finite() {
                max_range as u64
            } else {
                u64::MAX
            };
            let mut total_duration = Duration::from_secs(0);

            let mut num_iters = 0;
            while num_iters < iters {
                let mut map = gen_hash_and_offsets(&mut rng, max_range, preload_amt).collect::<OffsetMap>();
                for val in gen_hash_and_offsets(&mut rng, max_range, num_inserts_per_map) {
                    // Compute duration of offset map insertion.
                    let start = Instant::now();
                    map.insert(val.0, val.1);
                    total_duration += start.elapsed();

                    num_iters += 1;
                    if num_iters >= iters {
                        break;
                    }
                }
                // println!("{}, {}, {}", map.len(), map.num_collisions(), map.num_non_collisions());
                drop(map);
            }
            total_duration
        });
    };

    bench_group.throughput(Throughput::Elements(8));
    bench_group.bench_with_input(
        BenchmarkId::new("load/10_000/insert/1000/collisions", "0%"),
        &0.00_f64,
        bench_insert_inner,
    );
    bench_group.bench_with_input(
        BenchmarkId::new("load/10_000/insert/1000/collisions", "1%"),
        &0.01_f64,
        bench_insert_inner,
    );
    bench_group.bench_with_input(
        BenchmarkId::new("load/10_000/insert/1000/collisions", "10%"),
        &0.10_f64,
        bench_insert_inner,
    );
    bench_group.bench_with_input(
        BenchmarkId::new("load/10_000/insert/1000/collisions", "50%"),
        &0.50_f64,
        bench_insert_inner,
    );
    bench_group.bench_with_input(
        BenchmarkId::new("load/10_000/insert/1000/collisions", "100%"),
        &0.50_f64,
        bench_insert_inner,
    );

    bench_group.finish();
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);

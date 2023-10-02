//! Benchmarks for evaluating various ODB storage systems for blob storage.
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkGroup, BenchmarkId, Criterion, Throughput, BatchSize};
use rand::rngs::ThreadRng;
use rand::{Rng, thread_rng};
use spacetimedb_sats::flat::raw_page::{BufferOffset, PageOffset};
use spacetimedb_sats::flat::table::RowHash;
use std::time::{Instant, Duration};
use spacetimedb_sats::flat::offset_map::OffsetMap;


fn bench_insert(c: &mut Criterion) {
    let mut bench_group = c.benchmark_group("offset_map_insert");
    bench_group.throughput(Throughput::Elements(1));
    let bench_insert_inner = |bench: &mut Bencher<'_, _>, collision_ratio: &f64| {
        let setup_map = |rng: &mut ThreadRng, max_range: u64, preload_amt: usize| {
            let mut map = OffsetMap::new();
            for _ in 0..preload_amt {
                let hash = RowHash(rng.gen_range(0..max_range));
                let page = rng.gen::<usize>();
                let page_offset = PageOffset(rng.gen::<u16>());
                let offset = BufferOffset::new(page, page_offset);
                map.insert(hash, offset)
            }
            map
        };
        let gen_hashes = |rng: &mut ThreadRng, max_range: u64, num_hashes: usize| {
            let mut hashes = Vec::new();
            for _ in 0..num_hashes {
                let hash = RowHash(rng.gen_range(0..max_range));
                let page = rng.gen::<usize>();
                let page_offset = PageOffset(rng.gen::<u16>());
                let offset = BufferOffset::new(page, page_offset);
                hashes.push((hash, offset));
            }
            hashes
        };
        bench.iter_custom(|iters| {
            let mut rng = thread_rng();
            let num_inserts_per_map = 1000;
            let preload_amt = 10_000;
            let n = (preload_amt + num_inserts_per_map) as f64;
            let max_range = -1.0 / (-1.0 + f64::powf(1.0 - *collision_ratio, 1.0 / (-1.0 + n)));
            let max_range = if max_range.is_finite() { max_range as u64 } else { u64::MAX };
            let mut total_duration = Duration::from_secs(0);

            let mut num_iters = 0;
            while num_iters < iters {
                let mut map = setup_map(&mut rng, max_range, preload_amt);
                let hashes = gen_hashes(&mut rng, max_range, num_inserts_per_map);
                for val in hashes {
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
    bench_group.bench_with_input(BenchmarkId::new("load/10_000/insert/1000/collisions", "0%"), &0.00_f64, bench_insert_inner);
    bench_group.bench_with_input(BenchmarkId::new("load/10_000/insert/1000/collisions", "1%"), &0.01_f64, bench_insert_inner);
    bench_group.bench_with_input(BenchmarkId::new("load/10_000/insert/1000/collisions", "10%"), &0.10_f64, bench_insert_inner);
    bench_group.bench_with_input(BenchmarkId::new("load/10_000/insert/1000/collisions", "50%"), &0.50_f64, bench_insert_inner);
    bench_group.bench_with_input(BenchmarkId::new("load/10_000/insert/1000/collisions", "100%"), &0.50_f64, bench_insert_inner);

    bench_group.finish();
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);

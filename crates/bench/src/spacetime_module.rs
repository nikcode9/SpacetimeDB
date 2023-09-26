use spacetimedb::db::{Config, FsyncPolicy, Storage};
use spacetimedb_lib::{sats::ArrayValue, AlgebraicValue, ProductValue};
use spacetimedb_testing::modules::{start_runtime, CompiledModule, ModuleHandle};
use tokio::runtime::Runtime;

use crate::{
    database::BenchDatabase,
    schemas::{snake_case_table_name, table_name, BenchTable},
    ResultBench,
};

lazy_static::lazy_static! {
    pub static ref BENCHMARKS_MODULE: CompiledModule =
        CompiledModule::compile("benchmarks");
}

/// A benchmark backend that invokes a spacetime module.
/// This is tightly tied to the file `modules/benchmarks/src/lib.rs`;
/// all of the implementations of `BenchDatabase` methods just invoke reducers
/// in that module.
pub struct SpacetimeModule {
    runtime: Runtime,
    // it's necessary for this to be dropped BEFORE the runtime.
    // this is always Some when drop isn't running.
    module: Option<ModuleHandle>,
}

impl Drop for SpacetimeModule {
    fn drop(&mut self) {
        // enforce module being dropped
        drop(self.module.take());
    }
}

// Note: we use block_on for the methods here. It adds about 70ns of overhead.
// This isn't currently a problem. Overhead to call an empty reducer is currently 20_000 ns.
// It's easier to do it this way because async trades are a mess.
impl BenchDatabase for SpacetimeModule {
    fn name() -> &'static str {
        "stdb_module"
    }

    type TableId = TableId;

    fn build(in_memory: bool, fsync: bool) -> ResultBench<Self>
    where
        Self: Sized,
    {
        let runtime = start_runtime();
        let config = Config {
            fsync: if fsync {
                FsyncPolicy::EveryTx
            } else {
                FsyncPolicy::Never
            },
            storage: if in_memory { Storage::Memory } else { Storage::Disk },
        };
        let module = runtime.block_on(async { BENCHMARKS_MODULE.load_module(config).await });

        for thing in module.client.module.catalog().iter() {
            log::trace!("SPACETIME_MODULE: LOADED: {} {:?}", thing.0, thing.1.ty());
        }
        Ok(SpacetimeModule {
            runtime,
            module: Some(module),
        })
    }

    #[inline(never)]
    fn create_table<T: BenchTable>(&mut self, table_style: crate::schemas::TableStyle) -> ResultBench<Self::TableId> {
        // Noop. All tables are built into the "benchmarks" module.
        Ok(TableId {
            pascal_case: table_name::<T>(table_style),
            snake_case: snake_case_table_name::<T>(table_style),
        })
    }

    #[inline(never)]
    fn clear_table(&mut self, table_id: &Self::TableId) -> ResultBench<()> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();
        runtime.block_on(async move {
            // FIXME: this doesn't work. delete is unimplemented!!
            /*
            let name = format!("clear_table_{}", table_id.snake_case);
            module.call_reducer_binary(&name, ProductValue::new(&[])).await?;
            */
            // workaround for now
            module.client.module.clear_table(table_id.pascal_case.clone()).await?;
            Ok(())
        })
    }

    // Implemented by calling a reducer that logs, then looking for the resulting
    // message in the log.
    // This implementation will not work if other people are concurrently interacting with our module.
    #[inline(never)]
    fn count_table(&mut self, table_id: &Self::TableId) -> ResultBench<u32> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();

        let count = runtime.block_on(async move {
            let name = format!("count_{}", table_id.snake_case);
            module.call_reducer_binary(&name, ProductValue::new(&[])).await?;
            let logs = module.read_log(Some(1)).await;
            let message = serde_json::from_str::<LoggerRecord>(&logs)?;
            if !message.message.starts_with("COUNT: ") {
                anyhow::bail!("Improper count message format: {:?}", message.message);
            }

            let count = message.message["COUNT: ".len()..].parse::<u32>()?;
            Ok(count)
        })?;
        Ok(count)
    }

    #[inline(never)]
    fn empty_transaction(&mut self) -> ResultBench<()> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();

        runtime.block_on(async move {
            module.call_reducer_binary("empty", ProductValue::new(&[])).await?;
            Ok(())
        })
    }

    #[inline(never)]
    fn insert<T: BenchTable>(&mut self, table_id: &Self::TableId, row: T) -> ResultBench<()> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();
        let reducer_name = format!("insert_{}", table_id.snake_case);

        runtime.block_on(async move {
            module
                .call_reducer_binary(&reducer_name, row.into_product_value())
                .await?;
            Ok(())
        })
    }

    #[inline(never)]
    fn insert_bulk<T: BenchTable>(&mut self, table_id: &Self::TableId, rows: Vec<T>) -> ResultBench<()> {
        let args = ProductValue {
            elements: vec![AlgebraicValue::Builtin(spacetimedb_lib::sats::BuiltinValue::Array {
                val: ArrayValue::Product(rows.into_iter().map(|row| row.into_product_value()).collect()),
            })],
        };
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();
        let reducer_name = format!("insert_bulk_{}", table_id.snake_case);

        runtime.block_on(async move {
            module.call_reducer_binary(&reducer_name, args).await?;
            Ok(())
        })
    }

    #[inline(never)]
    fn iterate(&mut self, table_id: &Self::TableId) -> ResultBench<()> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();
        let reducer_name = format!("iterate_{}", table_id.snake_case);

        runtime.block_on(async move {
            module
                .call_reducer_binary(&reducer_name, ProductValue::new(&[]))
                .await?;
            Ok(())
        })
    }

    #[inline(never)]
    fn filter<T: BenchTable>(
        &mut self,
        table_id: &Self::TableId,
        column_index: u32,
        value: AlgebraicValue,
    ) -> ResultBench<()> {
        let SpacetimeModule { runtime, module } = self;
        let module = module.as_mut().unwrap();

        let product_type = T::product_type();
        let column_name = product_type.elements[column_index as usize].name.as_ref().unwrap();
        let reducer_name = format!("filter_{}_by_{}", table_id.snake_case, column_name);

        runtime.block_on(async move {
            module
                .call_reducer_binary(&reducer_name, ProductValue { elements: vec![value] })
                .await?;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct TableId {
    pascal_case: String,
    snake_case: String,
}

#[allow(unused)]
/// Sync with: `core::database_logger::Record`.
#[derive(serde::Deserialize)]
struct LoggerRecord {
    target: Option<String>,
    filename: Option<String>,
    line_number: Option<u32>,
    message: String,
}

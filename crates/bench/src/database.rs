use spacetimedb::db::datastore::traits::TableSchema;
use spacetimedb_lib::AlgebraicValue;
use spacetimedb_primitives::ColId;

use crate::schemas::{BenchTable, IndexStrategy};
use crate::ResultBench;

/// A database we can execute a standard benchmark suite against.
/// Currently implemented for SQLite, raw Spacetime outside a module boundary
/// (RelationalDB), and Spacetime through the module boundary.
///
/// Not all benchmarks have to go through this trait.
pub trait BenchDatabase: Sized {
    fn name() -> &'static str;

    type TableId: Clone + 'static;

    fn build(in_memory: bool, fsync: bool) -> ResultBench<Self>
    where
        Self: Sized;

    fn create_table<T: BenchTable>(&mut self, table_style: IndexStrategy) -> ResultBench<Self::TableId>;

    /// Return table metadata so we can remove this from the hot path
    fn get_table<T: BenchTable>(&mut self, table_id: &Self::TableId) -> ResultBench<TableSchema>;

    /// Should not drop the table, only delete all the rows.
    fn clear_table(&mut self, table_id: &Self::TableId) -> ResultBench<()>;

    /// Count the number of rows in the table.
    fn count_table(&mut self, table_id: &Self::TableId) -> ResultBench<u32>;

    /// Perform an empty transaction.
    fn empty_transaction(&mut self) -> ResultBench<()>;

    /// Perform a transaction that commits a single row.
    fn insert<T: BenchTable>(&mut self, table_id: &Self::TableId, row: T) -> ResultBench<()>;

    /// Perform a transaction that commits many rows.
    fn insert_bulk<T: BenchTable>(&mut self, table_id: &Self::TableId, rows: Vec<T>) -> ResultBench<()>;

    /// Perform a transaction that iterates an entire database table.
    /// Note: this can be non-generic because none of the implementations use the relevant generic argument.
    fn iterate(&mut self, table_id: &Self::TableId) -> ResultBench<()>;

    /// Filter the table on the specified column index for the specified value.
    fn filter<T: BenchTable>(
        &mut self,
        table: &TableSchema,
        column_index: ColId,
        value: AlgebraicValue,
    ) -> ResultBench<()>;

    /// Perform a `SELECT * FROM table`
    /// Note: this can be non-generic because none of the implementations use the relevant generic argument.
    fn sql_select(&mut self, table: &TableSchema) -> ResultBench<()>;

    /// Perform a `SELECT * FROM table WHERE column = value`
    /// Note: this can be non-generic because none of the implementations use the relevant generic argument.
    fn sql_where<T: BenchTable>(
        &mut self,
        table: &TableSchema,
        column_index: ColId,
        value: AlgebraicValue,
    ) -> ResultBench<()>;
}

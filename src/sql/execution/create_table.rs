use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::sql::CommandRunner;
use crate::storage::table_metadata::{TableMetadata, TableSchema};
use crate::storage::{ColumnDef, OutputTable, TableDef};

impl CommandRunner {
    pub fn create_table(
        table_def: &TableDef,
        columns: Vec<ColumnDef>,
        engine: EngineName,
        order_by: Vec<ColumnDef>,
    ) -> Result<OutputTable> {
        let table_schema = TableSchema { columns, order_by };
        let table_metadata = TableMetadata::try_new(table_schema, engine)?;

        // TODO: make atomic

        std::fs::create_dir(table_def.get_path()).map_err(|_| Error::InvalidTable)?;

        table_metadata.write_to(table_def)?;

        Ok(OutputTable::build_ok())
    }
}

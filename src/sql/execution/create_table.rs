use dashmap::Entry;
use log::error;

use crate::error::{Error, Result};
use crate::runtime_config::{TABLE_DATA, TableConfig};
use crate::sql::CommandRunner;
use crate::storage::table_metadata::{TABLE_METADATA_FILENAME, TABLE_METADATA_MAGIC_BYTES};
use crate::storage::{ColumnDef, OutputTable, TableDef, write_file_with_crc};
use crate::storage::{TableMetadata, TableSchema, TableSettings};

impl CommandRunner {
    /// Creates a table with metadata and filesystem structures.
    ///
    /// Atomically reserves table entry in memory, creates directory, and writes metadata.
    ///
    /// Returns:
    ///   * Ok: `OutputTable` with success status
    ///   * Error: `TableEntryAlreadyExists` or `CouldNotInsertData` on failure
    pub fn create_table(
        table_def: TableDef,
        columns: Vec<ColumnDef>,
        settings: TableSettings,
        order_by: Vec<ColumnDef>,
        primary_key: Vec<ColumnDef>,
    ) -> Result<OutputTable> {
        let table_schema = TableSchema {
            columns,
            order_by,
            primary_key,
        };
        let table_metadata = TableMetadata::try_new(table_schema, settings)?;

        let table_path = table_def.get_path();
        // will lock for mutual access
        let Entry::Vacant(entry) = TABLE_DATA.entry(table_def) else {
            return Err(Error::TableAlreadyExists);
        };

        std::fs::create_dir(&table_path)
            .map_err(|e| Error::CouldNotCreateTable(format!("Failed to create table dir: {e}")))?;
        let path = table_path.join(TABLE_METADATA_FILENAME);
        if let Err(error) = write_file_with_crc(&table_metadata, &path, TABLE_METADATA_MAGIC_BYTES)
        {
            if let Err(cleanup_err) = std::fs::remove_dir_all(table_path) {
                error!("Failed to cleanup directory after metadata write failure: {cleanup_err}",);
            }
            return Err(error);
        }
        let table_config = TableConfig {
            metadata: table_metadata,
            infos: Vec::new(),
        };

        entry.insert(table_config);

        Ok(OutputTable::build_ok())
    }
}

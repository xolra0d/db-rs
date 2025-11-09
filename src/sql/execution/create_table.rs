use scc::hash_index::Entry;

use crate::error::{Error, Result};
use crate::runtime_config::{TABLE_DATA, TableConfig};
use crate::sql::CommandRunner;
use crate::storage::{ColumnDef, OutputTable, TableDef};
use crate::storage::{TableMetadata, TableSchema, TableSettings};

impl CommandRunner {
    /// Creates a table with metadata and filesystem structures.
    ///
    /// Atomically reserves table entry in memory, creates directory, and writes metadata.
    ///
    /// Returns:
    ///   * Ok: OutputTable with success status
    ///   * Error: TableEntryAlreadyExists or CouldNotInsertData on failure
    pub fn create_table(
        table_def: TableDef,
        columns: Vec<ColumnDef>,
        settings: TableSettings,
        order_by: Vec<ColumnDef>,
    ) -> Result<OutputTable> {
        let table_schema = TableSchema { columns, order_by };
        let table_metadata = TableMetadata::try_new(table_schema, settings)?;

        match TABLE_DATA.entry_sync(table_def.clone()) {
            Entry::Occupied(_) => {
                // Table already exists
                return Err(Error::TableEntryAlreadyExists);
            }
            Entry::Vacant(vacant_entry) => {
                // Now we have exclusive access
                std::fs::create_dir(table_def.get_path()).map_err(|e| {
                    Error::CouldNotInsertData(format!("Failed to create table dir: {}", e))
                })?;

                if let Err(e) = table_metadata.write_to(&table_def) {
                    if let Err(cleanup_err) = std::fs::remove_dir_all(table_def.get_path()) {
                        // Log cleanup failure but still return the original error
                        log::error!(
                            "Warning: Failed to cleanup directory after metadata write failure: {}",
                            cleanup_err
                        );
                    }
                    return Err(e);
                }

                vacant_entry.insert_entry(TableConfig {
                    metadata: table_metadata,
                    indexes: Vec::new(),
                });
            }
        }

        Ok(OutputTable::build_ok())
    }
}

use scc::hash_index::Entry;

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
    ///   * Ok: OutputTable with success status
    ///   * Error: TableEntryAlreadyExists or CouldNotInsertData on failure
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

        match TABLE_DATA.entry_sync(table_def.clone()) {
            Entry::Occupied(_) => {
                return Err(Error::TableEntryAlreadyExists);
            }
            Entry::Vacant(vacant_entry) => {
                // Now we have exclusive access
                let table_path = table_def.get_path();
                std::fs::create_dir(&table_path).map_err(|e| {
                    Error::CouldNotInsertData(format!("Failed to create table dir: {}", e))
                })?;
                let path = table_path.join(TABLE_METADATA_FILENAME);
                if let Err(e) =
                    write_file_with_crc(&table_metadata, &path, TABLE_METADATA_MAGIC_BYTES)
                {
                    if let Err(cleanup_err) = std::fs::remove_dir_all(table_def.get_path()) {
                        log::error!(
                            "Warning: Failed to cleanup directory after metadata write failure: {}",
                            cleanup_err
                        );
                    }
                    return Err(e);
                }

                vacant_entry.insert_entry(TableConfig {
                    metadata: table_metadata,
                    infos: Vec::new(),
                });
            }
        }

        Ok(OutputTable::build_ok())
    }
}

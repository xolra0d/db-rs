use scc::Guard;

use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::CommandRunner;
use crate::storage::{Column, OutputTable, TableDef, TablePart};

impl CommandRunner {
    /// Executes INSERT operation by creating and persisting a new table part.
    ///
    /// Creates a new part, saves it to raw directory, then atomically moves to normal directory.
    /// Which results in atomic inserts.
    ///
    /// Returns:
    ///   * Ok: OutputTable with success status
    ///   * Error: TableNotFound or CouldNotInsertData on persistence failure
    pub fn insert(table_def: TableDef, columns: Vec<Column>) -> Result<OutputTable> {
        let guard = Guard::new();
        let Some(table_config) = TABLE_DATA.peek(&table_def, &guard) else {
            return Err(Error::TableNotFound);
        };

        let (table_part, data) = TablePart::try_new(&table_config.metadata, columns)?;

        table_part.save_raw(&table_def, &data)?;

        let move_result = table_part.move_to_normal(&table_def);
        if move_result.is_ok() {
            return Ok(OutputTable::build_ok());
        }

        if let Err(cleanup_err) = table_part.remove_raw(&table_def) {
            // Log cleanup failure, but don't override original error
            log::error!("Warning: Failed to clean up raw data: {}", cleanup_err);
        }

        Err(Error::CouldNotInsertData(
            move_result.unwrap_err().to_string(),
        ))
    }
}

use crate::error::{Error, Result};
use crate::sql::CommandRunner;
use crate::storage::{Column, OutputTable, TableDef, TablePart};

impl CommandRunner {
    /// Executes INSERT operation by creating and persisting a new table part.
    ///
    /// Creates a new part, saves it to raw directory, then atomically moves to normal directory.
    /// Which results in atomic inserts.
    ///
    /// Returns:
    ///   * Ok: `OutputTable` with success status
    ///   * Error: `TableNotFound` or `CouldNotInsertData` on persistence failure
    pub fn insert(table_def: &TableDef, columns: Vec<Column>) -> Result<OutputTable> {
        let mut table_part = TablePart::try_new(table_def, columns, None)?;

        table_part.save_raw(table_def)?;

        let move_result = table_part.move_to_normal(table_def);
        if move_result.is_ok() {
            return Ok(OutputTable::build_ok());
        }

        Err(Error::CouldNotInsertData(
            move_result.unwrap_err().to_string(),
        ))
    }
}

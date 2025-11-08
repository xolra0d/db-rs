use crate::CONFIG;
use crate::error::{Error, Result};
use crate::sql::CommandRunner;
use crate::storage::OutputTable;

impl CommandRunner {
    /// Creates a database directory.
    ///
    /// Returns:
    ///   * Ok: OutputTable with success status
    ///   * Error: InvalidDatabaseName if directory creation fails
    pub fn create_database(name: String) -> Result<OutputTable> {
        std::fs::create_dir(CONFIG.get_db_dir().join(name))
            .map_err(|_| Error::InvalidDatabaseName)?; // rethink error

        Ok(OutputTable::build_ok())
    }
}

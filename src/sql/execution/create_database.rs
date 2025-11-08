use crate::CONFIG;
use crate::error::{Error, Result};
use crate::sql::{CommandRunner, validate_name};
use crate::storage::OutputTable;

impl CommandRunner {
    /// Creates a database directory.
    ///
    /// Returns:
    ///   * Ok: OutputTable with success status
    ///   * Error: InvalidDatabaseName if directory creation fails
    pub fn create_database(name: String) -> Result<OutputTable> {
        if !validate_name(&name) {
            return Err(Error::InvalidDatabaseName);
        }
        std::fs::create_dir(CONFIG.get_db_dir().join(name)).map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => Error::DatabaseAlreadyExists,
            std::io::ErrorKind::PermissionDenied => Error::PermissionDenied,
            _ => Error::InvalidDatabaseName,
        })?;

        Ok(OutputTable::build_ok())
    }
}

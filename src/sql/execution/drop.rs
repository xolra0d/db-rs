use crate::config::CONFIG;
use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::CommandRunner;
use crate::storage::{OutputTable, TableDef};

impl CommandRunner {
    pub fn drop_table(table_def: &TableDef, if_exists: bool) -> Result<OutputTable> {
        let lock = TABLE_DATA.remove(table_def);

        if lock.is_none() && if_exists {
            return Ok(OutputTable::build_ok());
        } else if lock.is_none() && !if_exists {
            return Err(Error::TableNotFound);
        }
        let table_path = table_def.get_path();

        let remove_result = std::fs::remove_dir_all(&table_path);
        match (remove_result, if_exists) {
            (Ok(()), _) => Ok(OutputTable::build_ok()),
            (Err(error), true) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(OutputTable::build_ok())
            }
            (Err(error), false) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::TableNotFound)
            }
            (Err(error), _) => Err(Error::Internal(format!(
                "Could not remove table entry from disk: {}. Stop database, remove {:?} folder, and restart the database.",
                error,
                std::path::absolute(&table_path).unwrap_or(table_path),
            ))),
        }
    }

    pub fn drop_database(name: &str, if_exists: bool) -> Result<OutputTable> {
        TABLE_DATA.retain(|x, _| x.database != name);

        let remove_result = std::fs::remove_dir_all(CONFIG.get_db_dir().join(name));
        match (remove_result, if_exists) {
            (Ok(()), _) => Ok(OutputTable::build_ok()),
            (Err(error), true) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(OutputTable::build_ok())
            }
            (Err(error), false) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::DatabaseNotFound)
            }
            (Err(error), _) => Err(Error::Internal(format!(
                "Could not remove database entry from disk: {}. Stop database, remove {:?} folder, and restart the database.",
                error,
                std::path::absolute(CONFIG.get_db_dir().join(name)).unwrap_or(CONFIG.get_db_dir().join(name)),
            ))),
        }
    }
}

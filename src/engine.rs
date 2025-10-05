use crate::commands;
use crate::protocol::{Command, CommandError, CommandResult};
use crate::storage::TableMetadata;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct TableSpecifier {
    db_name: String,
    table_name: Option<String>,
}

impl From<TableSpecifier> for PathBuf {
    fn from(table_specifier: TableSpecifier) -> Self {
        let mut path = Self::from(table_specifier.db_name);

        if let Some(table_name) = table_specifier.table_name {
            path.push(table_name);
        }

        path
    }
}

impl From<&TableSpecifier> for PathBuf {
    fn from(table_specifier: &TableSpecifier) -> Self {
        let mut path = Self::from(&table_specifier.db_name);

        if let Some(table_name) = table_specifier.table_name.as_ref() {
            path.push(table_name);
        }
        path
    }
}

impl TableSpecifier {
    pub fn new(db_name: &str, table_name: Option<&str>) -> Self {
        Self {
            db_name: db_name.to_string(),
            table_name: table_name.map(std::string::ToString::to_string),
        }
    }

    pub fn exists(&self, engine: &Engine) -> bool {
        engine.get_db_dir().join(PathBuf::from(self)).exists()
    }
}

/// Main engine struct which executes received command.
#[derive(Debug)]
pub struct Engine {
    /// Path to the database location.
    db_dir: Arc<PathBuf>,
    /// `HashSet` of all tables under write operation (meaning not allowed to access for other connections)
    in_write_tables: Arc<RwLock<HashSet<TableSpecifier>>>,
    /// If `in_write_tables` is poisoned (look Rwlock poisoned) stop processing new requests and stop program.
    is_poisoned: Arc<AtomicBool>,
}

impl Clone for Engine {
    /// Creates new Engine struct by increasing the strong reference count of Arc.
    fn clone(&self) -> Self {
        Self {
            db_dir: Arc::clone(&self.db_dir),
            in_write_tables: Arc::clone(&self.in_write_tables),
            is_poisoned: Arc::clone(&self.is_poisoned),
        }
    }
}

impl Engine {
    /// Returns reference for database directory.
    pub const fn get_db_dir(&self) -> &Arc<PathBuf> {
        &self.db_dir
    }

    pub fn is_poisoned(&self) -> bool {
        self.is_poisoned.load(Ordering::Acquire)
    }

    /// Poison the engine to stop accepting new commands and connections
    pub fn poison(&self) {
        self.is_poisoned.store(true, Ordering::Release);
    }

    /// Creates another engine by increasing the strong reference count of Arc
    pub fn new(db_dir: PathBuf) -> Self {
        Self {
            db_dir: Arc::new(db_dir),
            in_write_tables: Arc::new(RwLock::new(HashSet::new())),
            is_poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Expects `Command::Array`.If command is not known returns error: `CommandError::InvalidCommandName`. Commands are case-insensitive.
    pub async fn execute_command(&self, command: Command) -> CommandResult<Command> {
        // Check if engine is poisoned before processing any commands
        if self.is_poisoned() {
            return Err(CommandError::Poisoned);
        }

        let array = command
            .get_array()
            .ok_or_else(|| CommandError::ExecutionError("Expected array command format".into()))?;

        if array.is_empty() {
            return Err(CommandError::ExecutionError("Empty command array".into()));
        }

        match &array[0] {
            Command::String(data) => {
                let command_name = data.to_ascii_lowercase();

                commands::get_command(&command_name).map_or_else(
                    || Err(CommandError::InvalidCommandName(data.clone())),
                    |command| command(&array[1..], self),
                )
            }
            Command::Array(_) => Err(CommandError::ExecutionError(
                "Nested arrays are not supported (currently) in command structure".into(),
            )),
        }
    }

    pub fn get_table_fields(
        &self,
        table_specifier: &TableSpecifier,
    ) -> CommandResult<Vec<(String, FieldType)>> {
        let path = self.get_db_dir().join(PathBuf::from(table_specifier));
        let metadata_path = path.join(".metadata");

        let mut file = File::open(&metadata_path).map_err(|e| {
            CommandError::ExecutionError(format!(
                "Failed to open metadata file '{}': {}",
                metadata_path.display(),
                e
            ))
        })?;

        let metadata = TableMetadata::read(&mut file)?;

        Ok(metadata
            .schema
            .columns
            .into_iter()
            .map(|col| (col.name, col.field_type))
            .collect())
    }

    pub fn lock_table(&self, table_specifier: TableSpecifier) -> CommandResult<()> {
        let hashset = self.in_write_tables.write();
        if let Ok(mut hashset) = hashset {
            hashset.insert(table_specifier);
            Ok(())
        } else {
            self.poison();
            Err(CommandError::Poisoned)
        }
    }

    pub fn unlock_table(&self, table_specifier: &TableSpecifier) -> CommandResult<()> {
        let hashset = self.in_write_tables.write();
        if let Ok(mut hashset) = hashset {
            hashset.remove(table_specifier);
            Ok(())
        } else {
            self.poison();
            Err(CommandError::Poisoned)
        }
    }
}

/// Denotes Allowed field types in table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Array,
}

impl FieldType {
    /// Converts `self` into `&str`.
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::String => "String",
            Self::Array => "Array",
        }
    }

    /// Converts `&str` into `FieldType`. if it's not know command type returns `None`.
    pub fn parse_field_type_from_str(field_type: &str) -> Option<Self> {
        match field_type {
            "String" => Some(Self::String),
            "Array" => Some(Self::Array),
            _ => None,
        }
    }

    pub const fn get_field_type_from_command(command: &Command) -> Self {
        match command {
            Command::String(_) => Self::String,
            Command::Array(_) => Self::Array,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let executor = Engine::new(std::path::PathBuf::from("test_db"));
        assert_eq!(
            executor
                .execute_command(Command::Array(vec![Command::String(String::from("ping"))]))
                .await
                .unwrap(),
            Command::String(String::from("PONG"))
        );
    }
}

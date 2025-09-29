use crate::commands;
use crate::protocol::{Command, CommandError, CommandResult};

use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::Arc;
use tokio_util::bytes::Bytes;

/// Main engine struct which executes received command.
#[derive(Debug)]
pub struct Engine {
    /// Path to the database location.
    db_dir: Arc<PathBuf>,
}

impl Clone for Engine {
    /// Creates new Engine struct by increasing the strong reference count of Arc.
    fn clone(&self) -> Self {
        Engine {
            db_dir: Arc::clone(&self.db_dir),
        }
    }
}

impl Engine {
    /// Returns reference for database directory.
    pub fn get_db_dir(&self) -> Arc<PathBuf> {
        Arc::clone(&self.db_dir)
    }

    /// Creates another engine by increasing the strong reference count of Arc
    pub fn new(db_dir: PathBuf) -> Self {
        Engine {
            db_dir: Arc::new(db_dir),
        }
    }

    /// Expects `Command::Array`.If command is not known returns error: `CommandError::InvalidCommandName`. Commands are case-insensitive.
    pub fn execute_command(&self, command: Command) -> CommandResult<Command> {
        let array = command
            .get_array()
            .ok_or_else(|| CommandError::ExecutionError("Expected array command format".into()))?;

        if array.is_empty() {
            return Err(CommandError::ExecutionError("Empty command array".into()));
        }

        match &array[0] {
            Command::String(data) => {
                let command_name = data.to_ascii_lowercase();

                if let Some(command) = commands::get_command(&command_name) {
                    command(&array[1..], self)
                } else {
                    Err(CommandError::InvalidCommandName(data.clone()))
                }
            }
            Command::Array(_) => Err(CommandError::ExecutionError(
                "Nested arrays are not supported (currently) in command structure".into(),
            )),
        }
    }

    /// Converts `bytes::Bytes` into `&str`. If it's not valid UTF-8 returns error: `CommandError::ExecutionError`.
    pub fn bytes_to_str(bytes: &Bytes) -> CommandResult<&str> {
        let bytes_str = match from_utf8(bytes) {
            Ok(name) => name,
            Err(error) => {
                return Err(CommandError::ExecutionError(
                    format!("Invalid character at {}", error.valid_up_to()).into(),
                ));
            }
        };

        Ok(bytes_str)
    }
}

/// Denotes Allowed field types in table.
pub enum FieldType {
    String,
}

impl FieldType {
    /// Converts `self` into `&str`.
    pub fn to_str(&self) -> &'static str {
        match self {
            FieldType::String => "String",
        }
    }
}

impl Engine {
    /// Converts `bytes::Bytes` into `FieldType`. if it's not know command type returns error `CommandError::ExecutionError`.
    pub fn parse_field_type(bytes: &Bytes) -> CommandResult<FieldType> {
        let command_str = Self::bytes_to_str(bytes)?;

        match command_str {
            "String" => Ok(FieldType::String),
            data => Err(CommandError::ExecutionError(
                format!("Unknown command type: {}", data).into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    #[test]
    fn test_ping() {
        let executor = Engine::new(std::path::PathBuf::from("test_db"));
        assert_eq!(
            executor
                .execute_command(Command::Array(vec![Command::String(Bytes::from("ping"))]))
                .unwrap(),
            Command::String(Bytes::from("PONG"))
        )
    }
}

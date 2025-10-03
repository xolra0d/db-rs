use serde::{Deserialize, Serialize};

/// Represents a parsed command in our custom protocol
#[derive(PartialEq, Clone, Debug, Eq, Serialize, Deserialize)]
pub enum Command {
    String(String),
    Array(Vec<Command>),
}

/// Errors that can occur during  command parsing or execution
#[derive(Debug, Serialize, Deserialize)]
pub enum CommandError {
    InvalidCommandName(String),
    ExecutionError(String),
    IOError(String),
    Poisoned,
}

impl From<std::io::Error> for CommandError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e.to_string())
    }
}

pub type CommandResult<T> = Result<T, CommandError>;

impl Command {
    pub fn get_array(self) -> Option<Vec<Self>> {
        match self {
            Self::Array(vec) => Some(vec),
            _ => None,
        }
    }
}

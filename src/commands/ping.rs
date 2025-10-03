use crate::commands::DatabaseCommand;
use crate::engine::Engine;
use crate::protocol::{Command, CommandResult};

/// Simple Ping command. always Returns "PONG" string.
pub struct PingCommand;

impl DatabaseCommand for PingCommand {
    fn name() -> &'static str {
        "ping"
    }

    fn execute(_args: &[Command], _engine: &Engine) -> CommandResult<Command> {
        Ok(Command::String(String::from("PONG")))
    }

    fn description() -> &'static str {
        "Return PONG response for connectivity testing"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_without_arguments() {
        let args = vec![];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = PingCommand::execute(&args, &engine).unwrap();

        if let Command::String(response) = result {
            assert_eq!(response, "PONG");
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_ping_with_arguments() {
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let args = vec![Command::String(String::from("ignored"))];
        let result = PingCommand::execute(&args, &engine).unwrap();

        if let Command::String(response) = result {
            assert_eq!(response, "PONG");
        } else {
            panic!("Expected string result");
        }
    }
}

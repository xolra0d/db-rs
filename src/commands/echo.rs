use crate::commands::DatabaseCommand;
use crate::engine::Engine;
use crate::protocol::{Command, CommandResult};

/// Echoes back all provided arguments.
pub struct EchoCommand;

impl DatabaseCommand for EchoCommand {
    fn name() -> &'static str {
        "echo"
    }

    fn execute(args: &[Command], _engine: &Engine) -> CommandResult<Command> {
        Ok(Command::Array(args.to_vec()))
    }

    fn description() -> &'static str {
        "Echoes back all provided arguments as an array"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    #[test]
    fn test_echo_single_argument() {
        let args = vec![Command::String(Bytes::from("hello"))];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = EchoCommand::execute(&args, &engine).unwrap();

        if let Command::Array(echoed_args) = result {
            assert_eq!(echoed_args, vec![Command::String(Bytes::from("hello"))]);
        } else {
            panic!("Unexpected result: {:?}", result);
        }
    }

    #[test]
    fn test_echo_multiple_arguments() {
        let args = vec![
            Command::String(Bytes::from("hello")),
            Command::String(Bytes::from("world")),
        ];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = EchoCommand::execute(&args, &engine).unwrap();

        if let Command::Array(echoed_args) = result {
            assert_eq!(
                echoed_args,
                vec![
                    Command::String(Bytes::from("hello")),
                    Command::String(Bytes::from("world"))
                ]
            );
        } else {
            panic!("Unexpected result: {:?}", result);
        }
    }

    #[test]
    fn test_echo_empty_arguments() {
        let args = vec![];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = EchoCommand::execute(&args, &engine).unwrap();

        if let Command::Array(echoed_args) = result {
            assert_eq!(echoed_args, vec![]);
        } else {
            panic!("Unexpected result: {:?}", result);
        }
    }
}

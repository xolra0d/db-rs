use crate::commands::{DatabaseCommand, get_all_command_names, get_command_description};
use crate::engine::Engine;
use crate::protocol::{Command, CommandResult};

/// Lists all available commands if no argument is provided. If there is an argument returns [`DatabaseCommand::description`].
pub struct HelpCommand;

impl DatabaseCommand for HelpCommand {
    fn name() -> &'static str {
        "help"
    }

    fn execute(args: &[Command], _engine: &Engine) -> CommandResult<Command> {
        let available_commands = get_all_command_names().join(", ");

        match args.len() {
            0 => Ok(Command::String(
                format!(
                    "Available commands: {}. Use 'help <command>' for more info.",
                    available_commands
                )
                .into(),
            )),
            1 => {
                if let Command::String(command_name_bytes) = &args[0] {
                    let command_name = command_name_bytes.to_ascii_lowercase();
                    let command_str = String::from_utf8_lossy(&command_name);

                    match get_command_description(&command_name) {
                        Some(description) => Ok(Command::String(description.into())),
                        None => Ok(Command::String(
                            format!(
                                "Unknown command '{}'. Available commands: {}",
                                command_str, available_commands
                            )
                            .into(),
                        )),
                    }
                } else {
                    Ok(Command::String(
                        "Help command expects a string argument.".into(),
                    ))
                }
            }
            _ => Ok(Command::String(
                "Help command takes at most one argument.".into(),
            )),
        }
    }

    fn description() -> &'static str {
        "Show available commands and their descriptions"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    #[test]
    fn test_help_no_arguments() {
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&[], &engine).unwrap();

        if let Command::String(help_text) = result {
            let help_str = String::from_utf8(help_text.to_vec()).unwrap();
            assert!(help_str.contains("Available commands"));
            assert!(help_str.contains("echo"));
            assert!(help_str.contains("ping"));
            assert!(help_str.contains("help"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_specific_command() {
        let args = vec![Command::String(Bytes::from("echo"))];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            let help_str = String::from_utf8(help_text.to_vec()).unwrap();
            assert!(help_str.contains("Echoes back all provided arguments as an array"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_unknown_command() {
        let args = vec![Command::String(Bytes::from("unknown"))];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            let help_str = String::from_utf8(help_text.to_vec()).unwrap();
            assert!(help_str.contains("Unknown command 'unknown'"));
            assert!(help_str.contains("Available commands"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_too_many_arguments() {
        let args = vec![
            Command::String(Bytes::from("echo")),
            Command::String(Bytes::from("extra")),
        ];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            let help_str = String::from_utf8(help_text.to_vec()).unwrap();
            assert!(help_str.contains("takes at most one argument"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_invalid_argument_type() {
        let args = vec![Command::Array(vec![])];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            let help_str = String::from_utf8(help_text.to_vec()).unwrap();
            assert!(help_str.contains("expects a string argument"));
        } else {
            panic!("Expected string result");
        }
    }
}

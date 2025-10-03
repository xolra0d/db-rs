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
            0 => Ok(Command::String(format!(
                "Available commands: {available_commands}. Use 'help <command>' for more info."
            ))),
            1 => {
                if let Command::String(command_name_bytes) = &args[0] {
                    let command_name = command_name_bytes.to_ascii_lowercase();

                    get_command_description(&command_name).map_or_else(|| Ok(Command::String(format!(
						"Unknown command '{command_name}'. Available commands: {available_commands}"))),
						|description| Ok(Command::String(description.into())))
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

    #[test]
    fn test_help_no_arguments() {
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&[], &engine).unwrap();

        if let Command::String(help_text) = result {
            assert!(help_text.contains("Available commands"));
            assert!(help_text.contains("echo"));
            assert!(help_text.contains("ping"));
            assert!(help_text.contains("help"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_specific_command() {
        let args = vec![Command::String(String::from("echo"))];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            assert_eq!(help_text, "Echoes back all provided arguments as an array");
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_unknown_command() {
        let args = vec![Command::String(String::from("unknown"))];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            assert!(help_text.contains("Unknown command 'unknown'"));
            assert!(help_text.contains("Available commands"));
            assert!(help_text.contains("echo"));
            assert!(help_text.contains("ping"));
            assert!(help_text.contains("help"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_help_too_many_arguments() {
        let args = vec![
            Command::String(String::from("echo")),
            Command::String(String::from("extra")),
        ];
        let db_dir = std::path::PathBuf::from("test_db");
        let engine = Engine::new(db_dir);
        let result = HelpCommand::execute(&args, &engine).unwrap();

        if let Command::String(help_text) = result {
            assert_eq!(help_text, "Help command takes at most one argument.");
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
            assert_eq!(help_text, "Help command expects a string argument.");
        } else {
            panic!("Expected string result");
        }
    }
}

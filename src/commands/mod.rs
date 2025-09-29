pub mod create;
pub mod echo;
pub mod help;
pub mod ping;

use crate::engine::Engine;
use crate::protocol::{Command, CommandResult};

pub trait DatabaseCommand {
    fn name() -> &'static str;

    fn execute(args: &[Command], engine: &Engine) -> CommandResult<Command>;

    fn description() -> &'static str;
}

type CommandHandler = fn(&[Command], &Engine) -> CommandResult<Command>;

/// If command is found (by name) returns a pointer to [`DatabaseCommand::execute`] of the command.
pub fn get_command(name: &[u8]) -> Option<CommandHandler> {
    match name {
        b"echo" => Some(echo::EchoCommand::execute),
        b"ping" => Some(ping::PingCommand::execute),
        b"help" => Some(help::HelpCommand::execute),
        b"create" => Some(create::CreateCommand::execute),
        _ => None,
    }
}

/// Returns command description by its name.
pub fn get_command_description(name: &[u8]) -> Option<&'static str> {
    match name {
        b"echo" => Some(echo::EchoCommand::description()),
        b"ping" => Some(ping::PingCommand::description()),
        b"help" => Some(help::HelpCommand::description()),
        b"create" => Some(create::CreateCommand::description()),
        _ => None,
    }
}

/// Lists all command names.
pub fn get_all_command_names() -> Vec<&'static str> {
    vec![
        echo::EchoCommand::name(),
        ping::PingCommand::name(),
        help::HelpCommand::name(),
        create::CreateCommand::name(),
    ]
}

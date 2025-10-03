pub mod create;
pub mod echo;
pub mod help;
pub mod insert;
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
pub fn get_command(name: &str) -> Option<CommandHandler> {
    match name {
        "echo" => Some(echo::EchoCommand::execute),
        "ping" => Some(ping::PingCommand::execute),
        "help" => Some(help::HelpCommand::execute),
        "create" => Some(create::CreateCommand::execute),
        "insert" => Some(insert::InsertCommand::execute),
        _ => None,
    }
}

/// Returns command description by its name.
pub fn get_command_description(name: &str) -> Option<&'static str> {
    match name {
        "echo" => Some(echo::EchoCommand::description()),
        "ping" => Some(ping::PingCommand::description()),
        "help" => Some(help::HelpCommand::description()),
        "create" => Some(create::CreateCommand::description()),
        "insert" => Some(insert::InsertCommand::description()),
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
        insert::InsertCommand::name(),
    ]
}

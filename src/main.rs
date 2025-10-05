mod commands;
mod config;
mod engine;
mod protocol;
mod storage;

use crate::config::{Config, write_error_and_exit};
use crate::engine::Engine;
use crate::protocol::protocol_parser::{Protocol, SendError};

use crate::protocol::CommandError;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = Config::build();
    let engine = Engine::new(config.get_db_dir().clone());

    let listener = TcpListener::bind(&config.get_socket_addr())
        .await
        .unwrap_or_else(|e| {
            write_error_and_exit(format!(
                "Failed to bind to {}: {}.",
                config.get_socket_addr(),
                e
            ))
        });

    info!("Database server listening on {}", config.get_socket_addr());
    info!("Database directory: {}", config.get_db_dir().display());
    info!("Log level: {:?}", config.get_log_level());

    // TODO: check that all commands complete before exiting, if they do not trigger other tables...

    loop {
        if engine.is_poisoned() {
            info!("Engine is poisoned, stopping server...");
            break;
        }

        match listener.accept().await {
            Ok((socket, addr)) => {
                if engine.is_poisoned() {
                    info!("Engine is poisoned, stopping server...");
                    drop(socket);
                    continue;
                }

                info!("New connection from {addr}");
                let engine_ = engine.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, engine_).await {
                        error!("Error handling connection from {addr}: {e}");
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {e}"),
        }
    }
}

/// Handles each connection by providing new Engine (look [`engine::Engine::clone`]).
async fn handle_connection(
    socket: TcpStream,
    engine: Engine,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut transport = Protocol.framed(socket);

    while let Some(command_result) = transport.next().await {
        if engine.is_poisoned() {
            info!("Engine is now poisoned, closing connection.");
            let _ = transport.send(Err(CommandError::Poisoned)).await;
            break;
        }

        match command_result {
            Err(error) => {
                warn!("Protocol error: {error:?}");
                let _ = transport.send(Err(error)).await;
            }
            Ok(command) => {
                let response = engine.execute_command(command).await;

                if let Err(CommandError::Poisoned) = &response {
                    info!("Engine is now poisoned, closing connection.");
                    let _ = transport.send(Err(CommandError::Poisoned)).await;
                    break;
                }

                if let Err(send_error) = transport.send(response).await {
                    let SendError::IOError(error) = send_error;
                    error!("Failed to send response: {error:?}");
                    break;
                }
            }
        }
    }

    info!("Connection closed.");
    Ok(())
}

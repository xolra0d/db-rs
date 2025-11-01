mod config;
mod engines;
mod error;
mod sql;
mod storage;
pub mod tcp_io_parser;

use futures::{SinkExt as _, StreamExt as _};
use log::{error, info};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::codec::Decoder as _;

use crate::config::CONFIG;
use crate::error::{Error, Result};
use crate::sql::CommandRunner;
use crate::tcp_io_parser::Parser;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(CONFIG.get_log_level())
        .init();

    let max_conn = Arc::new(Semaphore::new(CONFIG.get_max_connections()));

    let listener = TcpListener::bind(&CONFIG.get_tcp_socket_addr())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Failed to bind to {}: {}.",
                CONFIG.get_tcp_socket_addr(),
                error
            )
        });

    info!(
        "Database server listening on {}",
        CONFIG.get_tcp_socket_addr()
    );
    info!("Database directory: {}", CONFIG.get_db_dir().display());
    info!("Log level: {:?}", CONFIG.get_log_level());

    loop {
        let Ok(connection_permit) = Arc::clone(&max_conn).acquire_owned().await else {
            // semaphore is closed? currently unimplemented
            break;
        };
        match listener.accept().await {
            Ok((mut socket, addr)) => {
                tokio::spawn(async move {
                    if handle_connection(&mut socket).await.is_err() {
                        error!("Could not send to {addr}. Closing connection.");
                    }
                    drop(socket);
                    drop(connection_permit);
                });
            }
            Err(error) => error!("Failed to accept connection: {error}"),
        }
    }
}

async fn handle_connection(socket: &mut TcpStream) -> Result<()> {
    let mut transport = Parser.framed(socket);

    while let Some(sql_command) = transport.next().await {
        let Ok(value) = sql_command else {
            let error = sql_command.unwrap_err();
            if let Err(send_error) = transport.send(Err(error)).await {
                error!("Failed to send response: {send_error}");
                return Err(Error::SendResponse);
            }
            continue;
        };

        if value == "exit" {
            break;
        }

        let output = CommandRunner::execute_command(&value);
        if let Err(send_error) = transport.send(output).await {
            error!("Failed to send response: {send_error}");
            return Err(Error::SendResponse);
        }
    }
    info!("Connection closed.");
    Ok(())
}

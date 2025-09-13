mod config;
mod protocol;

use config::Config;
use protocol::Protocol;

use futures::StreamExt;
use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

#[tokio::main]
async fn main() {
    let cfg = Config::build();

    let listener = TcpListener::bind(&cfg.addr).await.unwrap_or_else(|e| {
        error!("bind error: {}", e);
        std::process::exit(1);
    });

    info!("Successfully bind to {}", cfg.addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("accepted connection from {:?}", addr);
                process(socket).await;
            }
            Err(e) => error!("accept error: {}", e),
        }
    }
}

async fn process(socket: TcpStream) {
    tokio::spawn(async move {
        let mut transport = Protocol.framed(socket);
        while let Some(value) = transport.next().await {
            info!("GOT VALUE: {:?}", &value);
        }
        info!("exiting connection");
    });
}

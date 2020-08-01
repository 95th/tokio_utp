#[macro_use]
extern crate unwrap;

#[macro_use]
extern crate log;

use futures::StreamExt;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_utp::*;

#[tokio::main]
async fn main() {
    env_logger::init();

    // Start a simple echo server

    let addr: SocketAddr = unwrap!("127.0.0.1:4561".parse());

    let (_, listener) = unwrap!(UtpSocket::bind(&addr));
    debug!("Listener started");

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        tokio::spawn(async move {
            if let Err(e) = handle(stream).await {
                warn!("{}", e);
            }
        });
    }
}

async fn handle(stream: io::Result<UtpStream>) -> io::Result<()> {
    let mut stream = stream?;
    let mut buf = vec![];

    debug!("Read data from client");
    stream.read_to_end(&mut buf).await?;

    debug!("Write data to client");
    stream.write_all(&buf).await?;

    Ok(())
}

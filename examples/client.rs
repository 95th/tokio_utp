#[macro_use]
extern crate unwrap;

#[macro_use]
extern crate log;

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_utp::*;

#[tokio::main]
async fn main() {
    unwrap!(env_logger::init());

    let local_addr: SocketAddr = unwrap!("127.0.0.1:0".parse());
    let remote_addr: SocketAddr = unwrap!("127.0.0.1:4561".parse());

    let (socket, _) = unwrap!(UtpSocket::bind(&local_addr));

    debug!("Connecting to the server");
    let mut stream = unwrap!(socket.connect(&remote_addr).await);

    debug!("Connected. send it some data");
    unwrap!(stream.write_all(b"hello world").await);

    debug!("shutdown our the write side of the connection");
    unwrap!(stream.shutdown().await);

    let mut data = vec![];
    // read the stream to completion.
    unwrap!(stream.read_to_end(&mut data).await);

    debug!("received {:?} from server", String::from_utf8(data));
}

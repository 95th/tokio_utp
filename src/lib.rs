#[cfg(test)]
macro_rules! addr {
    ($a:expr) => {
        $a.parse::<::std::net::SocketAddr>().unwrap()
    };
}

#[macro_use]
extern crate log;
#[macro_use]
extern crate unwrap;

#[cfg(test)]
#[macro_use]
extern crate hamcrest;

mod delays;
mod in_queue;
mod out_queue;
mod packet;
mod socket;
mod split;
mod util;

#[cfg(test)]
extern crate env_logger;
// #[cfg(test)]
// #[cfg(target_os = "linux")]
// #[cfg(feature = "netsim")]
// extern crate netsim;

#[cfg(test)]
mod test;

pub use socket::{
    Incoming, RawChannel, RawReceiver, UtpListener, UtpSocket, UtpSocketFinalize, UtpStream,
    UtpStreamConnect,
};

// max window size
const MAX_WINDOW_SIZE: usize = 64 * 1_024;
const MAX_DELTA_SEQ: usize = 32;
const TIMESTAMP_MASK: u32 = 0xFFFF_FFFF;

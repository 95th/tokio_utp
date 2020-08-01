use crate::{util, UtpSocket};
use futures::StreamExt;
use rand::Rng;
use std::{fs::File, future::Future, io::Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

trait FutureExt: Sized + Future + 'static {
    fn sendless_boxed(self) -> Box<dyn Future<Output = Self::Output> + 'static> {
        Box::new(self)
    }
}

impl<T: Future + 'static> FutureExt for T {}

#[tokio::test]
async fn send_lots_of_data_one_way() {
    const NUM_TESTS: usize = 100;

    env_logger::init();

    send_data_one_way(0).await;
    send_data_one_way(1).await;
    for i in 0..NUM_TESTS {
        info!("Test {} of {}", i + 1, NUM_TESTS);
        let num_bytes =
            util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
        send_data_one_way(num_bytes).await;
    }
}

#[tokio::test]
async fn send_lots_of_data_round_trip() {
    const NUM_TESTS: usize = 100;

    env_logger::init();
    util::reset_rand();

    send_data_round_trip(0).await;
    send_data_round_trip(1).await;
    for i in 0..NUM_TESTS {
        info!("Test {} of {}", i + 1, NUM_TESTS);
        let num_bytes =
            util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
        send_data_round_trip(num_bytes).await;
    }
}

async fn send_data_round_trip(num_bytes: usize) {
    println!("== sending {} bytes", num_bytes);

    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");

    let (socket_a, _) = unwrap!(UtpSocket::bind(&addr));
    let (_, listener_b) = unwrap!(UtpSocket::bind(&addr));

    let addr = &unwrap!(listener_b.local_addr());

    let task0 = async move {
        let mut stream_a = unwrap!(socket_a.connect(addr).await);
        let (read_half_a, write_half_a) = &mut stream_a.split();
        let task0 = async move {
            unwrap!(write_half_a.write_all(&random_send).await);
            trace!("finished writing from (a)");
            unwrap!(write_half_a.shutdown().await);
            random_send
        };
        let task1 = async move {
            let mut random_recv = Vec::new();
            unwrap!(read_half_a.read_to_end(&mut random_recv).await);
            trace!("finished reading at (a)");
            random_recv
        };
        futures::join!(task0, task1)
    };
    let task1 = async move {
        let mut incoming = listener_b.incoming();
        if let Some(stream_b) = incoming.next().await {
            let mut stream_b = unwrap!(stream_b);
            let (read_half_b, write_half_b) = &mut stream_b.split();
            unwrap!(tokio::io::copy(read_half_b, write_half_b).await);
            trace!("finished copying at (b)");
            unwrap!(write_half_b.shutdown().await);
        }
    };

    let ((random_send, random_recv), _) = futures::join!(task0, task1);
    if random_send != random_recv {
        let mut send = unwrap!(File::create("round-trip-failed-send.dat"));
        let mut recv = unwrap!(File::create("round-trip-failed-recv.dat"));
        unwrap!(send.write_all(&random_send));
        unwrap!(recv.write_all(&random_recv));
        panic!(
            "Data corrupted during round-trip! Sent/received data saved to
                           round-trip-failed-send.dat and round-trip-failed-recv.dat"
        );
    }
}

async fn send_data_one_way(num_bytes: usize) {
    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");

    let (socket_a, _) = unwrap!(UtpSocket::bind(&addr,));
    let (_, listener_b) = unwrap!(UtpSocket::bind(&addr,));

    let addr = &unwrap!(listener_b.local_addr());
    let task0 = async move {
        let mut stream_a = unwrap!(socket_a.connect(addr).await);
        unwrap!(stream_a.write_all(&random_send).await);
        unwrap!(stream_a.shutdown().await);
        random_send
    };
    let task1 = async move {
        let mut incoming = listener_b.incoming();
        let stream_b = unwrap!(incoming.next().await);
        let mut stream_b = unwrap!(stream_b);
        let mut random_recv = Vec::new();
        unwrap!(stream_b.read_to_end(&mut random_recv).await);
        random_recv
    };

    let (random_send, random_recv) = futures::join!(task0, task1);
    assert_eq!(random_send, random_recv);
    if random_send != random_recv {
        let mut send = unwrap!(File::create("one-way-failed-send.dat"));
        let mut recv = unwrap!(File::create("one-way-failed-recv.dat"));
        unwrap!(send.write_all(&random_send));
        unwrap!(recv.write_all(&random_recv));
        panic!(
            "Data corrupted during one way transfer! Sent/received data saved to
                           one-way-failed-send.dat and one-way-failed-recv.dat"
        );
    }
}

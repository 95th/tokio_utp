use crate::{
    delays::Delays,
    in_queue::InQueue,
    out_queue::OutQueue,
    packet::{self, Packet},
    split::{ReadHalf, WriteHalf},
    util, TIMESTAMP_MASK,
};

use arraydeque::ArrayDeque;
use bytes::{BufMut, BytesMut};
use futures::{channel::oneshot, future::FutureExt, ready, stream::Stream};
use slab::Slab;
use std::{
    cmp,
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    io::{Error, ErrorKind, Result},
    mem,
    mem::MaybeUninit,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UdpSocket,
    time::{delay_until, Delay},
};

#[cfg(unix)]
use nix::sys::socket::{getpeername, SockAddr};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// A uTP socket. Can be used to make outgoing connections.
pub struct UtpSocket {
    // Shared state
    inner: InnerCell,
    req_finalize: oneshot::Sender<oneshot::Sender<()>>,
}

impl fmt::Debug for UtpSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "UtpSocket")
    }
}

/// A uTP stream.
pub struct UtpStream {
    // Shared state
    inner: InnerCell,

    // Connection identifier
    token: usize,
}

impl fmt::Debug for UtpStream {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("UtpStream")
            .field("peer_addr", &self.peer_addr())
            .finish()
    }
}

/// Listens for incoming uTP connections.
pub struct UtpListener {
    // Shared state
    inner: InnerCell,
}

impl fmt::Debug for UtpListener {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "UtpListener")
    }
}

// Shared between the UtpSocket and each UtpStream
struct Inner {
    // State that needs to be passed to `Connection`. This is broken out to make
    // the borrow checker happy.
    shared: Shared,

    // Connection specific state
    connections: Slab<Connection>,

    // Lookup a connection by key
    connection_lookup: HashMap<Key, usize>,

    // Buffer used for in-bound data
    in_buf: BytesMut,

    accept_buf: VecDeque<UtpStream>,

    listener: Option<Waker>,

    listener_open: bool,

    // Reset packets to be sent. These packets are not associated with any connection. They
    // don't need to be acknowledged either.
    reset_packets: ArrayDeque<[(Packet, SocketAddr); MAX_CONNECTIONS_PER_SOCKET]>,
}

type InnerCell = Arc<RwLock<Inner>>;

unsafe impl Send for Inner {}

struct Shared {
    // The UDP socket backing everything!
    socket: UdpSocket,

    // The task waker
    waker: Option<Waker>,
}

// Owned by UtpSocket
#[derive(Debug)]
struct Connection {
    // Current socket state
    state: State,

    // A combination of the send ID and the socket address
    key: Key,

    // True when the `UtpStream` handle has been dropped
    released: bool,

    // False when `shutdown_write` has been called.
    write_open: bool,
    // Set to Fin packet sequence number when we've sent FIN, don't send more Fin packets.
    fin_sent: Option<u16>,
    // Stores sequence number of Fin packet that we received from remote peer.
    fin_received: Option<u16>,
    // True when we receive ack for Fin that we sent.
    fin_sent_acked: bool,

    // Used to signal readiness on the `UtpStream`
    waker: Option<Waker>,

    // Queue of outbound packets. Packets will stay in the queue until the peer
    // has acked them.
    out_queue: OutQueue,

    // Queue of inbound packets. The queue orders packets according to their
    // sequence number.
    in_queue: InQueue,

    // Activity deadline
    deadline: Option<Instant>,
    // Activitity disconnect deadline
    last_recv_time: Instant,
    disconnect_timeout_secs: u32,

    // Tracks delays for the congestion control algorithm
    our_delays: Delays,

    their_delays: Delays,

    last_maxed_out_window: Instant,
    // This is actually average of delay differences. It's used to recalculate `clock_drift`.
    // It's an average deviation from `average_delay_base`.
    average_delay: i32,
    current_delay_sum: i64,
    current_delay_samples: i64,
    // This is a reference value from which delay difference is measured.
    average_delay_base: u32,
    average_sample_time: Instant,
    clock_drift: i32,
    slow_start: bool,
    // Artifical packet loss rate (for testing)
    // Probability of dropping is loss_rate / u32::MAX
    //#[cfg(test)]
    //loss_rate: u32,
    closed_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Key {
    receive_id: u16,
    addr: SocketAddr,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum State {
    // Establishing a new connection, waiting for the peer to respond with a
    // STATE.
    SynSent,
    // Received Syn, the state packet is sent immediately, but the connection is
    // not transitioned to `Connected` until it has been accepted.
    SynRecv,
    // Fully connected state
    Connected,
    // A SYN has been sent and we are currently waiting for an ACK before
    // closing the connection.
    FinSent,
    // The connection has been reset by the remote.
    Reset,
    // Connection has been closed and waiting to be removed.
    Closed,
}

const MIN_BUFFER_SIZE: usize = 4 * 1_024;
const DEFAULT_IN_BUFFER_SIZE: usize = 64 * 1024;
const MAX_CONNECTIONS_PER_SOCKET: usize = 2 * 1024;
const DEFAULT_TIMEOUT_MS: u64 = 1_000;
const TARGET_DELAY: u32 = 100_000; // 100ms in micros
const DEFAULT_DISCONNECT_TIMEOUT_SECS: u32 = 60;

const SLOW_START_THRESHOLD: usize = DEFAULT_IN_BUFFER_SIZE;
const MAX_CWND_INCREASE_BYTES_PER_RTT: usize = 3000;
const MIN_WINDOW_SIZE: usize = 10;
const MAX_DATA_SIZE: usize = 1_400 - 20;

impl UtpSocket {
    /// Bind a new `UtpSocket` to the given socket address
    pub async fn bind(addr: &SocketAddr) -> Result<(UtpSocket, UtpListener)> {
        let socket = UdpSocket::bind(addr).await?;
        UtpSocket::from_socket(socket)
    }

    /// Gets the local address that the socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        unwrap!(self.inner.read()).shared.socket.local_addr()
    }

    /// Create a new `Utpsocket` backed by the provided `UdpSocket`.
    pub fn from_socket(socket: UdpSocket) -> Result<(UtpSocket, UtpListener)> {
        let (finalize_tx, finalize_rx) = oneshot::channel();

        let inner = Inner::new_shared(socket);
        let listener = UtpListener {
            inner: inner.clone(),
        };

        let socket = UtpSocket {
            inner: inner.clone(),
            req_finalize: finalize_tx,
        };

        let next_tick = Instant::now() + Duration::from_millis(500);
        let timeout = delay_until(next_tick.into());
        let refresher = SocketRefresher {
            inner,
            next_tick,
            timeout,
            req_finalize: finalize_rx,
        };

        tokio::spawn(refresher);

        Ok((socket, listener))
    }

    /// Connect a new `UtpSocket` to the given remote socket address
    pub fn connect(&self, addr: &SocketAddr) -> UtpStreamConnect {
        let mut inner = unwrap!(self.inner.write());
        let state = match inner.connect(addr, &self.inner) {
            Ok(stream) => UtpStreamConnectState::Waiting(stream),
            Err(e) => UtpStreamConnectState::Err(e),
        };
        UtpStreamConnect { state }
    }

    /// Consume the socket and the convert it to a future which resolves once all connections have
    /// been closed gracefully.
    pub fn finalize(self) -> UtpSocketFinalize {
        let (respond_tx, respond_rx) = oneshot::channel();
        unwrap!(self.req_finalize.send(respond_tx));
        UtpSocketFinalize {
            resp_finalize: respond_rx,
        }
    }
}

impl UtpListener {
    /// Get the local address that the listener is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        unwrap!(self.inner.read()).shared.socket.local_addr()
    }

    /// Receive a new inbound connection.
    ///
    /// This function will also advance the state of all associated connections.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<Result<UtpStream>> {
        let mut inner = unwrap!(self.inner.write());
        inner.poll_accept(cx)
    }

    /// Convert the `UtpListener` to a stream of incoming connections.
    pub fn incoming(self) -> Incoming {
        Incoming { listener: self }
    }
}

impl Drop for UtpListener {
    fn drop(&mut self) {
        let mut inner = unwrap!(self.inner.write());
        inner.listener_open = false;

        // Empty the connection queue
        let mut streams = Vec::new();
        let fut = futures::future::poll_fn(|cx| {
            while let Poll::Ready(Ok(stream)) = inner.poll_accept(cx) {
                streams.push(stream);
            }
            Poll::Ready(())
        });
        futures::executor::block_on(fut);

        // Must release the lock before dropping the streams
        drop(inner);
        drop(streams);
    }
}

#[cfg(test)]
impl UtpListener {
    pub fn is_readable(&self) -> bool {
        let inner = unwrap!(self.inner.read());
        inner.listener.is_none()
    }
}

impl UtpStream {
    /// Get the address of the remote peer.
    pub fn peer_addr(&self) -> SocketAddr {
        let inner = unwrap!(self.inner.read());
        let connection = &inner.connections[self.token];
        connection.key.addr
    }

    /// Get the local address that the stream is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        unwrap!(self.inner.read()).shared.socket.local_addr()
    }

    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        crate::split::split(self)
    }

    /// The same as `Read::read` except it does not require a mutable reference to the stream.
    pub(crate) fn poll_read_immutable(
        &self,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut inner = unwrap!(self.inner.write());
        let conn = &mut inner.connections[self.token];

        let ret = ready!(conn.in_queue.poll_read(cx, dst));
        conn.update_local_window();
        ret.into()
    }

    /// The same as `Write::write` except it does not require a mutable reference to the stream.
    pub(crate) fn poll_write_immutable(
        &self,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<Result<usize>> {
        let mut inner = unwrap!(self.inner.write());
        inner.poll_write(cx, self.token, src)
    }

    /// Shutdown the write-side of the uTP connection. The stream can still be used to read data
    /// received from the peer but can no longer be used to send data. Will cause the peer to
    /// receive and EOF.
    pub(crate) fn poll_shutdown_write(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut inner = unwrap!(self.inner.write());
        inner.poll_shutdown_write(cx, self.token)
    }

    /// Flush all outgoing data on the socket. Returns `Poll::Pending` if there remains data that
    /// could not be immediately written.
    pub(crate) fn poll_flush_immutable(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut inner = unwrap!(self.inner.write());
        inner.poll_flush(cx, self.token)
    }

    /// Sets how long we must lose contact with the remote peer for before we consider the
    /// connection to have died. Defaults to 1 minute.
    pub fn set_disconnect_timeout(&self, duration: Duration) {
        let mut inner = unwrap!(self.inner.write());
        let connection = &mut inner.connections[self.token];
        connection.disconnect_timeout_secs =
            cmp::min(u64::from(u32::MAX), duration.as_secs()) as u32;
    }

    /// Returns a future that waits until connection is gracefully shutdown: both peers have sent
    /// `Fin` packets and received corresponding acknowledgements.
    pub fn finalize(self) -> UtpStreamFinalize {
        let (signal_tx, signal_rx) = oneshot::channel();
        {
            let mut inner = unwrap!(self.inner.write());
            let conn = &mut inner.connections[self.token];
            conn.set_closed_tx(signal_tx);
        }

        UtpStreamFinalize {
            conn_closed: signal_rx,
            _stream: self,
        }
    }
}

#[cfg(test)]
impl UtpStream {
    // pub fn is_readable(&self) -> bool {
    //     let inner = unwrap!(self.inner.read());
    //     let connection = &inner.connections[self.token];
    //     connection.set_readiness.readiness().is_readable()
    // }

    // pub fn is_writable(&self) -> bool {
    //     let inner = unwrap!(self.inner.read());
    //     let connection = &inner.connections[self.token];
    //     connection.set_readiness.readiness().is_writable()
    // }

    /*
    pub fn set_loss_rate(&self, rate: f32) {
        let mut inner = unwrap!(self.inner.write());
        let connection = &mut inner.connections[self.token];
        connection.loss_rate = (rate as f64 * ::std::u32::MAX as f64) as u32;
    }
    */
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        let mut inner = unwrap!(self.inner.write());
        inner.close(self.token);
    }
}

/// A future that resolves once the uTP connection is gracefully closed. Created via
/// `UtpStream::finalize`.
pub struct UtpStreamFinalize {
    /// Waits for signal, ignores the value.
    conn_closed: oneshot::Receiver<()>,
    /// Hold stream so it wouldn't be dropped.
    _stream: UtpStream,
}

impl Future for UtpStreamFinalize {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // it can't fail cause `UtpStreamFinalize` holds `UtpStream` which makes sure `Inner`
        // and `Connection` instances are not dropped and `Connection` holds channel transmitter.
        let result = ready!(self.conn_closed.poll_unpin(cx));
        unwrap!(result);
        Poll::Ready(())
    }
}

impl Inner {
    fn new(socket: UdpSocket) -> Inner {
        Inner {
            shared: Shared::new(socket),
            connections: Slab::new(),
            connection_lookup: HashMap::new(),
            in_buf: BytesMut::with_capacity(DEFAULT_IN_BUFFER_SIZE),
            accept_buf: VecDeque::new(),
            listener: None,
            listener_open: true,
            reset_packets: ArrayDeque::new(),
        }
    }

    fn new_shared(socket: UdpSocket) -> InnerCell {
        Arc::new(RwLock::new(Inner::new(socket)))
    }

    fn poll_accept(&mut self, cx: &mut Context<'_>) -> Poll<Result<UtpStream>> {
        match self.accept_buf.pop_front() {
            Some(socket) => {
                let conn = &mut self.connections[socket.token];

                if conn.state == State::SynRecv {
                    conn.state = State::Connected;
                } else if conn.state.is_closed() {
                    // Connection is being closed, but there may be data in the
                    // buffer...
                } else {
                    unreachable!();
                }

                conn.update_readiness();
                Poll::Ready(Ok(socket))
            }
            None => {
                self.listener = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        token: usize,
        src: &[u8],
    ) -> Poll<Result<usize>> {
        let conn = &mut self.connections[token];

        if conn.state.is_closed() || !conn.write_open {
            return Err(ErrorKind::BrokenPipe.into()).into();
        }

        match conn.out_queue.poll_write(src) {
            Poll::Ready(n) => {
                ready!(conn.poll_flush(cx, &mut self.shared))?;
                conn.update_readiness();
                Ok(n).into()
            }
            Poll::Pending => {
                conn.last_maxed_out_window = Instant::now();
                conn.update_readiness();
                Poll::Pending
            }
        }
    }

    /// Connect a new `UtpSocket` to the given remote socket address
    fn connect(&mut self, addr: &SocketAddr, inner: &InnerCell) -> Result<UtpStream> {
        if self.connections.len() >= MAX_CONNECTIONS_PER_SOCKET {
            debug_assert!(self.connections.len() <= MAX_CONNECTIONS_PER_SOCKET);
            return Err(Error::new(ErrorKind::Other, "socket has max connections"));
        }

        // The peer establishing the connection picks the identifiers uses for
        // the stream.
        let (receive_id, mut send_id) = util::generate_sequential_identifiers();

        let mut key = Key {
            receive_id,
            addr: *addr,
        };

        // Because the IDs are randomly generated, there could already be an
        // existing connection with the key, so sequentially scan until we hit a
        // free slot.
        while self.connection_lookup.contains_key(&key) {
            key.receive_id += 1;
            send_id += 1;
        }

        let connection = Connection::new_outgoing(key.clone(), send_id);
        let token = self.connections.insert(connection);

        // Track the connection in the lookup
        self.connection_lookup.insert(key, token);

        Ok(UtpStream {
            inner: inner.clone(),
            token,
        })
    }

    fn poll_shutdown_write(&mut self, cx: &mut Context<'_>, token: usize) -> Poll<Result<()>> {
        let conn = &mut self.connections[token];
        conn.write_open = false;
        if conn.schedule_fin() {
            ready!(conn.poll_flush(cx, &mut self.shared))?;
        }
        Ok(()).into()
    }

    fn close(&mut self, token: usize) {
        let finalized = {
            let conn = &mut self.connections[token];
            conn.released = true;
            if !conn.state.is_closed() {
                let _ = conn.schedule_fin();
                conn.state = State::FinSent;
                //let _ = conn.flush(&mut self.shared);
            }
            conn.is_finalized()
        };

        if finalized {
            self.remove_connection(token);
        }
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>, inner: &InnerCell) -> Poll<Result<bool>> {
        // trace!("ready; ready={:?}", ready);

        // Update readiness
        self.shared.wake();

        loop {
            // Try to receive a packet
            let (bytes, addr) = match self.poll_recv_from(cx) {
                Poll::Ready(Ok(v)) => v,
                Poll::Ready(Err(ref e)) if e.kind() == ErrorKind::ConnectionReset => {
                    continue;
                }
                Poll::Ready(Err(e)) => {
                    trace!("recv_from; error={:?}", e);
                    return Err(e).into();
                }
                Poll::Pending => {
                    trace!("ready -> pending");
                    break;
                }
            };

            //trace!("recv_from; addr={:?}; packet={:?}", addr, b);

            match self.process(cx, bytes, addr, inner) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Err(e).into(),
                Poll::Pending => {
                    panic!("NOPE");
                }
            }
        }

        let _still_writable = ready!(self.poll_flush_all(cx))?;
        let _ = self.flush_reset_packets(cx)?;
        Ok(self.connection_lookup.is_empty()).into()
    }

    fn poll_refresh(&mut self, cx: &mut Context<'_>, inner: &InnerCell) -> Poll<Result<bool>> {
        self.poll_ready(cx, inner)
    }

    fn tick(&mut self, cx: &mut Context<'_>) -> Result<()> {
        trace!("Socket::tick");
        let mut reset_packets = Vec::new();

        for &idx in self.connection_lookup.values() {
            if let Poll::Ready(Some((conn_id, peer_addr))) =
                self.connections[idx].tick(cx, &mut self.shared)?
            {
                // partial borrowing is currently not possible in Rust so we can't call
                // self.schedule_reset() in here, hence collect packets :/
                reset_packets.push((conn_id, peer_addr));
            }
        }

        for (conn_id, peer_addr) in reset_packets {
            self.schedule_reset(conn_id, peer_addr);
        }

        Ok(())
    }

    fn process(
        &mut self,
        cx: &mut Context<'_>,
        bytes: BytesMut,
        addr: SocketAddr,
        inner: &InnerCell,
    ) -> Poll<Result<()>> {
        let packet = match Packet::parse(bytes) {
            Ok(packet) => packet,
            Err(_) => return Ok(()).into(),
        };
        trace!("recv_from; addr={:?}; packet={:?}", addr, packet);
        // Process the packet
        match packet.ty() {
            packet::Type::Syn => {
                // SYN packets are special
                self.process_syn(cx, &packet, addr, inner)
            }
            _ => {
                // All other packets are associated with a connection, and as
                // such they should be sequenced.
                let key = Key::new(packet.connection_id(), addr);

                match self.connection_lookup.get(&key) {
                    Some(&token) => {
                        let finalized = {
                            let conn = &mut self.connections[token];
                            ready!(conn.process(cx, packet, &mut self.shared))?
                        };

                        if finalized {
                            self.remove_connection(token);
                        }

                        Ok(()).into()
                    }
                    None => self.process_unknown(packet, addr).into(),
                }
            }
        }
    }

    /// Handle packets with unknown ID.
    fn process_unknown(&mut self, packet: Packet, addr: SocketAddr) -> Result<()> {
        trace!("no connection associated with ID; treating as raw data");

        if packet.ty() != packet::Type::Reset {
            self.schedule_reset(packet.connection_id(), addr);
        }

        Ok(())
    }

    fn process_syn(
        &mut self,
        cx: &mut Context<'_>,
        packet: &Packet,
        addr: SocketAddr,
        inner: &InnerCell,
    ) -> Poll<Result<()>> {
        let send_id = packet.connection_id();
        let receive_id = send_id + 1;
        let key = Key { receive_id, addr };

        if let Some(&token) = self.connection_lookup.get(&key) {
            trace!(
                "connection(id={}) already established, ignoring Syn packet",
                send_id
            );
            // Ack packet anyway
            let conn = &mut self.connections[token];
            conn.out_queue.maybe_resend_ack_for(&packet);
            ready!(conn.poll_flush(cx, &mut self.shared))?;
            return Ok(()).into();
        }

        if !self.listener_open || self.connections.len() >= MAX_CONNECTIONS_PER_SOCKET {
            debug_assert!(self.connections.len() <= MAX_CONNECTIONS_PER_SOCKET);
            self.schedule_reset(packet.connection_id(), addr);
            return Ok(()).into();
        }

        let mut connection = Connection::new_incoming(key.clone(), send_id, packet.seq_nr());
        // This will handle the state packet being sent
        ready!(connection.poll_flush(cx, &mut self.shared))?;

        let token = self.connections.insert(connection);
        self.connection_lookup.insert(key, token);

        // Store the connection in the accept buffer
        self.accept_buf.push_back(UtpStream {
            inner: inner.clone(),
            token,
        });

        // Notify the listener
        if let Some(waker) = self.listener.take() {
            waker.wake();
        }

        Ok(()).into()
    }

    fn poll_recv_from(&mut self, cx: &mut Context<'_>) -> Poll<Result<(BytesMut, SocketAddr)>> {
        // Ensure the buffer has at least 4kb of available space.
        self.in_buf.reserve(MIN_BUFFER_SIZE);

        // Read in the bytes
        let addr = unsafe {
            let buf = self.in_buf.bytes_mut();
            let buf = std::mem::transmute(buf);
            let (n, addr) = ready!(self.shared.poll_recv_from(cx, buf))?;
            self.in_buf.advance_mut(n);
            addr
        };

        let bytes = self.in_buf.split();
        Ok((bytes, addr)).into()
    }

    fn poll_flush_all(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.connection_lookup.is_empty() {
            return Ok(()).into();
        }

        // Iterate in semi-random order so that bandwidth is divided fairly between connections.
        let skip_point = util::rand::<usize>() % self.connection_lookup.len();
        let tokens = self
            .connection_lookup
            .values()
            .skip(skip_point)
            .chain(self.connection_lookup.values().take(skip_point));
        for &token in tokens {
            let conn = &mut self.connections[token];
            ready!(conn.poll_flush(cx, &mut self.shared))?;
        }
        Ok(()).into()
    }

    /// Enqueues Reset packet with given information.
    fn schedule_reset(&mut self, conn_id: u16, dest_addr: SocketAddr) {
        // Send the RESET packet, ignoring errors...
        let mut p = Packet::reset();
        p.set_connection_id(conn_id);
        let _ = self.reset_packets.push_back((p, dest_addr));
    }

    /// Attempts to send enqueued Reset packets.
    fn flush_reset_packets(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while let Some((packet, dest_addr)) = self.reset_packets.pop_front() {
            match self.shared.poll_send_to(cx, packet.as_slice(), &dest_addr) {
                Poll::Ready(Ok(n)) => {
                    // should never fail!
                    assert_eq!(n, packet.as_slice().len());
                }
                Poll::Ready(Err(e)) => return Err(e).into(),
                Poll::Pending => {
                    self.shared.register(cx);
                    let _ = self.reset_packets.push_back((packet, dest_addr));
                    return Poll::Pending;
                }
            }
        }
        Ok(()).into()
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>, token: usize) -> Poll<Result<()>> {
        let connection = &mut self.connections[token];
        connection.poll_flush(cx, &mut self.shared)
    }

    fn remove_connection(&mut self, token: usize) {
        let connection = self.connections.remove(token);
        self.connection_lookup.remove(&connection.key);
        trace!(
            "removing connection state; token={:?}, addr={:?}; id={:?}",
            token,
            connection.key.addr,
            connection.key.receive_id
        );
    }
}

impl Shared {
    fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            waker: None,
        }
    }

    fn is_ready(&self) -> bool {
        self.waker.is_none()
    }

    fn register(&mut self, cx: &mut Context<'_>) {
        self.waker = Some(cx.waker().clone());
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: &SocketAddr,
    ) -> Poll<Result<usize>> {
        if self.connected_peer_addr().is_none() {
            self.socket.poll_send_to(cx, buf, target)
        } else {
            self.socket.poll_send(cx, buf)
        }
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<(usize, SocketAddr)>> {
        if let Some(peer_addr) = self.connected_peer_addr() {
            self.socket
                .poll_recv(cx, buf)
                .map_ok(|bytes_received| (bytes_received, peer_addr))
        } else {
            self.socket.poll_recv_from(cx, buf)
        }
    }

    /// If UDP socket was connected, return peer address.
    /// On Windows always returns `None`, cause Windows allows to use `send_to/recv_from` on
    /// connected UDP sockets.
    fn connected_peer_addr(&self) -> Option<SocketAddr> {
        #[cfg(unix)]
        {
            // NOTE, `peer_addr()` might be implemented for `UdpSocket` in the future.
            // then getpeername() could be removed
            let res = getpeername(self.socket.as_raw_fd());
            match res {
                Ok(addr) => {
                    if let SockAddr::Inet(addr) = addr {
                        Some(addr.to_std())
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
}

impl Connection {
    fn new(
        state: State,
        key: Key,
        out_queue: OutQueue,
        in_queue: InQueue,
        deadline_after: Option<u64>,
    ) -> Self {
        let now = Instant::now();
        let deadline = deadline_after.map(|millis| now + Duration::from_millis(millis));
        Self {
            state,
            key,
            out_queue,
            in_queue,
            our_delays: Delays::new(),
            their_delays: Delays::new(),
            released: false,
            write_open: true,
            fin_sent: None,
            fin_sent_acked: false,
            waker: None,
            fin_received: None,
            deadline,
            last_recv_time: now,
            disconnect_timeout_secs: DEFAULT_DISCONNECT_TIMEOUT_SECS,
            last_maxed_out_window: now,
            average_delay: 0,
            current_delay_sum: 0,
            current_delay_samples: 0,
            average_delay_base: 0,
            average_sample_time: now,
            clock_drift: 0,
            slow_start: true,
            //#[cfg(test)]
            //loss_rate: 0,
            closed_tx: None,
        }
    }

    /// Constructs new connection that we initiated with `UtpSocket::connect()`.
    fn new_outgoing(key: Key, send_id: u16) -> Self {
        // SYN packet has seq_nr of 1
        let mut out_queue = OutQueue::new(send_id, 0, None);

        let mut packet = Packet::syn();
        packet.set_connection_id(key.receive_id);

        // Queue the syn packet
        let _ = out_queue.push(packet);

        Self::new(
            State::SynSent,
            key,
            out_queue,
            InQueue::new(None),
            Some(DEFAULT_TIMEOUT_MS),
        )
    }

    /// Constructs new incoming connection from Syn packet.
    fn new_incoming(key: Key, send_id: u16, ack_nr: u16) -> Self {
        let seq_nr = util::rand();
        let out_queue = OutQueue::new(send_id, seq_nr, Some(ack_nr));
        let in_queue = InQueue::new(Some(ack_nr));
        Self::new(State::SynRecv, key, out_queue, in_queue, None)
    }

    /// Checks if connection is readable.
    /// Reads are closed when we receive Fin packet.
    fn read_open(&self) -> bool {
        self.fin_received.is_none()
    }

    fn update_local_window(&mut self) {
        self.out_queue
            .set_local_window(self.in_queue.local_window());
    }

    /// Sets connection closed signal transmitter which is used to notify graceful connnection
    /// shutdown.
    fn set_closed_tx(&mut self, closed_tx: oneshot::Sender<()>) {
        if self.state == State::Closed {
            let _ = closed_tx.send(());
        } else {
            self.closed_tx = Some(closed_tx);
        }
    }

    /// Process an inbound packet for the connection
    fn process(
        &mut self,
        cx: &mut Context<'_>,
        packet: Packet,
        shared: &mut Shared,
    ) -> Poll<Result<bool>> {
        if self.state == State::Reset {
            return Ok(self.is_finalized()).into();
        }

        if packet.ty() == packet::Type::Reset {
            self.state = State::Reset;

            // Update readiness
            self.update_readiness();

            return Ok(self.is_finalized()).into();
        }

        // TODO: Invalid packets should be discarded here.

        let now = Instant::now();
        self.update_delays(now, &packet);
        self.out_queue.set_peer_window(packet.wnd_size());

        self.check_acks_fin_sent(&packet);
        let packet_accepted = if packet.is_ack() {
            self.process_ack(&packet);
            true
        } else {
            // TODO: validate the packet's ack_nr

            self.out_queue.maybe_resend_ack_for(&packet);
            // Add the packet to the inbound queue. This handles ordering
            trace!("inqueue -- push packet");
            self.in_queue.push(packet)
        };

        // TODO: count duplicate ACK counter

        trace!("polling from in_queue");
        while let Some(packet) = self.in_queue.poll() {
            self.process_queued(&packet);
        }

        trace!(
            "updating local window, acks; window={:?}; ack={:?}",
            self.in_queue.local_window(),
            self.in_queue.ack_nr()
        );

        self.update_local_window();
        let (ack_nr, selective_acks) = self.in_queue.ack_nr();
        self.out_queue.set_local_ack(ack_nr, selective_acks);
        self.last_recv_time = Instant::now();

        if packet_accepted {
            self.reset_timeout();
        }

        // Flush out queue
        ready!(self.poll_flush(cx, shared))?;

        // Update readiness
        self.update_readiness();

        Ok(self.is_finalized()).into()
    }

    fn check_acks_fin_sent(&mut self, packet: &Packet) {
        if self.acks_fin_sent(&packet) {
            self.fin_sent_acked = true;
            if self.fin_received.is_some() {
                self.notify_conn_closed();
            }
        }
    }

    /// Handles `State` packets.
    fn process_ack(&mut self, packet: &Packet) {
        // State packets are special, they do not have an associated sequence number, thus do not
        // require ordering. They are only used to ACK packets, which is handled above, and to
        // transition a connection into the connected state.
        if self.state == State::SynSent {
            self.in_queue.set_initial_ack_nr(packet.seq_nr());
            self.state = State::Connected;
        }
    }

    /// Processes packet that was already queued.
    fn process_queued(&mut self, packet: &Packet) {
        trace!("process; packet={:?}; state={:?}", packet, self.state);
        // At this point, we only receive CTL frames. Data is held in the queue
        match packet.ty() {
            packet::Type::Reset => {
                self.state = State::Reset;
            }
            packet::Type::Fin => {
                self.fin_received = Some(packet.seq_nr());
            }
            packet::Type::Data | packet::Type::Syn | packet::Type::State => unreachable!(),
        }
    }

    /// Checks if given packet acks Fin we sent.
    fn acks_fin_sent(&self, packet: &Packet) -> bool {
        self.fin_sent.map_or(false, |seq_nr| {
            packet::acks_seq_nr(seq_nr, packet.ack_nr(), &packet.selective_acks())
        })
    }

    /// Returns true, if socket is still ready to write.
    fn poll_flush(&mut self, cx: &mut Context<'_>, shared: &mut Shared) -> Poll<Result<()>> {
        let mut sent = false;

        if self.state == State::Reset {
            return Ok(()).into();
        }

        while let Some(next) = self.out_queue.next() {
            if !shared.is_ready() {
                shared.register(cx);
                return Poll::Pending;
            }

            trace!(
                "send_to; addr={:?}; packet={:?}",
                self.key.addr,
                next.packet()
            );

            // this packet acks Fin we received before.
            if (next.packet().is_ack()
                && self
                    .fin_received
                    .map_or(false, |seq_nr| seq_nr == next.packet().ack_nr())
                && self.fin_sent_acked)
                || (self.fin_received.is_some() && next.fin_resend_limit_reached())
            {
                if let Some(closed_tx) = self.closed_tx.take() {
                    let _ = closed_tx.send(());
                }
                self.state = State::Closed;
            }

            // We randomly drop packets when testing.
            //#[cfg(test)]
            //let drop_packet = self.loss_rate >= util::rand();
            //#[cfg(not(test))]
            let drop_packet = false;

            if drop_packet {
                next.sent();
                sent = true;
            } else {
                match shared.poll_send_to(cx, next.packet().as_slice(), &self.key.addr) {
                    Poll::Ready(Ok(n)) => {
                        assert_eq!(n, next.packet().as_slice().len());
                        next.sent();

                        // Reset the connection timeout
                        sent = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        shared.register(cx);
                        return Poll::Pending;
                    }
                }
            }
        }

        if sent {
            self.reset_timeout();
        }

        Ok(()).into()
    }

    /// Schedule Fin packet to be sent to remote peer, if it was not schedules yet.
    /// Returns true, if Fin was scheduled, false otherwise.
    fn schedule_fin(&mut self) -> bool {
        if !self.state.is_closed() && self.fin_sent.is_none() {
            let seq_nr = self.out_queue.push(Packet::fin());
            self.fin_sent = Some(seq_nr);
            true
        } else {
            false
        }
    }

    /// Wakes up `UtpStreamFinalize` future.
    fn notify_conn_closed(&mut self) {
        if let Some(closed_tx) = self.closed_tx.take() {
            let _ = closed_tx.send(());
        }
        self.state = State::Closed;
    }

    fn tick(
        &mut self,
        cx: &mut Context<'_>,
        shared: &mut Shared,
    ) -> Poll<Result<Option<(u16, SocketAddr)>>> {
        if self.state == State::Reset {
            return Ok(None).into();
        }

        let now = Instant::now();
        if now > self.last_recv_time + Duration::new(u64::from(self.disconnect_timeout_secs), 0) {
            self.state = State::Reset;
            self.update_readiness();
            return Ok(Some((self.out_queue.connection_id(), self.key.addr))).into();
        }

        if let Some(deadline) = self.deadline {
            if now >= deadline {
                trace!(
                    "connection timed out; id={}",
                    self.out_queue.connection_id()
                );
                self.out_queue.timed_out();
                ready!(self.poll_flush(cx, shared))?;
            }
        }

        Ok(None).into()
    }

    // TODO(povilas): extract to congestion control structure
    fn update_delays(&mut self, now: Instant, packet: &Packet) {
        let mut actual_delay = u32::MAX;

        if packet.timestamp() > 0 {
            self.fix_clock_skew(packet, now);

            actual_delay = packet.timestamp_diff();
            if actual_delay != u32::MAX {
                self.our_delays.add_sample(actual_delay, now);

                if self.average_delay_base == 0 {
                    self.average_delay_base = actual_delay;
                }
                self.sum_delay_diffs(actual_delay);

                // recalculate delays every 5 seconds or so
                if now > self.average_sample_time {
                    let prev_average_delay = self.average_delay;
                    self.recalculate_avg_delay();
                    self.average_sample_time = now + Duration::from_secs(5);
                    let prev_average_delay = self.adjust_average_delay(prev_average_delay);

                    // Update the clock drive estimate
                    let drift = i64::from(self.average_delay) - i64::from(prev_average_delay);

                    self.clock_drift = ((i64::from(self.clock_drift) * 7 + drift) / 8) as i32;
                }
            }
        }

        // Ack all packets
        if let Some((acked_bytes, min_rtt)) =
            self.out_queue
                .set_their_ack(packet.ack_nr(), &packet.selective_acks(), now)
        {
            let min_rtt = util::as_wrapping_micros(min_rtt);

            if let Some(delay) = self.our_delays.get() {
                if delay > min_rtt {
                    self.our_delays.shift(delay.wrapping_sub(min_rtt));
                }
            }

            if actual_delay != u32::MAX && acked_bytes >= 1 {
                trace!(
                    "applying congenstion control; bytes_acked={}; actual_delay={}; min_rtt={}",
                    acked_bytes,
                    actual_delay,
                    min_rtt
                );
                self.apply_congestion_control(acked_bytes, min_rtt, now);
            }
        }
    }

    fn fix_clock_skew(&mut self, packet: &Packet, now: Instant) {
        // Use the packet to update the delay value
        let their_delay = self.out_queue.update_their_delay(packet.timestamp());
        let prev_base_delay = self.their_delays.base_delay();

        // Track the delay
        self.their_delays.add_sample(their_delay, now);

        if let Some(prev) = prev_base_delay {
            let new = self.their_delays.base_delay().unwrap();

            // If their new base delay is less than their previous one, we
            // should shift our delay base in the other direction in order
            // to take the clock skew into account.
            let lt = util::wrapping_lt(new, prev, TIMESTAMP_MASK);
            let diff = prev.wrapping_sub(new);

            if lt && diff <= 10_000 {
                self.our_delays.shift(diff);
            }
        }
    }

    /// Sums the difference between given delay and current average base delay.
    /// If current delay is bigger, the difference is added. If it's smaller, the diff is
    /// subtracted. So `self.current_delay_sum` is tracking how delays are increasing.
    // TODO(povilas): extract to congestion control structure
    fn sum_delay_diffs(&mut self, actual_delay: u32) {
        let average_delay_sample;
        let dist_down = self.average_delay_base.wrapping_sub(actual_delay);
        let dist_up = actual_delay.wrapping_sub(self.average_delay_base);

        // the logic behind this is:
        //   if wrapping_lt(self.average_delay_base, actual_delay)
        if dist_down > dist_up {
            average_delay_sample = i64::from(dist_up);
        } else {
            average_delay_sample = -i64::from(dist_down);
        }

        self.current_delay_sum = self.current_delay_sum.wrapping_add(average_delay_sample);
        self.current_delay_samples += 1;
    }

    fn recalculate_avg_delay(&mut self) {
        self.average_delay = (self.current_delay_sum / self.current_delay_samples) as i32;
        self.current_delay_sum = 0;
        self.current_delay_samples = 0;
    }

    fn adjust_average_delay(&mut self, mut prev_average_delay: i32) -> i32 {
        let min_sample = cmp::min(prev_average_delay, self.average_delay);
        let max_sample = cmp::max(prev_average_delay, self.average_delay);

        if min_sample > 0 {
            self.average_delay_base = self.average_delay_base.wrapping_add(min_sample as u32);
            self.average_delay -= min_sample;
            prev_average_delay -= min_sample;
        } else if max_sample < 0 {
            let adjust = -max_sample;

            self.average_delay_base = self.average_delay_base.wrapping_sub(adjust as u32);
            self.average_delay += adjust;
            prev_average_delay += adjust;
        }

        prev_average_delay
    }

    // TODO(povilas): extract to congestion control structure
    fn apply_congestion_control(&mut self, bytes_acked: usize, min_rtt: u32, now: Instant) {
        let target = TARGET_DELAY;

        let mut our_delay = cmp::min(self.our_delays.get().unwrap(), min_rtt);
        let max_window = self.out_queue.max_window() as usize;

        if self.clock_drift < -200_000 {
            let penalty = (-self.clock_drift - 200_000) / 7;

            if penalty > 0 {
                our_delay += penalty as u32;
            } else {
                our_delay -= (-penalty) as u32;
            }
        }

        let off_target = (i64::from(target) - i64::from(our_delay)) as f64;
        let delay_factor = off_target / f64::from(target);

        let window_factor =
            cmp::min(bytes_acked, max_window) as f64 / cmp::max(max_window, bytes_acked) as f64;
        let mut scaled_gain = MAX_CWND_INCREASE_BYTES_PER_RTT as f64 * window_factor * delay_factor;

        if scaled_gain > 0.0 && now - self.last_maxed_out_window > Duration::from_secs(1) {
            // if it was more than 1 second since we tried to send a packet and
            // stopped because we hit the max window, we're most likely rate
            // limited (which prevents us from ever hitting the window size) if
            // this is the case, we cannot let the max_window grow indefinitely
            scaled_gain = 0.0;
        }

        let new_window = max_window as i64 + scaled_gain as i64;
        let ledbat_cwnd = if new_window < MIN_WINDOW_SIZE as i64 {
            MIN_WINDOW_SIZE
        } else {
            new_window as usize
        };

        if self.slow_start {
            let ss_cwnd = max_window + window_factor as usize * MAX_DATA_SIZE;

            if ss_cwnd > SLOW_START_THRESHOLD || our_delay > (f64::from(target) * 0.9) as u32 {
                // Even if we're a little under the target delay, we
                // conservatively discontinue the slow start phase
                self.slow_start = false;
            } else {
                self.out_queue
                    .set_max_window(cmp::max(ss_cwnd, ledbat_cwnd) as u32);
            }
        } else {
            self.out_queue.set_max_window(ledbat_cwnd as u32);
        }
    }

    fn reset_timeout(&mut self) {
        self.deadline = self.out_queue.socket_timeout().map(|dur| {
            trace!("resetting timeout; duration={:?}", dur);
            Instant::now() + dur
        });
    }

    fn is_finalized(&self) -> bool {
        self.released
            && ((self.out_queue.is_empty() && self.state.is_closed()) || self.state == State::Reset)
    }

    /// Update the UtpStream's readiness
    fn update_readiness(&mut self) {
        if self.state == State::Connected {
            if self.is_readable() || self.is_writable() {
                if let Some(waker) = self.waker.take() {
                    waker.wake();
                }
            }
        } else if self.state.is_closed() {
            // when connection is closed we need to unblock read call. The way to do this is
            // to set "read ready" flag. Then `stream.read_immutable` will return 0 indicating EOF.
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    // =========

    /// Returns true, if connection should be polled for reads.
    fn is_readable(&self) -> bool {
        // Note that when reads are closed (`read_open() -> false`), connection is readable.
        // That's because we want to get `stream.read_immutable()` called and in such case it
        // returns 0 indicating that connection reads were closed - Fin was received.
        !self.read_open() || self.in_queue.is_readable()
    }

    fn is_writable(&self) -> bool {
        self.write_open && self.out_queue.is_writable()
    }
}

impl State {
    /// Returns `true`, if connection is being closed or is already closed and we should not
    /// send any more data over this connection
    fn is_closed(&self) -> bool {
        match *self {
            State::FinSent | State::Reset | State::Closed => true,
            _ => false,
        }
    }
}

impl Key {
    fn new(receive_id: u16, addr: SocketAddr) -> Key {
        Key { receive_id, addr }
    }
}

/// A future that resolves with the connected `UtpStream`.
pub struct UtpStreamConnect {
    state: UtpStreamConnectState,
}

enum UtpStreamConnectState {
    Err(Error),
    Empty,
    Waiting(UtpStream),
}

impl Future for UtpStreamConnect {
    type Output = Result<UtpStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = mem::replace(&mut self.state, UtpStreamConnectState::Empty);
        match inner {
            UtpStreamConnectState::Waiting(stream) => {
                let mut x = unwrap!(stream.inner.write());
                let inner = &mut *x;
                let conn = &mut inner.connections[stream.token];
                match conn.poll_flush(cx, &mut inner.shared) {
                    Poll::Pending => {
                        drop(x);
                        self.state = UtpStreamConnectState::Waiting(stream);
                        Poll::Pending
                    }
                    Poll::Ready(_) => {
                        drop(x);
                        Ok(stream).into()
                    }
                }
            }
            UtpStreamConnectState::Err(e) => Err(e).into(),
            UtpStreamConnectState::Empty => panic!("can't poll UtpStreamConnect twice!"),
        }
    }
}

struct SocketRefresher {
    inner: InnerCell,
    next_tick: Instant,
    timeout: Delay,
    req_finalize: oneshot::Receiver<oneshot::Sender<()>>,
}

impl Future for SocketRefresher {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if let Err(e) = ready!(self.poll_inner(cx)) {
            error!("UtpSocket died! {:?}", e);
        }
        Poll::Ready(())
    }
}

impl SocketRefresher {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while let Poll::Ready(()) = self.timeout.poll_unpin(cx) {
            unwrap!(self.inner.write()).tick(cx)?;
            self.next_tick += Duration::from_millis(500);
            self.timeout.reset(self.next_tick.into());
        }

        if let Poll::Ready(true) = unwrap!(self.inner.write()).poll_refresh(cx, &self.inner)? {
            if 1 == Arc::strong_count(&self.inner) {
                if let Poll::Ready(Ok(resp_finalize)) = self.req_finalize.poll_unpin(cx) {
                    let _ = resp_finalize.send(());
                }
                return Poll::Ready(Ok(()));
            }
        }
        Poll::Pending
    }
}

/// A future that resolves once the socket has been finalised. Created via `UtpSocket::finalize`.
pub struct UtpSocketFinalize {
    resp_finalize: oneshot::Receiver<()>,
}

impl Future for UtpSocketFinalize {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.resp_finalize.poll_unpin(cx));
        unwrap!(result);
        Poll::Ready(())
    }
}

/// A stream of incoming uTP connections. Created via `UtpListener::incoming`.
pub struct Incoming {
    listener: UtpListener,
}

impl Stream for Incoming {
    type Item = Result<UtpStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let s = ready!(self.listener.poll_accept(cx))?;
        Poll::Ready(Some(Ok(s)))
    }
}

impl AsyncRead for UtpStream {
    unsafe fn prepare_uninitialized_buffer(&self, _: &mut [MaybeUninit<u8>]) -> bool {
        false
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        self.poll_read_immutable(cx, buf)
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        self.poll_write_immutable(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush_immutable(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_shutdown_write(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{future, StreamExt};

    /// Reduces boilerplate and creates connection with some specific parameters.
    fn test_connection() -> Connection {
        let key = Key {
            receive_id: 12_345,
            addr: addr!("1.2.3.4:5000"),
        };
        Connection::new_outgoing(key, 12_346)
    }

    mod inner {
        use super::*;
        use futures::channel::mpsc;

        /// Creates new UDP socket and waits for incoming uTP packets.
        /// Returns future that yields received packet and listener address.
        async fn wait_for_packets() -> (mpsc::UnboundedReceiver<Packet>, SocketAddr) {
            let (packets_tx, packets_rx) = mpsc::unbounded();
            let mut listener = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")).await);
            let listener_addr = unwrap!(listener.local_addr());

            let recv_response = async move {
                let mut buf = vec![0u8; 256];
                loop {
                    let (n, _addr) = unwrap!(listener.recv_from(&mut buf[..]).await);
                    let buf = BytesMut::from(&buf[..n]);
                    let packet = unwrap!(Packet::parse(buf));
                    unwrap!(packets_tx.unbounded_send(packet));
                }
            };
            tokio::spawn(recv_response);

            (packets_rx, listener_addr)
        }

        /// Reduce some boilerplate.
        /// Returns socket inner with some common defaults.
        async fn make_socket_inner() -> InnerCell {
            let socket = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")).await);
            Inner::new_shared(socket)
        }

        mod process_unknown {
            use super::*;

            #[tokio::test]
            async fn when_packet_is_syn_it_schedules_reset_back() {
                let inner = make_socket_inner().await;

                let mut packet = Packet::syn();
                packet.set_connection_id(12_345);
                let peer_addr = addr!("1.2.3.4:5000");

                unwrap!(unwrap!(inner.write()).process_unknown(packet, peer_addr));

                let packet_opt = unwrap!(inner.write()).reset_packets.pop_front();
                let (packet, dest_addr) = unwrap!(packet_opt);
                assert_eq!(packet.connection_id(), 12_345);
                assert_eq!(packet.ty(), packet::Type::Reset);
                assert_eq!(dest_addr, addr!("1.2.3.4:5000"));
            }

            #[tokio::test]
            async fn when_packet_is_reset_nothing_is_sent_back() {
                let inner = make_socket_inner().await;

                let mut packet = Packet::reset();
                packet.set_connection_id(12_345);
                let peer_addr = addr!("1.2.3.4:5000");

                unwrap!(unwrap!(inner.write()).process_unknown(packet, peer_addr));

                assert!(unwrap!(inner.write()).reset_packets.is_empty());
            }
        }

        mod flush_reset_packets {
            use super::*;
            use hamcrest::prelude::*;

            #[tokio::test]
            async fn it_attempts_to_send_all_queued_reset_packets() {
                let inner = make_socket_inner().await;

                let task = async {
                    let (packets_rx, remote_peer_addr) = wait_for_packets().await;

                    let mut packet = Packet::reset();
                    packet.set_connection_id(12_345);
                    unwrap!(unwrap!(inner.write())
                        .reset_packets
                        .push_back((packet, remote_peer_addr)));
                    let mut packet = Packet::reset();
                    packet.set_connection_id(23_456);
                    unwrap!(unwrap!(inner.write())
                        .reset_packets
                        .push_back((packet, remote_peer_addr)));

                    // keep retrying until all packets are sent out
                    let result =
                        future::poll_fn(move |cx| unwrap!(inner.write()).flush_reset_packets(cx))
                            .await;
                    unwrap!(result);
                    packets_rx.take(2).collect::<Vec<_>>().await
                };
                let packets = task.await;

                let packet_ids: Vec<u16> = packets.iter().map(|p| p.connection_id()).collect();
                assert_that!(&packet_ids, contains(vec![12_345, 23_456]).exactly());
            }
        }

        mod process_syn {
            use super::*;

            #[tokio::test]
            async fn when_listener_is_closed_it_schedules_reset_packet_back() {
                let inner = make_socket_inner().await;
                unwrap!(inner.write()).listener_open = false;

                let mut packet = Packet::syn();
                packet.set_connection_id(12_345);
                let peer_addr = addr!("1.2.3.4:5000");

                unwrap!(
                    future::poll_fn(|cx| {
                        unwrap!(inner.write()).process_syn(cx, &packet, peer_addr, &inner)
                    })
                    .await
                );

                let packet_opt = unwrap!(inner.write()).reset_packets.pop_front();
                let (packet, dest_addr) = unwrap!(packet_opt);
                assert_eq!(packet.connection_id(), 12_345);
                assert_eq!(packet.ty(), packet::Type::Reset);
                assert_eq!(dest_addr, addr!("1.2.3.4:5000"));
            }
        }

        mod shutdown_write {
            use super::*;

            #[tokio::test]
            async fn it_closes_further_writes() {
                let inner = make_socket_inner().await;
                let mut inner = unwrap!(inner.write());

                let conn = test_connection();
                assert!(conn.write_open);
                let conn_token = inner.connections.insert(conn);

                unwrap!(future::poll_fn(|cx| inner.poll_shutdown_write(cx, conn_token)).await);

                let conn = &inner.connections[conn_token];
                assert!(!conn.write_open);
            }

            #[tokio::test]
            async fn when_fin_was_not_sent_yet_it_enqueues_fin_packet() {
                let inner = make_socket_inner().await;
                let mut inner = unwrap!(inner.write());

                let mut conn = test_connection();
                if let Some(next) = conn.out_queue.next() {
                    next.sent()
                } // skip queued Syn packet
                let conn_token = inner.connections.insert(conn);

                unwrap!(future::poll_fn(|cx| inner.poll_shutdown_write(cx, conn_token)).await);

                let conn = &mut inner.connections[conn_token];
                if let Some(next) = conn.out_queue.next() {
                    assert_eq!(next.packet().ty(), packet::Type::Fin);
                } else {
                    panic!("Packet expected in out_queue");
                }
            }
        }

        mod close {
            use super::*;

            #[tokio::test]
            async fn when_fin_packet_was_sent_it_does_not_enqueue_another() {
                let inner = make_socket_inner().await;
                let mut inner = unwrap!(inner.write());

                let mut conn = test_connection();
                if let Some(next) = conn.out_queue.next() {
                    next.sent()
                } // skip queued Syn packet
                let conn_token = inner.connections.insert(conn);
                unwrap!(future::poll_fn(|cx| inner.poll_shutdown_write(cx, conn_token)).await);

                inner.close(conn_token);

                let conn = &mut inner.connections[conn_token];
                // skip first queued Fin packet
                if let Some(next) = conn.out_queue.next() {
                    next.sent()
                }
                assert!(conn.out_queue.next().is_none());
            }
        }
    }

    mod connection {
        use super::*;

        mod tick {
            use super::*;

            #[tokio::test]
            async fn when_no_data_is_received_within_timeout_it_schedules_reset() {
                let key = Key {
                    receive_id: 12_345,
                    addr: addr!("1.2.3.4:5000"),
                };
                let mut conn = Connection::new_outgoing(key, 12_346);
                // make connection timeout
                conn.disconnect_timeout_secs = 0;
                let sock = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")).await);
                let mut shared = Shared::new(sock);

                let reset_info = unwrap!(future::poll_fn(|cx| conn.tick(cx, &mut shared)).await);

                assert_eq!(reset_info, Some((12_346, addr!("1.2.3.4:5000"))));
            }
        }

        mod sum_delay_diffs {
            use super::*;

            mod when_avg_delay_base_is_bigger_than_actual_delay {
                use super::*;

                #[test]
                fn it_adds_their_negative_difference() {
                    let mut conn = test_connection();
                    conn.average_delay_base = 500;

                    conn.sum_delay_diffs(200);

                    assert_eq!(conn.current_delay_sum, -300);
                }
            }

            mod when_actual_delay_is_bigger_than_avg_base_delay {
                use super::*;

                #[test]
                fn it_adds_their_difference() {
                    let mut conn = test_connection();
                    conn.average_delay_base = 200;

                    conn.sum_delay_diffs(500);

                    assert_eq!(conn.current_delay_sum, 300);
                }
            }
        }

        mod recalculate_avg_delay {
            use super::*;

            #[test]
            fn it_sets_average_delay_from_current_delay_sum() {
                let mut conn = test_connection();
                conn.current_delay_sum = 2500;
                conn.current_delay_samples = 5;

                conn.recalculate_avg_delay();

                assert_eq!(conn.average_delay, 500);
            }

            #[test]
            fn it_clears_current_delay_sum() {
                let mut conn = test_connection();
                conn.current_delay_sum = 2500;
                conn.current_delay_samples = 5;

                conn.recalculate_avg_delay();

                assert_eq!(conn.current_delay_sum, 0);
                assert_eq!(conn.current_delay_samples, 0);
            }
        }

        mod adjust_average_delay {
            use super::*;

            mod when_prev_and_current_avg_delays_are_positive {
                use super::*;

                #[test]
                fn it_adds_smaller_average_delay_to_avg_delay_base() {
                    let mut conn = test_connection();
                    conn.average_delay = 4000;
                    conn.average_delay_base = 1000;

                    let _ = conn.adjust_average_delay(2000);

                    assert_eq!(conn.average_delay_base, 3000);
                }

                #[test]
                fn it_adds_smaller_average_delay_to_avg_delay_base_and_wraps_when_overflow() {
                    let mut conn = test_connection();
                    conn.average_delay = 4000;
                    conn.average_delay_base = -1000i32 as u32;

                    let _ = conn.adjust_average_delay(2000);

                    assert_eq!(conn.average_delay_base, 1000);
                }

                #[test]
                fn it_subtracts_smaller_average_delay_from_avg_delay() {
                    let mut conn = test_connection();
                    conn.average_delay = 4000;

                    let _ = conn.adjust_average_delay(1000);

                    assert_eq!(conn.average_delay, 3000);
                }

                #[test]
                fn it_subtracts_smaller_average_delay_from_prev_avg_delay() {
                    let mut conn = test_connection();
                    conn.average_delay = 1000;

                    let prev_average_delay = conn.adjust_average_delay(4000);

                    assert_eq!(prev_average_delay, 3000);
                }
            }

            mod when_prev_and_current_avg_delays_are_negative {
                use super::*;

                #[test]
                fn it_subtracts_bigger_average_delay_from_avg_delay_base() {
                    let mut conn = test_connection();
                    conn.average_delay = -4000;
                    conn.average_delay_base = 5000;

                    let _ = conn.adjust_average_delay(-2000);

                    assert_eq!(conn.average_delay_base, 3000);
                }

                #[test]
                fn it_subtracts_bigger_avg_delay_from_avg_delay_base_and_wraps_when_underflow() {
                    let mut conn = test_connection();
                    conn.average_delay = -4000;
                    conn.average_delay_base = 1000;

                    let _ = conn.adjust_average_delay(-2000);

                    assert_eq!(conn.average_delay_base, -1000i32 as u32);
                }

                #[test]
                fn it_adds_bigger_average_delay_to_avg_delay() {
                    let mut conn = test_connection();
                    conn.average_delay = -4000;

                    let _ = conn.adjust_average_delay(-1000);

                    assert_eq!(conn.average_delay, -3000);
                }

                #[test]
                fn it_adds_bigger_average_delay_to_prev_avg_delay() {
                    let mut conn = test_connection();
                    conn.average_delay = -1000;

                    let prev_average_delay = conn.adjust_average_delay(-4000);

                    assert_eq!(prev_average_delay, -3000);
                }
            }
        }

        mod read_open {
            use super::*;

            #[test]
            fn when_fin_was_not_received_it_returns_true() {
                let conn = test_connection();
                assert!(conn.fin_received.is_none());

                assert!(conn.read_open());
            }

            #[test]
            fn when_fin_received_it_returns_false() {
                let mut conn = test_connection();
                conn.fin_received = Some(123);

                assert!(!conn.read_open());
            }
        }

        mod is_readable {
            use super::*;

            #[test]
            fn when_reads_are_closed_it_returns_true() {
                let mut conn = test_connection();
                conn.fin_received = Some(123);

                assert!(conn.is_readable());
            }

            #[test]
            fn when_reads_are_open_but_input_queue_is_emtpy_it_returns_false() {
                let conn = test_connection();
                assert!(conn.fin_received.is_none());

                assert!(!conn.is_readable());
            }
        }

        mod schedule_fin {
            use super::*;

            mod when_fin_was_not_sent_yet {
                use super::*;

                #[test]
                fn it_enqueues_fin_packet() {
                    let mut conn = test_connection();
                    // skip queued Syn packet
                    if let Some(next) = conn.out_queue.next() {
                        next.sent()
                    }

                    let _ = conn.schedule_fin();

                    if let Some(next) = conn.out_queue.next() {
                        assert_eq!(next.packet().ty(), packet::Type::Fin);
                    } else {
                        panic!("Packet expected in out_queue");
                    }
                }

                #[test]
                fn it_notes_that_fin_was_sent() {
                    let mut conn = test_connection();
                    assert!(conn.fin_sent.is_none());

                    let _ = conn.schedule_fin();

                    assert!(conn.fin_sent.is_some());
                }

                #[test]
                fn it_returns_true() {
                    let mut conn = test_connection();

                    let fin_queued = conn.schedule_fin();

                    assert!(fin_queued);
                }
            }

            #[test]
            fn when_fin_was_already_queued_it_returns_false() {
                let mut conn = test_connection();
                let _ = conn.schedule_fin();

                let fin_queued = conn.schedule_fin();

                assert!(!fin_queued);
            }
        }

        mod acks_fin_sent {
            use super::*;

            #[test]
            fn when_fin_was_not_sent_yet_it_returns_false() {
                let conn = test_connection();
                assert!(conn.fin_sent.is_none());

                let acks = conn.acks_fin_sent(&Packet::state());

                assert!(!acks);
            }

            mod when_fin_was_sent {
                use super::*;

                #[test]
                fn when_ack_packet_is_not_for_our_fin_it_returns_false() {
                    let mut conn = test_connection();
                    conn.fin_sent = Some(5);
                    let mut packet = Packet::state();
                    packet.set_ack_nr(4);

                    let acks = conn.acks_fin_sent(&packet);

                    assert!(!acks);
                }

                #[test]
                fn when_ack_packet_is_or_our_fin_it_returns_true() {
                    let mut conn = test_connection();
                    conn.fin_sent = Some(5);
                    let mut packet = Packet::state();
                    packet.set_ack_nr(5);

                    let acks = conn.acks_fin_sent(&packet);

                    assert!(acks);
                }
            }
        }

        mod check_acks_fin_sent {
            use super::*;

            #[test]
            fn it_notes_that_our_fin_was_acked() {
                let mut conn = test_connection();
                conn.fin_sent = Some(5);
                assert!(!conn.fin_sent_acked);

                let mut packet = Packet::state();
                packet.set_ack_nr(5);
                conn.check_acks_fin_sent(&packet);

                assert!(conn.fin_sent_acked);
            }

            #[test]
            fn when_fin_was_received_connection_state_is_changed_to_closed() {
                let mut conn = test_connection();
                conn.fin_sent = Some(5);
                conn.fin_received = Some(123);
                assert!(!conn.fin_sent_acked);

                let mut packet = Packet::state();
                packet.set_ack_nr(5);
                conn.check_acks_fin_sent(&packet);

                assert_eq!(conn.state, State::Closed);
            }
        }

        mod process_queued {
            use super::*;

            #[test]
            fn when_packet_is_fin_it_saves_its_sequence_number() {
                let mut conn = test_connection();
                assert!(conn.fin_received.is_none());
                let mut fin_packet = Packet::fin();
                fin_packet.set_seq_nr(123);

                conn.process_queued(&fin_packet);

                assert_eq!(conn.fin_received, Some(123));
            }
        }

        mod set_closed_tx {
            use super::*;

            #[tokio::test]
            async fn when_connection_is_already_closed_it_notifies_via_given_transmitter() {
                let mut conn = test_connection();
                conn.state = State::Closed;

                let (closed_tx, closed_rx) = oneshot::channel();
                conn.set_closed_tx(closed_tx);

                unwrap!(closed_rx.await);
            }
        }
    }

    mod state {
        use super::*;

        #[test]
        fn is_closed_returns_true_for_states_when_we_shouldnt_send_data_over_connection() {
            let closing_states = vec![State::FinSent, State::Reset, State::Closed];

            for state in closing_states {
                assert!(state.is_closed());
            }
        }

        #[test]
        fn is_closed_returns_false_for_states_when_we_can_send_data_over_connection() {
            let closing_states = vec![State::SynSent, State::SynRecv, State::Connected];

            for state in closing_states {
                assert!(!state.is_closed());
            }
        }
    }

    mod utp_stream {
        use super::*;

        mod flush_immutable {
            use super::*;
            use futures::future;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            #[tokio::test]
            async fn when_writes_are_blocked_it_reschedules_current_task_polling_in_the_future() {
                env_logger::init();
                let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")).await);
                let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")).await);
                let listener_addr = unwrap!(listener.local_addr());

                tokio::spawn(async {
                    let mut incoming = listener.incoming();
                    while let Some(stream) = incoming.next().await {
                        let mut stream = unwrap!(stream);
                        let f = async {
                            unwrap!(stream.write_all(b"some data").await);
                            unwrap!(stream.read_to_end(&mut vec![]).await);
                        };
                        let result = tokio::time::timeout(Duration::from_secs(100), f).await;
                        unwrap!(result)
                    }
                });

                let stream = unwrap!(sock.connect(&listener_addr).await);

                let mut flush_called = 0_usize;
                let flush_tx = future::poll_fn(move |cx| {
                    if flush_called == 0 {
                        flush_called += 1;
                        // block the first flush
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    flush_called += 1;

                    ready!(stream.poll_flush_immutable(cx))?;
                    Ok::<_, Error>(flush_called).into()
                });
                let flush_called = unwrap!(flush_tx.await);
                assert!(flush_called > 1);
            }
        }
    }
}

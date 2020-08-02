use crate::UtpStream;
use tokio::io::{AsyncRead, AsyncWrite};

use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct ReadHalf<'a>(&'a UtpStream);

#[derive(Debug)]
pub struct WriteHalf<'a>(&'a UtpStream);

pub(crate) fn split(stream: &mut UtpStream) -> (ReadHalf<'_>, WriteHalf<'_>) {
    (ReadHalf(&*stream), WriteHalf(&*stream))
}

impl AsyncRead for ReadHalf<'_> {
    unsafe fn prepare_uninitialized_buffer(&self, _: &mut [MaybeUninit<u8>]) -> bool {
        false
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_read_immutable(cx, buf)
    }
}

impl AsyncWrite for WriteHalf<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_write_immutable(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_flush_immutable(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_shutdown_write(cx)
    }
}

impl AsRef<UtpStream> for ReadHalf<'_> {
    fn as_ref(&self) -> &UtpStream {
        self.0
    }
}

impl AsRef<UtpStream> for WriteHalf<'_> {
    fn as_ref(&self) -> &UtpStream {
        self.0
    }
}

use std::{
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::{atomic::{AtomicBool, Ordering}, Arc},
    task::{Context, Poll},
};
use futures::{FutureExt, Sink, SinkExt};
use tokio::{net::{TcpListener, TcpStream}, net::tcp::OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{Message, Handler, Decodec, EnCodec, Writer};

pub struct TcpReceiver<SendMsg, RecvMsg, MsgHandler>
where
    SendMsg: Message,
    RecvMsg: Message,
    MsgHandler: Handler<SendMsg, RecvMsg>,
{
    _x: PhantomData<(SendMsg, RecvMsg)>,
    address: SocketAddr,
    handler: MsgHandler,
}

impl<SendMsg, RecvMsg, MsgHandler> TcpReceiver<SendMsg, RecvMsg, MsgHandler>
where 
    SendMsg: Message,
    RecvMsg: Message,
    MsgHandler: Handler<SendMsg, RecvMsg>,
{
    pub fn spawn(address: SocketAddr, handler: MsgHandler) {
        tokio::spawn(async move {
            Self { _x: PhantomData, address, handler }.run().await;
        });
    }
    
    fn into_sink(writer: AckBatcher<FramedWrite<OwnedWriteHalf, EnCodec<SendMsg>>>) -> Writer<SendMsg> 
    {
        Box::new(writer) as Writer<SendMsg>
    }

    fn spawn_runner(socket: TcpStream, peer_address: SocketAddr, handler: MsgHandler) 
    {
        tokio::spawn(async move {
            let (rd, wr) = socket.into_split();
            let mut reader = FramedRead::new(
                rd, 
                Decodec::<RecvMsg>::new()
            );
            let force_flush = Arc::new(AtomicBool::new(false));
            let writer = AckBatcher {
                inner: FramedWrite::new(wr, EnCodec::<SendMsg>::new()),
                force_flush: force_flush.clone(),
            };
            let mut writer = Self::into_sink(writer); // Convert to dynamic type
            // for uniform abstraction for users

            loop {
                // Handlers write their replies (ACKs) without a flush. While more
                // frames are ready to read, keep dispatching; flush the replies
                // only before waiting for new input, so that one write carries
                // the replies to a whole burst of messages.
                let next = match reader.next().now_or_never() {
                    Some(next) => next,
                    None => {
                        force_flush.store(true, Ordering::Relaxed);
                        let flushed = writer.flush().await;
                        force_flush.store(false, Ordering::Relaxed);
                        if let Err(e) = flushed {
                            log::warn!("Failed to send replies to peer {}: {}", peer_address, e);
                            return;
                        }
                        reader.next().await
                    }
                };
                match next {
                    Some(Ok(new_msg)) => {
                        handler.dispatch(new_msg, &mut writer).await;
                    },
                    Some(Err(e)) => {
                        log::warn!("Connection error for Peer {}: {}", peer_address, e);
                        return;
                    },
                    None => break,
                }
            }
            log::warn!("Connection closed by peer");
        });
    }

    async fn run(&self) {
        let listener = match TcpListener::bind(self.address).await {
            Ok(listener) => listener,
            Err(e) => {
                log::error!("Failed to bind receiver to {}: {}", self.address, e);
                return;
            }
        };

        log::debug!("TCP Receiver is listening on {}", self.address);
        loop {
            let (sock, peer_addr) = match listener.accept().await {
                Err(e) => {
                    log::warn!("Listener error: {}", e);
                    return;
                },
                Ok(x) => x,
            };
            log::info!("Connected to {}", peer_addr);
            Self::spawn_runner(sock, peer_addr, self.handler.clone());
        }
    }
}

/// The reply writer given to handlers. `poll_flush` writes to the socket only when
/// the receive loop sets `force_flush`; a handler's `send` (feed + flush) only
/// buffers its reply. The inner `FramedWrite` still flushes by itself when its
/// buffer is full.
struct AckBatcher<W> {
    inner: W,
    force_flush: Arc<AtomicBool>,
}

impl<W, Item> Sink<Item> for AckBatcher<W>
where
    W: Sink<Item, Error = std::io::Error> + Unpin,
{
    type Error = std::io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.force_flush.load(Ordering::Relaxed) {
            Pin::new(&mut self.inner).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

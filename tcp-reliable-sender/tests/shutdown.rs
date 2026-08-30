//! Shutdown behaviour of the per-peer connection task.
//!
//! A connection task owns the receiving half of the channel its
//! `TcpReliableSender` writes into. Once that sender is dropped the channel is
//! closed for good, so the task has nothing left to do and must end rather
//! than reconnect.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use fnv::FnvHashMap;
use tcp_reliable_sender::TcpReliableSender;
use tokio::net::{TcpListener, TcpStream};

/// Listener that counts accepted connections and holds them open, so a
/// reconnect by the sender shows up as an extra accept.
async fn spawn_counting_listener() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let counter = accepts.clone();
    tokio::spawn(async move {
        // Keeping the streams alive stops the peer from seeing EOF, so any
        // further accept really is the sender reconnecting.
        let mut held: Vec<TcpStream> = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::Relaxed);
            held.push(stream);
        }
    });
    (addr, accepts)
}

/// Dropping the sender must end the connection task, not restart it.
///
/// Regression test: the task used to treat a closed channel as a recoverable
/// keep-alive error and loop back into `TcpStream::connect`. Because the
/// backoff is reset on every successful connect, that produced an unbounded
/// reconnect storm -- tens of thousands of connections per second -- for the
/// remaining lifetime of the process.
#[tokio::test]
async fn dropping_sender_ends_connection_task() {
    let (addr, accepts) = spawn_counting_listener().await;

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, addr);
    let mut sender = TcpReliableSender::<usize, Bytes>::with_peers(peers);
    let _cancel = sender
        .send(0, Bytes::from_static(b"payload"))
        .await
        .expect("send should be accepted");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = accepts.load(Ordering::Relaxed);
    assert!(before >= 1, "sender should have connected at least once");

    drop(sender);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = accepts.load(Ordering::Relaxed);

    assert_eq!(
        after, before,
        "connection task reconnected {} time(s) after the sender was dropped; \
         the channel cannot reopen, so it should have ended instead",
        after - before
    );
}

/// A peer that is simply unreachable must still be retried, with backoff.
///
/// This guards the other side of the fix: only a closed channel ends the task.
/// Ordinary connection failures stay recoverable.
#[tokio::test]
async fn unreachable_peer_is_still_retried() {
    // Bind and immediately drop the listener to get a port nothing listens on.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, dead);
    let mut sender = TcpReliableSender::<usize, Bytes>::with_peers(peers);
    let _cancel = sender
        .send(0, Bytes::from_static(b"payload"))
        .await
        .expect("send should be accepted even when the peer is down");

    // Give the task time to fail and back off several times, then bring the
    // peer up and confirm the sender still reconnects to it.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let listener = TcpListener::bind(dead).await.expect("port should be free");
    let accepted = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await;
    assert!(
        accepted.is_ok(),
        "sender gave up on a peer that came back online"
    );
}

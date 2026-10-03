mod sender;
use bitcode::{DecodeOwned, decode};
use futures_util::StreamExt;
pub use sender::Sender;
use tokio::{select, sync::oneshot};
use tracing::{info, warn};

use std::{collections::HashMap, io::{Error, ErrorKind::{self, BrokenPipe}, Result}, sync::{Arc, atomic::AtomicU64}};

use interprocess::local_socket::{ConnectOptions, GenericNamespaced, ToNsName, tokio::RecvHalf, traits::tokio::Stream};
use tokio_util::{bytes::Buf, codec::{FramedRead, FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};

pub struct Connection<RP> {
    recv: FramedRead<RecvHalf, LengthDelimitedCodec>,
    sender: Arc<Sender<RP>>,
    pending: Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<RP>>>>>,
    cancel: CancellationToken
}

impl<RP: DecodeOwned> Connection<RP> {
    pub async fn new(name: &str) -> Result<Connection<RP>> {
        let (recv, send) = ConnectOptions::new()
            .name(name.to_ns_name::<GenericNamespaced>()?)
            .connect_tokio()
            .await?
            .split();

        let recv = FramedRead::new(recv, LengthDelimitedCodec::new());
        let send = tokio::sync::Mutex::new(FramedWrite::new(send, LengthDelimitedCodec::new()));

        let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();

        Ok(
            Connection {
                recv,
                sender: Arc::new(
                    Sender {
                        id: AtomicU64::new(0),
                        send,
                        pending: Arc::downgrade(&pending),
                        cancel: cancel.clone()
                    }
                ),
                pending,
                cancel
            }
        )
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            select! {
                read = self.recv.next() => {
                    if let Some(resolved) = read {
                        let mut frame = resolved?;

                        let id = frame.try_get_u64()?;
                        let response: Result<RP> = decode(&frame).map_err(|err| Error::new(ErrorKind::InvalidData, err));

                        if let Some(tx) = self.pending.lock().unwrap().remove(&id) {
                            _ = tx.send(response);
                        }
                        else {
                            warn!("orphaned response with id {}", id);
                        }
                    }
                    else {
                        break;
                    }
                }

                _ = self.cancel.cancelled() => {
                    info!("connection dropping...");

                    let mut pending = Arc::into_inner(self.pending).unwrap();
                    for (_, tx) in pending.get_mut().unwrap().drain() {
                        _ = tx.send(Err(Error::new(BrokenPipe, "connection has been dropped")));
                    }

                    break;
                }
            }
        }

        Ok(())
    }

    pub fn sender(&self) -> Arc<Sender<RP>> {
        self.sender.clone()
    }

    pub fn drop(&self) {
        self.cancel.cancel();
    }
}
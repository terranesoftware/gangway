use std::{collections::HashMap, io::{Error, ErrorKind, Result}, sync::{Weak, atomic::{AtomicU64, Ordering}}};

use bitcode::{Encode, encode};
use futures_util::SinkExt;
use interprocess::local_socket::tokio::SendHalf;
use tokio::{select, sync::oneshot::{self, channel}};
use tokio_util::{bytes::{BufMut, BytesMut}, codec::{FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};

pub struct Caller<RP> {
    pub(super) id: AtomicU64,
    pub(super) send: tokio::sync::Mutex<FramedWrite<SendHalf, LengthDelimitedCodec>>,
    pub(super) pending: Weak<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<RP>>>>>,
    pub(super) cancel: CancellationToken
}

impl<RP> Caller<RP> {
    pub async fn hail(&self, request: impl Encode) -> Result<RP> {
        select! {
            result = async {
                let id = self.id.fetch_add(1, Ordering::Relaxed);
                let pending = self.pending.upgrade().unwrap();

                let (tx, rx) = channel();
                _ = pending.lock().unwrap().insert(id, tx);

                let request = encode(&request);
                let mut frame = BytesMut::with_capacity(8 + request.len());
                frame.put_u64(id);
                frame.extend(request);

                if let Err(err) = self.send.lock().await.send(frame.freeze()).await {
                    _ = pending.lock().unwrap().remove(&id);

                    return Err(err);
                }

                rx.await.map_err(|err| Error::new(ErrorKind::Other, err))?
            } => {
                result
            }

            _ = self.cancel.cancelled() => {
                self.send.lock().await.flush().await?;

                Err(Error::new(ErrorKind::BrokenPipe, "caller is disembarking or has disembarked"))?
            }
        }
    }
}
mod sender;
pub use sender::Sender;
use tokio::sync::oneshot;

use std::{collections::HashMap, io::Result, sync::{Arc, atomic::AtomicU64}};

use interprocess::local_socket::{ConnectOptions, GenericNamespaced, ToNsName, tokio::RecvHalf, traits::tokio::Stream};
use tokio_util::{codec::{FramedRead, FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};

pub struct Connection<RP> {
    recv: FramedRead<RecvHalf, LengthDelimitedCodec>,
    sender: Arc<Sender<RP>>,
    pending: Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<RP>>>>>,
    cancel: CancellationToken
}

impl<RP> Connection<RP> {
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
}
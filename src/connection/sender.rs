use std::{collections::HashMap, io::Result, sync::{Weak, atomic::AtomicU64}};

use interprocess::local_socket::tokio::SendHalf;
use tokio::sync::oneshot;
use tokio_util::{codec::{FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};

pub struct Sender<RP> {
    pub(super) id: AtomicU64,
    pub(super) send: tokio::sync::Mutex<FramedWrite<SendHalf, LengthDelimitedCodec>>,
    pub(super) pending: Weak<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<RP>>>>>,
    pub(super) cancel: CancellationToken
}
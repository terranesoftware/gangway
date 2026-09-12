use bitcode::{Decode, decode};
use futures_util::StreamExt;
use interprocess::local_socket::{tokio::Stream, traits::tokio::Stream as _};
use tokio_util::{bytes::Buf, codec::{FramedRead, FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};
use tracing::error;

pub async fn handle_connection<RQ>(stream: Stream, cancel: CancellationToken)
where
    for<'a> RQ: Decode<'a>
{
    let (recv, send) = stream.split();
    let mut recv = FramedRead::new(recv, LengthDelimitedCodec::new());
    let send = FramedWrite::new(send, LengthDelimitedCodec::new());

    while let Some(resolved) = recv.next().await {
        match resolved {
            Ok(mut frame) => {
                let id = frame.try_get_u64().map_err(|err| error!("unable to get id from stream"));
                let request: Option<RQ> = decode(&frame).ok();
                let cancel = cancel.clone();
            }
            Err(err) => {}
        }
    }
}
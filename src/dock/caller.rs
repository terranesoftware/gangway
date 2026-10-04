use std::sync::Arc;

use bitcode::{Decode, Encode, decode, encode};
use futures_util::{SinkExt, StreamExt};
use interprocess::local_socket::{tokio::Stream, traits::tokio::Stream as _};
use tokio_util::{bytes::{Buf, BufMut, BytesMut}, codec::{FramedRead, FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};
use tracing::error;

/// Handles a connection by attempting to read the request and sending back over the response.
pub(super) async fn handle_caller<RQ, RP, F, Fut>(stream: Stream, cancel: CancellationToken, handler: Arc<F>)
where
    for<'a> RQ: Decode<'a>,
    RP: Encode,
    F: Fn(RQ, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RP> + Send
{
    let (recv, send) = stream.split();
    let mut recv = FramedRead::new(recv, LengthDelimitedCodec::new());
    let mut send = FramedWrite::new(send, LengthDelimitedCodec::new());

    while let Some(resolved) = recv.next().await {
        match resolved {
            Ok(mut frame) => {
                let Ok(id) = frame.try_get_u64() else {
                    error!("unable to get id from stream");
                    break;
                };
                let Ok(request) = decode(&frame) else {
                    error!("unable to decode request payload");
                    break;
                };
                let cancel = cancel.clone();

                let payload = encode::<RP>(&handler(request, cancel).await);
                let mut response = BytesMut::with_capacity(8 + payload.len());
                response.put_u64(id);
                response.extend(payload);

                if let Err(err) = send.send(response.freeze()).await {
                    error!("unable to send response with id {}: {}", id, err);
                    break;
                };
            }
            Err(err) => {
                error!("unable to read frame: {}", err);
                break;
            }
        }
    }
}
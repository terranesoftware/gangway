use bitcode::{Decode, Encode, decode, encode};
use futures_util::{SinkExt, StreamExt};
use interprocess::local_socket::{tokio::Stream, traits::tokio::Stream as _};
use tokio_util::{bytes::{Buf, Bytes}, codec::{FramedRead, FramedWrite, LengthDelimitedCodec}, sync::CancellationToken};
use tracing::error;

/// Handles a connection by attempting to read the request and sending back over the response.
pub async fn handle_connection<RQ, RP>(stream: Stream, cancel: CancellationToken, handler: fn(u64, Option<RQ>, CancellationToken) -> RP)
where
    for<'a> RQ: Decode<'a>,
    RP: Encode
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
                let request: Option<RQ> = decode(&frame).ok();
                let cancel = cancel.clone();

                let response = Bytes::from(encode::<RP>(&handler(id, request, cancel)));

                if let Err(err) = send.send(response).await {
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
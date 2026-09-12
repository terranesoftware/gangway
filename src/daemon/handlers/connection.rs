use interprocess::local_socket::tokio::Stream;
use tokio_util::sync::CancellationToken;

pub async fn handle_connection<R: Request, Encode, Decode>(stream: Stream, cancel: CancellationToken) {
    
}
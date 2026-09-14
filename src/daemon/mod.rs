pub mod connection;

use std::io::Result;

use bitcode::{Decode, Encode};
use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName, tokio::Listener, traits::tokio::Listener as _};
use tokio::{select, spawn};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::daemon::connection::handle_connection;

/// A named, asynchronous daemon.
pub struct Daemon {
    name: String,
    listener: Listener,
    cancel: CancellationToken
}

impl Daemon {
    /// Binds a `Daemon` named `name` to the given `token`.
    pub fn bind(name: String, token: &str) -> Result<(Self, CancellationToken)> {
        let cancel = CancellationToken::new();

        let listener = ListenerOptions::new()
            .name(token.to_ns_name::<GenericNamespaced>()?)
            .create_tokio()?;
        info!("{} is listening", name);

        Ok(
            (
                Daemon {
                    name,
                    listener,
                    cancel: cancel.clone()
                },
                cancel
            )
        )
    }

    /// Consumes a `Daemon` and allows it to handle connections and requests.
    pub async fn run<RQ, RP>(self, handler: fn(u64, Option<RQ>, CancellationToken) -> RP)
    where
        for<'a> RQ: Decode<'a> + 'static,
        RP: Encode + 'static
    {
        loop {
            select! {
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok(stream) => {
                            let cancel = self.cancel.clone();
                            
                            spawn(handle_connection::<RQ, RP>(stream, cancel, handler));
                        }
                        Err(err) => {
                            warn!("failed to accept connection: {}", err);
                        }
                    }
                }

                _ = self.cancel.cancelled() => {
                    info!("{} is shutting down...", self.name);
                    break;
                }
            }
        }
    }
}
mod caller;

use std::io::Result;

use bitcode::{Decode, Encode};
use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName, tokio::Listener, traits::tokio::Listener as _};
use tokio::{select, spawn};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::dock::caller::handle_caller;

/// A named, asynchronous daemon.
pub struct Dock {
    listener: Listener,
    cancel: CancellationToken
}

impl Dock {
    /// Binds a `Dock` to the given `berth`.
    pub fn bind(berth: &str) -> Result<(Self, CancellationToken)> {
        let cancel = CancellationToken::new();

        let listener = ListenerOptions::new()
            .name(berth.to_ns_name::<GenericNamespaced>()?)
            .create_tokio()?;

        Ok(
            (
                Dock {
                    listener,
                    cancel: cancel.clone()
                },
                cancel
            )
        )
    }

    /// Consumes a `Dock` and allows it to handle connections and requests.
    pub async fn open<RQ, RP>(self, handler: fn(u64, Option<RQ>, CancellationToken) -> RP)
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
                            
                            spawn(handle_caller::<RQ, RP>(stream, cancel, handler));
                        }
                        Err(err) => {
                            warn!("failed to accept caller: {}", err);
                        }
                    }
                }

                _ = self.cancel.cancelled() => {
                    info!("dock is shutting down...");
                    break;
                }
            }
        }
    }
}
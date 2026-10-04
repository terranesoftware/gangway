//! Request and response over a socket, that's it.
//! 
//! A [`Caller`] sends a request and awaits the response. `gangway` handles the boilerplate: message framing, tagging, and muxing/demuxing.
//! 
//! It is *not* an RPC framework, and will never become one. The reason for its creation is for it to be lightweight. Your request and response types only have to implement [`bitcode::Encode`] and [`bitcode::Decode`].
//! 
//! # Serving
//! 
//! ```no_run
//! # use bitcode::{Decode, Encode};
//! # use gangway::Dock;
//! # use std::io::Result;
//! # use tokio::spawn;
//! # use tokio_util::sync::CancellationToken;
//! 
//! # #[derive(Decode)]
//! # struct Request;
//! 
//! # #[derive(Encode)]
//! # struct Response;
//! 
//! # async fn first() -> Result<()> {
//! let (dock, shutdown) = Dock::bind("myberth")?;
//! 
//! spawn(dock.open(|request: Request, _cancel| async move {
//!     Response
//! }));
//!     
//! # Ok(())
//! # }
//! ```
//! 
//! # Calling
//! 
//! ```no_run
//! # use bitcode::{Decode, Encode};
//! # use gangway::Gangway;
//! # use std::io::Result;
//! # use tokio::spawn;
//! 
//! # #[derive(Encode)]
//! # struct Request;
//! 
//! # #[derive(Decode)]
//! # struct Response;
//! 
//! # async fn second() -> Result<()> {
//! let gangway = Gangway::<Request, Response>::rig("myberth").await?;
//! let caller = gangway.caller();
//! spawn(gangway.deploy());
//! 
//! // Since it's wrapped in an `Arc`, it's freely clonable.
//! let caller_two = caller.clone();
//! 
//! let response = caller.hail(Request).await?;
//! 
//! # Ok(())
//! # }
//! ```
//! 
//! Requests on a single connection are served in order. The dock awaits each handler before reading the next frame.

mod dock;
pub use dock::Dock;

mod gangway;
pub use gangway::{Caller, Gangway};
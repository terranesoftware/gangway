//! # gangway
//! 
//! **It's like playing telephone — but with one person, repeatedly.**
//! 
//! ## Serving
//! 
//! ```no_run
//! use bitcode::{Decode, Encode};
//! use gangway::Dock;
//! use std::io::Result;
//! use tokio::spawn;
//! use tokio_util::sync::CancellationToken;
//! 
//! #[derive(Decode)]
//! struct Request;
//! 
//! #[derive(Encode)]
//! struct Response;
//! 
//! async fn first() -> Result<()> {
//!     let (dock, shutdown) = Dock::bind("myberth")?;
//!         
//!     spawn(dock.open(|request: Request, _cancel| async move {
//!         Response
//!     }));
//!             
//!     Ok(())
//! }
//! ```
//! 
//! ## Connecting and Sending
//! 
//! ```no_run
//! use bitcode::{Decode, Encode};
//! use gangway::Gangway;
//! use std::io::Result;
//! use tokio::spawn;
//! 
//! #[derive(Encode)]
//! struct Request;
//! 
//! #[derive(Decode)]
//! struct Response;
//! 
//! async fn second() -> Result<()> {
//!     let gangway = Gangway::<Request, Response>::rig("myberth").await?;
//!     let caller = gangway.caller();
//!     
//!     spawn(gangway.deploy());
//!     
//!     let response = caller.hail(Request).await?;
//!     
//!     Ok(())
//! }
//! ```

mod dock;
pub use dock::Dock;

mod gangway;
pub use gangway::{Caller, Gangway};
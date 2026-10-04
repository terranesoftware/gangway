use bitcode::{Decode, Encode};
use gangway::{Dock, Gangway};
use tokio::{spawn, test};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Decode, Encode, PartialEq)]
struct Request {
    letter: char,
    sequence: Vec<i64>
}

#[derive(Debug, Decode, Encode, PartialEq)]
struct Response {
    number: usize,
    words: String
}

#[test]
async fn round_trip() {
    let (dock, shutdown) = Dock::bind("testabc").expect("Failed to bind");
    let dock = spawn(dock.open(handler));
    
    let gangway = Gangway::<Request, Response>::rig("testabc").await.expect("Failed to connect");
    let caller = gangway.caller();
    let gangway = spawn(gangway.deploy());
    
    let response = caller.hail(Request { letter: 'a', sequence: vec![5, 3, 2] }).await.unwrap();
    assert_eq!(response, Response { number: 10, words: "This is a test.".to_string() });
    
    caller.stow();
    shutdown.cancel();
    dock.await.unwrap();
    gangway.await.unwrap().unwrap();
}

async fn handler(request: Request, _cancel: CancellationToken) -> Response {
    assert_eq!(request, Request { letter: 'a', sequence: vec![5, 3, 2] });

    Response { number: 10, words: "This is a test.".to_string() }
}
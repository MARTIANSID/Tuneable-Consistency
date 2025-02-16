use tonic::{Request, Response, Status};
use raft_service::raft_client::RaftClient;
use raft_service::{ClientMessage, Transaction};
use std::error::Error;

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RaftClient::connect("http://[::1]:50052").await?;

    let transactions = vec![
        ClientMessage {
            t: Some(Transaction {
                sender: "sid".to_string(),
                receiver: "vansh".to_string(),
                amount: 200.0,
                id: "1".to_string(),
            }),
        },
        ClientMessage {
            t: Some(Transaction {
                sender: "john".to_string(),
                receiver: "alice".to_string(),
                amount: 500.0,
                id: "2".to_string(),
            }),
        },
        ClientMessage {
            t: Some(Transaction {
                sender: "mary".to_string(),
                receiver: "bob".to_string(),
                amount: 300.0,
                id: "3".to_string(),
            }),
        },
    ];

    let mut handles = vec![];

    for transaction in transactions {
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let request = Request::new(transaction);
            match client_clone.send_transaction(request).await {
                Ok(response) => {
                    println!("Transaction successful: {}", response.into_inner().data);
                }
                Err(e) => {
                    eprintln!("Error sending transaction: {}", e);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

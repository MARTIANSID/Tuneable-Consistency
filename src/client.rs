use tonic::Request;
use payments::bitcoin_client::BitcoinClient;
use payments::BtcPaymentRequest;

use raft_service::raft_client::RaftClient;
use raft_service::ClientMessage;
use raft_service::Transaction;

pub mod payments {
    tonic::include_proto!("payments");
}

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RaftClient::connect("http://[::1]:50051").await?;

    let request = Request::new(ClientMessage {
        t: Some(Transaction {
            sender: "sid".to_string(),
            receiver: "vansh".to_string(),
            amount: 200.0,
            id: "1".to_string(),
        }),
    });

    let response = client.send_transaction(request).await?;
    Ok(())
}
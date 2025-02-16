use tonic::{transport::Server, Request, Response, Status};

use payments::bitcoin_server::{Bitcoin, BitcoinServer};
use payments::{BtcPaymentResponse, BtcPaymentRequest};


use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};

pub mod payments {
    tonic::include_proto!("payments");
}

pub mod raft_service {
    tonic::include_proto!("raft_service");
}


#[derive(Debug, Default)]
pub struct BitcoinService {}

#[derive(Debug, Default)]
pub struct RaftService {}

#[tonic::async_trait]
impl Bitcoin for BitcoinService {
    async fn send_payment(
        &self,
        request: Request<BtcPaymentRequest>,
    ) -> Result<Response<BtcPaymentResponse>, Status> {
        println!("Got a request: {:?}", request);

        let req = request.into_inner();

        let reply = BtcPaymentResponse {
            successful: true,
            message: format!("Sent {}BTC to {}.", req.amount, req.to_addr).into(),
        };
        Ok(Response::new(reply))
    }
}
#[tonic::async_trait]
impl Raft for RaftService {
    async fn append_entries(&self, request: Request<AppendEntriesArgument>) -> Result<Response<AppendEntriesResult>, Status> {
        todo!()
    }

    async fn request_vote(&self, request: Request<RequestVoteArguments>) -> Result<Response<RequestVoteResult>, Status> {
        todo!()
    }
    async fn send_transaction(&self, request: Request<ClientMessage>) -> Result<Response<Empty>, Status> {
        println!("Got a request: {:?}", request);
        Ok(Response::new(Empty {
            data: 2
        })).expect("TODO: panic message");

        todo!()
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft_service = RaftService::default();
    Server::builder().add_service(RaftServer::new(raft_service)).serve(addr).await?;
    Ok(())
}
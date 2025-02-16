use tonic::{transport::Server, Request, Response, Status};



use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};



pub mod raft_service {
    tonic::include_proto!("raft_service");
}



#[derive(Debug, Default)]
pub struct RaftService {}

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
        let result = Empty {
            data : 2
        };

        Ok(Response::new(result))

    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft_service = RaftService::default();
    Server::builder().add_service(RaftServer::new(raft_service)).serve(addr).await?;
    Ok(())
}
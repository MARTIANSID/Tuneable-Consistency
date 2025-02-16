use std::sync::{Arc, Mutex};
// imports
use tonic::{transport::Server, Request, Response, Status};
use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};
use utilities::log;
use crate::utilities::log::Log;

// modules
mod utilities;



pub mod raft_service {
    tonic::include_proto!("raft_service");
}
// #[derive(Debug, Default)]

pub struct RaftService {
    current_term : i32,
    voted_for: Option<i32>,
    log :  Arc<Mutex<Vec<Log>>>,
    commit_index : i32,
    last_applied: i32,
}

impl RaftService {
    pub fn new() -> Self {
        RaftService {
            current_term: 0,
            voted_for: None,
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index : -1,
            last_applied: 0,
        }
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
        let result = Empty {
            data : 2
        };

        Ok(Response::new(result))

    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft_service = RaftService::new();
    Server::builder().add_service(RaftServer::new(raft_service)).serve(addr).await?;
    Ok(())
}
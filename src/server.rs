// imports
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};
use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};
use crate::utilities::log::Log;

// modules
mod utilities;


pub mod raft_service {
    tonic::include_proto!("raft_service");
}
// #[derive(Debug, Default)]
enum StatusOfServer {
    LEADER,
    CANDIDATE,
    FOLLOWER,
}
pub struct RaftService {
    current_term : i32,
    voted_for: Option<i32>,
    log :  Arc<Mutex<Vec<Log>>>,
    commit_index : i32,
    last_applied: i32,
    status_of_server: StatusOfServer,
    next_index: Arc<Mutex<[i32; 5]>>,
    match_index: Arc<Mutex<[i32; 5]>>,
}

impl RaftService {
    pub fn new() -> Self {
        RaftService {
            current_term: 0,
            voted_for: None,
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index : -1,
            last_applied: -1,
            status_of_server : StatusOfServer::FOLLOWER,
            next_index : Arc::new(Mutex::new([0,0,0,0,0])),
            match_index: Arc::new(Mutex::new([0,0,0,0,0]))
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

// for now fixing the number of threads
#[tokio::main]
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // starting 5 servers

    let addresses = ["[::1]:50051", "[::1]:50052", "[::1]:50053","[::1]:50054","[::1]:50055"];
    let mut servers: Vec<JoinHandle<()>> = Vec::new();

    for addr in addresses {
        let addr = addr.parse()?;
        let raft_service = RaftService::new();
        servers.push(tokio::spawn(async move {
            println!("Server listening on {:?}", addr);
            Server::builder()
                .add_service(RaftServer::new(raft_service))
                .serve(addr)
                .await
                .unwrap();
        }));
    }
    for handle in servers {
        handle.await.unwrap();
    }

    Ok(())
}
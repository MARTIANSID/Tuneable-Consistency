use std::error::Error;
// imports
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};
use tonic::transport::Channel;
use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};
use crate::raft_service::raft_client::RaftClient;
use crate::utilities::log::Log;
use crate::utilities::statusofserver::StatusOfServer;

// modules
mod utilities;


pub mod raft_service {
    tonic::include_proto!("raft_service");
}
// #[derive(Debug, Default)]
pub struct RaftService {
    current_term : Arc<Mutex<i32>>,
    voted_for: Arc<RwLock<Option<i32>>>,
    log :  Arc<Mutex<Vec<Log>>>,
    commit_index : Arc<Mutex<i32>>,
    last_applied: Arc<Mutex<i32>>,
    status_of_server: Arc<Mutex<StatusOfServer>>,
    next_index: Arc<Mutex<[i32; 5]>>,
    match_index: Arc<Mutex<[i32; 5]>>,
    peers : Vec<RaftClient<Channel>>,
    server_id : i32
}

impl RaftService {
    pub fn new(server_id : i32) -> Self {
        // creating peers vector
        RaftService {
            current_term:Arc::new(Mutex::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index : Arc::new(Mutex::new(0)),
            last_applied: Arc::new(Mutex::new(0)),
            status_of_server : Arc::new(Mutex::new(StatusOfServer::FOLLOWER)),
            next_index : Arc::new(Mutex::new([0,0,0,0,0])),
            match_index: Arc::new(Mutex::new([0,0,0,0,0])),
            peers,
            server_id
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
// #[tokio::main]
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // starting 5 servers

    let addresses = ["[::1]:50051", "[::1]:50052", "[::1]:50053","[::1]:50054","[::1]:50055"];
    let mut servers: Vec<JoinHandle<()>> = Vec::new();

    let mut server_id : i32 = 0;

    for addr in addresses {
        let addr = addr.parse()?;
        let raft_service = RaftService::new(server_id);
        servers.push(tokio::spawn(async move {
            println!("Server listening on {:?}", addr);
            Server::builder()
                .add_service(RaftServer::new(raft_service))
                .serve(addr)
                .await
                .unwrap();
        }));
        server_id += 1;
    }
    for handle in servers {
        handle.await.unwrap();
    }
    Ok(())
}
use std::collections::HashMap;
use std::error::Error;
// use tokio::sync::mpsc;
// imports
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use tokio::time::sleep;
use std::time::Duration;
use prost::Message;
use tokio::task;
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};
use tonic::transport::Channel;
use raft_service::raft_server::{RaftServer, Raft};
use crate::raft_service::{AppendEntriesArgument, AppendEntriesResult, ClientMessage, RequestVoteArguments, RequestVoteResult, Empty};
use crate::raft_service::raft_client::RaftClient;
use crate::utilities::log::Log;
use crate::utilities::statusofserver::StatusOfServer;
use utilities::timer::Timer;
use tokio::sync::mpsc::{self, Sender, Receiver};

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
    peers : HashMap<String, RaftClient<Channel>>,
    server_id : i32,
    test : Arc<Mutex<i32>>,
    addresses : [&'static str; 5],
    election_timer : Option<Timer>,
    election_timeout_tx: Sender<()>

}


impl RaftService {
    pub fn start_election(&self) {
        let mut status = self.status_of_server.lock().unwrap();
        if *status == StatusOfServer::FOLLOWER {
            *status = StatusOfServer::CANDIDATE;
            println!("Server {} is now a candidate!", self.server_id);
            // More election logic will go here
        }
    }

    pub fn start_election_timer(&mut self, mut election_timeout_rx: mpsc::Receiver<()>) {
        let (election_timeout_tx, status_of_server, server_id) = (
            self.election_timeout_tx.clone(),
            Arc::clone(&self.status_of_server),
            self.server_id,
        );

        // Create a random election timeout duration between 3 to 5 seconds
        let timeout_duration = Duration::from_secs(3 );

        let mut timer = Timer::new(timeout_duration, election_timeout_tx);
        timer.start();

        self.election_timer = Some(timer);
        tokio::spawn(async move {
            loop {
                // Wait for the timeout signal using the receiver
                let _ = election_timeout_rx.recv().await;

                let mut status = status_of_server.lock().unwrap();
                if *status == StatusOfServer::FOLLOWER {
                    println!("Election timeout! Server {} starting election...", server_id);
                    *status = StatusOfServer::CANDIDATE;
                }
            }
        });
    }

    pub fn new(server_id : i32) -> Self {
        // creating peers vector
        let (election_timeout_tx, election_timeout_rx) = mpsc::channel(1); // Create a channel for timeouts

        let mut service = RaftService {
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index: Arc::new(Mutex::new(0)),
            last_applied: Arc::new(Mutex::new(0)),
            status_of_server: Arc::new(Mutex::new(StatusOfServer::FOLLOWER)),
            next_index: Arc::new(Mutex::new([0, 0, 0, 0, 0])),
            match_index: Arc::new(Mutex::new([0, 0, 0, 0, 0])),
            peers: HashMap::new(),
            server_id,
            test: Arc::new(Mutex::new(0)),
            addresses: ["[::1]:50051", "[::1]:50052", "[::1]:50053", "[::1]:50054", "[::1]:50055"],
            election_timer: None,
            election_timeout_tx,
        };

        service.start_election_timer(election_timeout_rx);
        service
    
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
        let mut test = self.test.lock().unwrap();
        *test += 1;
        let result = Empty {
            data : *test
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

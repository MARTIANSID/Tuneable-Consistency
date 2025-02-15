#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientMessage {
    #[prost(message, optional, tag="1")]
    pub t: ::core::option::Option<Transaction>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(string, tag="1")]
    pub sender: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub receiver: ::prost::alloc::string::String,
    #[prost(double, tag="3")]
    pub amount: f64,
    #[prost(string, tag="4")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogEntry {
    #[prost(int32, tag="1")]
    pub log_index: i32,
    #[prost(int32, tag="2")]
    pub term: i32,
    #[prost(message, optional, tag="3")]
    pub t: ::core::option::Option<Transaction>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    #[prost(message, repeated, tag="1")]
    pub log: ::prost::alloc::vec::Vec<LogEntry>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesArgument {
    #[prost(int32, tag="1")]
    pub leaders_term: i32,
    #[prost(int32, tag="2")]
    pub prev_log_index: i32,
    #[prost(int32, tag="3")]
    pub leaders_id: i32,
    #[prost(int32, tag="4")]
    pub prev_log_term: i32,
    #[prost(message, optional, tag="5")]
    pub entries_to_append: ::core::option::Option<Log>,
    #[prost(int32, tag="6")]
    pub leaders_commit: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteArguments {
    #[prost(int32, tag="1")]
    pub candidates_term: i32,
    #[prost(int32, tag="2")]
    pub candidate_id: i32,
    #[prost(int32, tag="3")]
    pub last_log_index: i32,
    #[prost(int32, tag="4")]
    pub last_log_term: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesResult {
    #[prost(int32, tag="1")]
    pub current_term: i32,
    #[prost(bool, tag="2")]
    pub is_success_full: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteResult {
    #[prost(int32, tag="1")]
    pub current_term: i32,
    #[prost(bool, tag="2")]
    pub is_vote_granted: bool,
}
/// Generated client implementations.
pub mod raft_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RaftClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            RaftClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn send_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::ClientMessage>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/RaftService.Raft/SendTransaction",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::AppendEntriesArgument>,
        ) -> Result<tonic::Response<super::AppendEntriesResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/RaftService.Raft/AppendEntries",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn request_vote(
            &mut self,
            request: impl tonic::IntoRequest<super::RequestVoteArguments>,
        ) -> Result<tonic::Response<super::RequestVoteResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/RaftService.Raft/RequestVote",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod raft_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with RaftServer.
    #[async_trait]
    pub trait Raft: Send + Sync + 'static {
        async fn send_transaction(
            &self,
            request: tonic::Request<super::ClientMessage>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
        async fn append_entries(
            &self,
            request: tonic::Request<super::AppendEntriesArgument>,
        ) -> Result<tonic::Response<super::AppendEntriesResult>, tonic::Status>;
        async fn request_vote(
            &self,
            request: tonic::Request<super::RequestVoteArguments>,
        ) -> Result<tonic::Response<super::RequestVoteResult>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RaftServer<T: Raft> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Raft> RaftServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftServer<T>
    where
        T: Raft,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/RaftService.Raft/SendTransaction" => {
                    #[allow(non_camel_case_types)]
                    struct SendTransactionSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::ClientMessage>
                    for SendTransactionSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClientMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).send_transaction(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SendTransactionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/RaftService.Raft/AppendEntries" => {
                    #[allow(non_camel_case_types)]
                    struct AppendEntriesSvc<T: Raft>(pub Arc<T>);
                    impl<
                        T: Raft,
                    > tonic::server::UnaryService<super::AppendEntriesArgument>
                    for AppendEntriesSvc<T> {
                        type Response = super::AppendEntriesResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AppendEntriesArgument>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).append_entries(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/RaftService.Raft/RequestVote" => {
                    #[allow(non_camel_case_types)]
                    struct RequestVoteSvc<T: Raft>(pub Arc<T>);
                    impl<
                        T: Raft,
                    > tonic::server::UnaryService<super::RequestVoteArguments>
                    for RequestVoteSvc<T> {
                        type Response = super::RequestVoteResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RequestVoteArguments>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).request_vote(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RequestVoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Raft> Clone for RaftServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Raft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Raft> tonic::transport::NamedService for RaftServer<T> {
        const NAME: &'static str = "RaftService.Raft";
    }
}

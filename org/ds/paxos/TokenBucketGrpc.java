package org.ds.paxos;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.72.0)",
    comments = "Source: src/main/resources/Raft.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TokenBucketGrpc {

  private TokenBucketGrpc() {}

  public static final java.lang.String SERVICE_NAME = "org.ds.paxos.TokenBucket";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.TokenStatusRequest,
      org.ds.paxos.TokenStatusResponse> getGetTokenStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTokenStatus",
      requestType = org.ds.paxos.TokenStatusRequest.class,
      responseType = org.ds.paxos.TokenStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.TokenStatusRequest,
      org.ds.paxos.TokenStatusResponse> getGetTokenStatusMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.TokenStatusRequest, org.ds.paxos.TokenStatusResponse> getGetTokenStatusMethod;
    if ((getGetTokenStatusMethod = TokenBucketGrpc.getGetTokenStatusMethod) == null) {
      synchronized (TokenBucketGrpc.class) {
        if ((getGetTokenStatusMethod = TokenBucketGrpc.getGetTokenStatusMethod) == null) {
          TokenBucketGrpc.getGetTokenStatusMethod = getGetTokenStatusMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.TokenStatusRequest, org.ds.paxos.TokenStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTokenStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.TokenStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.TokenStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TokenBucketMethodDescriptorSupplier("GetTokenStatus"))
              .build();
        }
      }
    }
    return getGetTokenStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.TokenUpdateRequest,
      org.ds.paxos.Empty> getUpdateTokenStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateTokenState",
      requestType = org.ds.paxos.TokenUpdateRequest.class,
      responseType = org.ds.paxos.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.TokenUpdateRequest,
      org.ds.paxos.Empty> getUpdateTokenStateMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.TokenUpdateRequest, org.ds.paxos.Empty> getUpdateTokenStateMethod;
    if ((getUpdateTokenStateMethod = TokenBucketGrpc.getUpdateTokenStateMethod) == null) {
      synchronized (TokenBucketGrpc.class) {
        if ((getUpdateTokenStateMethod = TokenBucketGrpc.getUpdateTokenStateMethod) == null) {
          TokenBucketGrpc.getUpdateTokenStateMethod = getUpdateTokenStateMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.TokenUpdateRequest, org.ds.paxos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateTokenState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.TokenUpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new TokenBucketMethodDescriptorSupplier("UpdateTokenState"))
              .build();
        }
      }
    }
    return getUpdateTokenStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.TokenConsumeRequest,
      org.ds.paxos.TokenConsumeResponse> getConsumeTokensMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ConsumeTokens",
      requestType = org.ds.paxos.TokenConsumeRequest.class,
      responseType = org.ds.paxos.TokenConsumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.TokenConsumeRequest,
      org.ds.paxos.TokenConsumeResponse> getConsumeTokensMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.TokenConsumeRequest, org.ds.paxos.TokenConsumeResponse> getConsumeTokensMethod;
    if ((getConsumeTokensMethod = TokenBucketGrpc.getConsumeTokensMethod) == null) {
      synchronized (TokenBucketGrpc.class) {
        if ((getConsumeTokensMethod = TokenBucketGrpc.getConsumeTokensMethod) == null) {
          TokenBucketGrpc.getConsumeTokensMethod = getConsumeTokensMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.TokenConsumeRequest, org.ds.paxos.TokenConsumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ConsumeTokens"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.TokenConsumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.TokenConsumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TokenBucketMethodDescriptorSupplier("ConsumeTokens"))
              .build();
        }
      }
    }
    return getConsumeTokensMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TokenBucketStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TokenBucketStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TokenBucketStub>() {
        @java.lang.Override
        public TokenBucketStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TokenBucketStub(channel, callOptions);
        }
      };
    return TokenBucketStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static TokenBucketBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TokenBucketBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TokenBucketBlockingV2Stub>() {
        @java.lang.Override
        public TokenBucketBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TokenBucketBlockingV2Stub(channel, callOptions);
        }
      };
    return TokenBucketBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TokenBucketBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TokenBucketBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TokenBucketBlockingStub>() {
        @java.lang.Override
        public TokenBucketBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TokenBucketBlockingStub(channel, callOptions);
        }
      };
    return TokenBucketBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TokenBucketFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TokenBucketFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TokenBucketFutureStub>() {
        @java.lang.Override
        public TokenBucketFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TokenBucketFutureStub(channel, callOptions);
        }
      };
    return TokenBucketFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getTokenStatus(org.ds.paxos.TokenStatusRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.TokenStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTokenStatusMethod(), responseObserver);
    }

    /**
     */
    default void updateTokenState(org.ds.paxos.TokenUpdateRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateTokenStateMethod(), responseObserver);
    }

    /**
     */
    default void consumeTokens(org.ds.paxos.TokenConsumeRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.TokenConsumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getConsumeTokensMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TokenBucket.
   */
  public static abstract class TokenBucketImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TokenBucketGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TokenBucket.
   */
  public static final class TokenBucketStub
      extends io.grpc.stub.AbstractAsyncStub<TokenBucketStub> {
    private TokenBucketStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TokenBucketStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TokenBucketStub(channel, callOptions);
    }

    /**
     */
    public void getTokenStatus(org.ds.paxos.TokenStatusRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.TokenStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTokenStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateTokenState(org.ds.paxos.TokenUpdateRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateTokenStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void consumeTokens(org.ds.paxos.TokenConsumeRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.TokenConsumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getConsumeTokensMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TokenBucket.
   */
  public static final class TokenBucketBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<TokenBucketBlockingV2Stub> {
    private TokenBucketBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TokenBucketBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TokenBucketBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public org.ds.paxos.TokenStatusResponse getTokenStatus(org.ds.paxos.TokenStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTokenStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty updateTokenState(org.ds.paxos.TokenUpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateTokenStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.TokenConsumeResponse consumeTokens(org.ds.paxos.TokenConsumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConsumeTokensMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service TokenBucket.
   */
  public static final class TokenBucketBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TokenBucketBlockingStub> {
    private TokenBucketBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TokenBucketBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TokenBucketBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.ds.paxos.TokenStatusResponse getTokenStatus(org.ds.paxos.TokenStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTokenStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty updateTokenState(org.ds.paxos.TokenUpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateTokenStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.TokenConsumeResponse consumeTokens(org.ds.paxos.TokenConsumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConsumeTokensMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TokenBucket.
   */
  public static final class TokenBucketFutureStub
      extends io.grpc.stub.AbstractFutureStub<TokenBucketFutureStub> {
    private TokenBucketFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TokenBucketFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TokenBucketFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.TokenStatusResponse> getTokenStatus(
        org.ds.paxos.TokenStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTokenStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.Empty> updateTokenState(
        org.ds.paxos.TokenUpdateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateTokenStateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.TokenConsumeResponse> consumeTokens(
        org.ds.paxos.TokenConsumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getConsumeTokensMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_TOKEN_STATUS = 0;
  private static final int METHODID_UPDATE_TOKEN_STATE = 1;
  private static final int METHODID_CONSUME_TOKENS = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_TOKEN_STATUS:
          serviceImpl.getTokenStatus((org.ds.paxos.TokenStatusRequest) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.TokenStatusResponse>) responseObserver);
          break;
        case METHODID_UPDATE_TOKEN_STATE:
          serviceImpl.updateTokenState((org.ds.paxos.TokenUpdateRequest) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.Empty>) responseObserver);
          break;
        case METHODID_CONSUME_TOKENS:
          serviceImpl.consumeTokens((org.ds.paxos.TokenConsumeRequest) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.TokenConsumeResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetTokenStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.TokenStatusRequest,
              org.ds.paxos.TokenStatusResponse>(
                service, METHODID_GET_TOKEN_STATUS)))
        .addMethod(
          getUpdateTokenStateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.TokenUpdateRequest,
              org.ds.paxos.Empty>(
                service, METHODID_UPDATE_TOKEN_STATE)))
        .addMethod(
          getConsumeTokensMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.TokenConsumeRequest,
              org.ds.paxos.TokenConsumeResponse>(
                service, METHODID_CONSUME_TOKENS)))
        .build();
  }

  private static abstract class TokenBucketBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TokenBucketBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.ds.paxos.RaftOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TokenBucket");
    }
  }

  private static final class TokenBucketFileDescriptorSupplier
      extends TokenBucketBaseDescriptorSupplier {
    TokenBucketFileDescriptorSupplier() {}
  }

  private static final class TokenBucketMethodDescriptorSupplier
      extends TokenBucketBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TokenBucketMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (TokenBucketGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TokenBucketFileDescriptorSupplier())
              .addMethod(getGetTokenStatusMethod())
              .addMethod(getUpdateTokenStateMethod())
              .addMethod(getConsumeTokensMethod())
              .build();
        }
      }
    }
    return result;
  }
}

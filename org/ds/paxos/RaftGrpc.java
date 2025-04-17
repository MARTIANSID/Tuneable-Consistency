package org.ds.paxos;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.72.0)",
    comments = "Source: src/main/resources/Raft.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RaftGrpc {

  private RaftGrpc() {}

  public static final java.lang.String SERVICE_NAME = "org.ds.paxos.Raft";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.ClientMessage,
      org.ds.paxos.Empty> getSendTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendTransaction",
      requestType = org.ds.paxos.ClientMessage.class,
      responseType = org.ds.paxos.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.ClientMessage,
      org.ds.paxos.Empty> getSendTransactionMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.ClientMessage, org.ds.paxos.Empty> getSendTransactionMethod;
    if ((getSendTransactionMethod = RaftGrpc.getSendTransactionMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getSendTransactionMethod = RaftGrpc.getSendTransactionMethod) == null) {
          RaftGrpc.getSendTransactionMethod = getSendTransactionMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.ClientMessage, org.ds.paxos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.ClientMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("SendTransaction"))
              .build();
        }
      }
    }
    return getSendTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.AppendEntriesArgument,
      org.ds.paxos.AppendEntriesResult> getAppendEntriesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AppendEntries",
      requestType = org.ds.paxos.AppendEntriesArgument.class,
      responseType = org.ds.paxos.AppendEntriesResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.AppendEntriesArgument,
      org.ds.paxos.AppendEntriesResult> getAppendEntriesMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.AppendEntriesArgument, org.ds.paxos.AppendEntriesResult> getAppendEntriesMethod;
    if ((getAppendEntriesMethod = RaftGrpc.getAppendEntriesMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getAppendEntriesMethod = RaftGrpc.getAppendEntriesMethod) == null) {
          RaftGrpc.getAppendEntriesMethod = getAppendEntriesMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.AppendEntriesArgument, org.ds.paxos.AppendEntriesResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendEntries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.AppendEntriesArgument.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.AppendEntriesResult.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("AppendEntries"))
              .build();
        }
      }
    }
    return getAppendEntriesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.RequestVoteArguments,
      org.ds.paxos.RequestVoteResult> getRequestVoteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RequestVote",
      requestType = org.ds.paxos.RequestVoteArguments.class,
      responseType = org.ds.paxos.RequestVoteResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.RequestVoteArguments,
      org.ds.paxos.RequestVoteResult> getRequestVoteMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.RequestVoteArguments, org.ds.paxos.RequestVoteResult> getRequestVoteMethod;
    if ((getRequestVoteMethod = RaftGrpc.getRequestVoteMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getRequestVoteMethod = RaftGrpc.getRequestVoteMethod) == null) {
          RaftGrpc.getRequestVoteMethod = getRequestVoteMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.RequestVoteArguments, org.ds.paxos.RequestVoteResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RequestVote"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.RequestVoteArguments.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.RequestVoteResult.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("RequestVote"))
              .build();
        }
      }
    }
    return getRequestVoteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.Empty,
      org.ds.paxos.Empty> getPrintLogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "printLog",
      requestType = org.ds.paxos.Empty.class,
      responseType = org.ds.paxos.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.Empty,
      org.ds.paxos.Empty> getPrintLogMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.Empty, org.ds.paxos.Empty> getPrintLogMethod;
    if ((getPrintLogMethod = RaftGrpc.getPrintLogMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getPrintLogMethod = RaftGrpc.getPrintLogMethod) == null) {
          RaftGrpc.getPrintLogMethod = getPrintLogMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.Empty, org.ds.paxos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "printLog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("printLog"))
              .build();
        }
      }
    }
    return getPrintLogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.Ack,
      org.ds.paxos.Empty> getSendAckToClientMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "sendAckToClient",
      requestType = org.ds.paxos.Ack.class,
      responseType = org.ds.paxos.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.Ack,
      org.ds.paxos.Empty> getSendAckToClientMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.Ack, org.ds.paxos.Empty> getSendAckToClientMethod;
    if ((getSendAckToClientMethod = RaftGrpc.getSendAckToClientMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getSendAckToClientMethod = RaftGrpc.getSendAckToClientMethod) == null) {
          RaftGrpc.getSendAckToClientMethod = getSendAckToClientMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.Ack, org.ds.paxos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "sendAckToClient"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Ack.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("sendAckToClient"))
              .build();
        }
      }
    }
    return getSendAckToClientMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.ds.paxos.ReadRequest,
      org.ds.paxos.Balance> getSendReadRequestMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "sendReadRequest",
      requestType = org.ds.paxos.ReadRequest.class,
      responseType = org.ds.paxos.Balance.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.ds.paxos.ReadRequest,
      org.ds.paxos.Balance> getSendReadRequestMethod() {
    io.grpc.MethodDescriptor<org.ds.paxos.ReadRequest, org.ds.paxos.Balance> getSendReadRequestMethod;
    if ((getSendReadRequestMethod = RaftGrpc.getSendReadRequestMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getSendReadRequestMethod = RaftGrpc.getSendReadRequestMethod) == null) {
          RaftGrpc.getSendReadRequestMethod = getSendReadRequestMethod =
              io.grpc.MethodDescriptor.<org.ds.paxos.ReadRequest, org.ds.paxos.Balance>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "sendReadRequest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.ds.paxos.Balance.getDefaultInstance()))
              .setSchemaDescriptor(new RaftMethodDescriptorSupplier("sendReadRequest"))
              .build();
        }
      }
    }
    return getSendReadRequestMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftStub>() {
        @java.lang.Override
        public RaftStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftStub(channel, callOptions);
        }
      };
    return RaftStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static RaftBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftBlockingV2Stub>() {
        @java.lang.Override
        public RaftBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftBlockingV2Stub(channel, callOptions);
        }
      };
    return RaftBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftBlockingStub>() {
        @java.lang.Override
        public RaftBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftBlockingStub(channel, callOptions);
        }
      };
    return RaftBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftFutureStub>() {
        @java.lang.Override
        public RaftFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftFutureStub(channel, callOptions);
        }
      };
    return RaftFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void sendTransaction(org.ds.paxos.ClientMessage request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendTransactionMethod(), responseObserver);
    }

    /**
     */
    default void appendEntries(org.ds.paxos.AppendEntriesArgument request,
        io.grpc.stub.StreamObserver<org.ds.paxos.AppendEntriesResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAppendEntriesMethod(), responseObserver);
    }

    /**
     */
    default void requestVote(org.ds.paxos.RequestVoteArguments request,
        io.grpc.stub.StreamObserver<org.ds.paxos.RequestVoteResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRequestVoteMethod(), responseObserver);
    }

    /**
     */
    default void printLog(org.ds.paxos.Empty request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintLogMethod(), responseObserver);
    }

    /**
     */
    default void sendAckToClient(org.ds.paxos.Ack request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendAckToClientMethod(), responseObserver);
    }

    /**
     */
    default void sendReadRequest(org.ds.paxos.ReadRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Balance> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendReadRequestMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Raft.
   */
  public static abstract class RaftImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RaftGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Raft.
   */
  public static final class RaftStub
      extends io.grpc.stub.AbstractAsyncStub<RaftStub> {
    private RaftStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftStub(channel, callOptions);
    }

    /**
     */
    public void sendTransaction(org.ds.paxos.ClientMessage request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void appendEntries(org.ds.paxos.AppendEntriesArgument request,
        io.grpc.stub.StreamObserver<org.ds.paxos.AppendEntriesResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void requestVote(org.ds.paxos.RequestVoteArguments request,
        io.grpc.stub.StreamObserver<org.ds.paxos.RequestVoteResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void printLog(org.ds.paxos.Empty request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintLogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendAckToClient(org.ds.paxos.Ack request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendAckToClientMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendReadRequest(org.ds.paxos.ReadRequest request,
        io.grpc.stub.StreamObserver<org.ds.paxos.Balance> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendReadRequestMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Raft.
   */
  public static final class RaftBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<RaftBlockingV2Stub> {
    private RaftBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public org.ds.paxos.Empty sendTransaction(org.ds.paxos.ClientMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendTransactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.AppendEntriesResult appendEntries(org.ds.paxos.AppendEntriesArgument request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAppendEntriesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.RequestVoteResult requestVote(org.ds.paxos.RequestVoteArguments request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRequestVoteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty printLog(org.ds.paxos.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintLogMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty sendAckToClient(org.ds.paxos.Ack request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendAckToClientMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Balance sendReadRequest(org.ds.paxos.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendReadRequestMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service Raft.
   */
  public static final class RaftBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RaftBlockingStub> {
    private RaftBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.ds.paxos.Empty sendTransaction(org.ds.paxos.ClientMessage request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendTransactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.AppendEntriesResult appendEntries(org.ds.paxos.AppendEntriesArgument request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAppendEntriesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.RequestVoteResult requestVote(org.ds.paxos.RequestVoteArguments request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRequestVoteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty printLog(org.ds.paxos.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintLogMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Empty sendAckToClient(org.ds.paxos.Ack request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendAckToClientMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.ds.paxos.Balance sendReadRequest(org.ds.paxos.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendReadRequestMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Raft.
   */
  public static final class RaftFutureStub
      extends io.grpc.stub.AbstractFutureStub<RaftFutureStub> {
    private RaftFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.Empty> sendTransaction(
        org.ds.paxos.ClientMessage request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendTransactionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.AppendEntriesResult> appendEntries(
        org.ds.paxos.AppendEntriesArgument request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.RequestVoteResult> requestVote(
        org.ds.paxos.RequestVoteArguments request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.Empty> printLog(
        org.ds.paxos.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintLogMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.Empty> sendAckToClient(
        org.ds.paxos.Ack request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendAckToClientMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.ds.paxos.Balance> sendReadRequest(
        org.ds.paxos.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendReadRequestMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_TRANSACTION = 0;
  private static final int METHODID_APPEND_ENTRIES = 1;
  private static final int METHODID_REQUEST_VOTE = 2;
  private static final int METHODID_PRINT_LOG = 3;
  private static final int METHODID_SEND_ACK_TO_CLIENT = 4;
  private static final int METHODID_SEND_READ_REQUEST = 5;

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
        case METHODID_SEND_TRANSACTION:
          serviceImpl.sendTransaction((org.ds.paxos.ClientMessage) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.Empty>) responseObserver);
          break;
        case METHODID_APPEND_ENTRIES:
          serviceImpl.appendEntries((org.ds.paxos.AppendEntriesArgument) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.AppendEntriesResult>) responseObserver);
          break;
        case METHODID_REQUEST_VOTE:
          serviceImpl.requestVote((org.ds.paxos.RequestVoteArguments) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.RequestVoteResult>) responseObserver);
          break;
        case METHODID_PRINT_LOG:
          serviceImpl.printLog((org.ds.paxos.Empty) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.Empty>) responseObserver);
          break;
        case METHODID_SEND_ACK_TO_CLIENT:
          serviceImpl.sendAckToClient((org.ds.paxos.Ack) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.Empty>) responseObserver);
          break;
        case METHODID_SEND_READ_REQUEST:
          serviceImpl.sendReadRequest((org.ds.paxos.ReadRequest) request,
              (io.grpc.stub.StreamObserver<org.ds.paxos.Balance>) responseObserver);
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
          getSendTransactionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.ClientMessage,
              org.ds.paxos.Empty>(
                service, METHODID_SEND_TRANSACTION)))
        .addMethod(
          getAppendEntriesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.AppendEntriesArgument,
              org.ds.paxos.AppendEntriesResult>(
                service, METHODID_APPEND_ENTRIES)))
        .addMethod(
          getRequestVoteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.RequestVoteArguments,
              org.ds.paxos.RequestVoteResult>(
                service, METHODID_REQUEST_VOTE)))
        .addMethod(
          getPrintLogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.Empty,
              org.ds.paxos.Empty>(
                service, METHODID_PRINT_LOG)))
        .addMethod(
          getSendAckToClientMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.Ack,
              org.ds.paxos.Empty>(
                service, METHODID_SEND_ACK_TO_CLIENT)))
        .addMethod(
          getSendReadRequestMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.ds.paxos.ReadRequest,
              org.ds.paxos.Balance>(
                service, METHODID_SEND_READ_REQUEST)))
        .build();
  }

  private static abstract class RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.ds.paxos.RaftOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Raft");
    }
  }

  private static final class RaftFileDescriptorSupplier
      extends RaftBaseDescriptorSupplier {
    RaftFileDescriptorSupplier() {}
  }

  private static final class RaftMethodDescriptorSupplier
      extends RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RaftMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (RaftGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftFileDescriptorSupplier())
              .addMethod(getSendTransactionMethod())
              .addMethod(getAppendEntriesMethod())
              .addMethod(getRequestVoteMethod())
              .addMethod(getPrintLogMethod())
              .addMethod(getSendAckToClientMethod())
              .addMethod(getSendReadRequestMethod())
              .build();
        }
      }
    }
    return result;
  }
}

package org.example.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.example.Transport.KvWireProtocol;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;

import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;

/** Shared persistent framed transport for every logical client session. */
public final class KvFramedTransport implements AutoCloseable {

    public record RpcFailure(Status status, long serverReplyEpochMs) {
    }

    public interface RequestHandle {
        void cancel();
    }

    private final EventLoopGroup ioGroup;
    private final ExecutorService callbackExecutor;
    private final Connection[][] connections;
    private final AtomicInteger[] nextConnection;
    private final AtomicLong correlationIds = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Channel> allChannels = new ArrayList<>();

    public KvFramedTransport(List<String> hosts, int clientBasePort, int connectionsPerServer) {
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("hosts must not be empty");
        }
        if (connectionsPerServer <= 0) {
            throw new IllegalArgumentException("connectionsPerServer must be positive");
        }
        AtomicInteger ioThreadId = new AtomicInteger();
        ioGroup = new NioEventLoopGroup(0, runnable -> {
            Thread thread = new Thread(runnable, "kv-framed-io-" + ioThreadId.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger callbackThreadId = new AtomicInteger();
        int callbackThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
        callbackExecutor = new ThreadPoolExecutor(
                callbackThreads,
                callbackThreads,
                0,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "kv-framed-callback-" + callbackThreadId.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                (task, executor) -> {
                    if (executor.isShutdown()) {
                        throw new RejectedExecutionException("framed callback executor is shut down");
                    }
                    task.run();
                });

        connections = new Connection[hosts.size()][connectionsPerServer];
        nextConnection = new AtomicInteger[hosts.size()];
        Bootstrap bootstrap = new Bootstrap()
                .group(ioGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true);
        try {
            for (int node = 0; node < hosts.size(); node++) {
                nextConnection[node] = new AtomicInteger();
                for (int index = 0; index < connectionsPerServer; index++) {
                    Connection connection = new Connection(node);
                    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(
                                            KvWireProtocol.MAX_FRAME_BYTES,
                                            0,
                                            KvWireProtocol.LENGTH_FIELD_BYTES,
                                            0,
                                            KvWireProtocol.LENGTH_FIELD_BYTES))
                                    .addLast(new FlushConsolidationHandler(256, true))
                                    .addLast(connection);
                        }
                    });
                    Channel channel = bootstrap
                            .connect(hosts.get(node), clientBasePort + node + 1)
                            .syncUninterruptibly().channel();
                    connection.channel = channel;
                    connections[node][index] = connection;
                    allChannels.add(channel);
                }
            }
        } catch (RuntimeException e) {
            close();
            throw new IllegalStateException("failed to connect framed client transport", e);
        }
    }

    public int numServers() {
        return connections.length;
    }

    public RequestHandle execute(int nodeId, KvRequest request, long remainingDeadlineNanos,
            Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError) {
        if (closed.get()) {
            onError.accept(new RpcFailure(Status.UNAVAILABLE.withDescription("framed transport is closed"), 0));
            return () -> { };
        }
        if (nodeId < 0 || nodeId >= connections.length) {
            throw new IllegalArgumentException("invalid nodeId " + nodeId);
        }
        if (remainingDeadlineNanos <= 0) {
            onError.accept(new RpcFailure(
                    Status.DEADLINE_EXCEEDED.withDescription("SLA deadline expired before dispatch"), 0));
            return () -> { };
        }
        int index = Math.floorMod(nextConnection[nodeId].getAndIncrement(), connections[nodeId].length);
        Connection connection = connections[nodeId][index];
        long correlationId = correlationIds.incrementAndGet();
        long sendEpochMs = System.currentTimeMillis();
        long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingDeadlineNanos)
                + (remainingDeadlineNanos % 1_000_000L == 0 ? 0 : 1));
        long deadlineEpochMs = saturatedAdd(sendEpochMs, remainingMs);
        PendingCall pending = new PendingCall(correlationId, onResponse, onError, connection);
        connection.pending.put(correlationId, pending);

        byte[] payload = request.toByteArray();
        int frameLength = KvWireProtocol.REQUEST_HEADER_BYTES + payload.length;
        if (frameLength > KvWireProtocol.MAX_FRAME_BYTES) {
            pending.fail(Status.INVALID_ARGUMENT.withDescription("request frame exceeds maximum size"), 0);
            return pending;
        }
        ByteBuf frame = connection.channel.alloc().buffer(KvWireProtocol.LENGTH_FIELD_BYTES + frameLength);
        frame.writeInt(frameLength);
        frame.writeByte(KvWireProtocol.VERSION);
        frame.writeByte(KvWireProtocol.REQUEST);
        frame.writeLong(correlationId);
        frame.writeLong(sendEpochMs);
        frame.writeLong(deadlineEpochMs);
        frame.writeBytes(payload);
        connection.channel.writeAndFlush(frame).addListener(future -> {
            if (!future.isSuccess()) {
                pending.fail(Status.UNAVAILABLE.withDescription("request write failed")
                        .withCause(future.cause()), 0);
            }
        });
        return pending;
    }

    private static long saturatedAdd(long left, long right) {
        return right > 0 && left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private final class Connection extends SimpleChannelInboundHandler<ByteBuf> {
        private final int nodeId;
        private final ConcurrentHashMap<Long, PendingCall> pending = new ConcurrentHashMap<>();
        private volatile Channel channel;

        private Connection(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) throws Exception {
            if (frame.readableBytes() < KvWireProtocol.TERMINAL_HEADER_BYTES) {
                throw new CorruptedFrameException("server response frame is shorter than the fixed header");
            }
            byte version = frame.readByte();
            byte type = frame.readByte();
            if (version != KvWireProtocol.VERSION) {
                throw new CorruptedFrameException("unsupported server frame version " + version);
            }
            long correlationId = frame.readLong();
            long serverReplyEpochMs = frame.readLong();
            PendingCall call = pending.remove(correlationId);
            if (call == null || !call.terminal.compareAndSet(false, true)) {
                return;
            }
            switch (type) {
                case KvWireProtocol.RESPONSE -> {
                    try {
                        KvResponse response = KvResponse.parseFrom(
                                frame.nioBuffer(frame.readerIndex(), frame.readableBytes()));
                        dispatch(() -> call.onResponse.accept(response));
                    } catch (InvalidProtocolBufferException e) {
                        dispatch(() -> call.onError.accept(new RpcFailure(
                                Status.DATA_LOSS.withDescription("invalid response payload").withCause(e),
                                serverReplyEpochMs)));
                    }
                }
                case KvWireProtocol.REJECTED -> dispatch(() -> call.onError.accept(new RpcFailure(
                        Status.RESOURCE_EXHAUSTED.withDescription("server ingress hard cap reached"),
                        serverReplyEpochMs)));
                case KvWireProtocol.DEADLINE_EXCEEDED -> dispatch(() -> call.onError.accept(new RpcFailure(
                        Status.DEADLINE_EXCEEDED.withDescription("request reached server after its SLA deadline"),
                        serverReplyEpochMs)));
                default -> throw new CorruptedFrameException("unsupported server frame type " + type);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            Status status = Status.UNAVAILABLE.withDescription(
                    "framed connection to server " + nodeId + " closed");
            pending.values().forEach(call -> call.fail(status, 0));
            pending.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private final class PendingCall implements RequestHandle {
        private final long correlationId;
        private final Consumer<KvResponse> onResponse;
        private final Consumer<RpcFailure> onError;
        private final Connection connection;
        private final AtomicBoolean terminal = new AtomicBoolean();

        private PendingCall(long correlationId, Consumer<KvResponse> onResponse,
                Consumer<RpcFailure> onError, Connection connection) {
            this.correlationId = correlationId;
            this.onResponse = onResponse;
            this.onError = onError;
            this.connection = connection;
        }

        @Override
        public void cancel() {
            if (terminal.compareAndSet(false, true)) {
                connection.pending.remove(correlationId, this);
            }
        }

        private void fail(Status status, long serverReplyEpochMs) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            connection.pending.remove(correlationId, this);
            dispatch(() -> onError.accept(new RpcFailure(status, serverReplyEpochMs)));
        }
    }

    private void dispatch(Runnable callback) {
        try {
            callbackExecutor.execute(callback);
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                throw e;
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        allChannels.forEach(Channel::close);
        for (Channel channel : allChannels) {
            channel.closeFuture().syncUninterruptibly();
        }
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        callbackExecutor.shutdown();
        try {
            if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                callbackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callbackExecutor.shutdownNow();
        }
    }
}

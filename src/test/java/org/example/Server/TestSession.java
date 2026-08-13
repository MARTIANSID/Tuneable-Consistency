package org.example.Server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.example.raft.KvClientGrpc;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Minimal test session over the real client protocol: blocking single calls
 * plus an optional async listener for load tests. Requests are SLA-driven
 * like the real client; helpers fill the session-index fields.
 */
final class TestSession implements AutoCloseable {

    private final ManagedChannel channel;
    private final StreamObserver<KvRequest> stream;
    private final ConcurrentHashMap<Long, CompletableFuture<KvResponse>> waiting = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private volatile Consumer<KvResponse> asyncListener;

    TestSession(int port) {
        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        stream = KvClientGrpc.newStub(channel).session(new StreamObserver<>() {
            @Override
            public void onNext(KvResponse response) {
                Consumer<KvResponse> listener = asyncListener;
                if (listener != null) {
                    listener.accept(response);
                }
                CompletableFuture<KvResponse> future = waiting.remove(response.getRequestId());
                if (future != null) {
                    future.complete(response);
                }
            }

            @Override
            public void onError(Throwable t) {
                waiting.values().forEach(f -> f.completeExceptionally(t));
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    void setAsyncListener(Consumer<KvResponse> listener) {
        this.asyncListener = listener;
    }

    KvResponse call(KvRequest.Builder request) throws Exception {
        long id = ids.incrementAndGet();
        CompletableFuture<KvResponse> future = new CompletableFuture<>();
        waiting.put(id, future);
        sendRaw(request.setRequestId(id).build());
        return future.get(10, TimeUnit.SECONDS);
    }

    /** Fire and forget; responses reach the async listener only. */
    void send(KvRequest.Builder request) {
        sendRaw(request.setRequestId(ids.incrementAndGet()).build());
    }

    private void sendRaw(KvRequest request) {
        synchronized (stream) {
            stream.onNext(request);
        }
    }

    KvResponse write(String key, String value, int applicationId, int slaId) throws Exception {
        return call(writeRequest(key, value, applicationId, slaId));
    }

    KvResponse read(String key, int applicationId, int slaId, int committedAnchor, int uncommittedAnchor)
            throws Exception {
        return call(readRequest(key, applicationId, slaId, committedAnchor, uncommittedAnchor));
    }

    static KvRequest.Builder writeRequest(String key, String value, int applicationId, int slaId) {
        return KvRequest.newBuilder().setIsRead(false).setKey(key).setValue(value)
                .setApplicationId(applicationId).setSlaId(slaId)
                .setCommittedSessionIndex(-1).setUncommittedSessionIndex(-1);
    }

    static KvRequest.Builder readRequest(String key, int applicationId, int slaId, int committedAnchor,
            int uncommittedAnchor) {
        return KvRequest.newBuilder().setIsRead(true).setKey(key)
                .setApplicationId(applicationId).setSlaId(slaId)
                .setCommittedSessionIndex(committedAnchor).setUncommittedSessionIndex(uncommittedAnchor);
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}

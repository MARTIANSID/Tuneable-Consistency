// TokenBucketServer.java
package org.example.Server;

import java.io.IOException;
import org.ds.paxos.Empty;
import org.ds.paxos.TokenBucketGrpc;
import org.ds.paxos.TokenStatusRequest;
import org.ds.paxos.TokenStatusResponse;
import org.ds.paxos.TokenUpdateRequest;
import org.ds.paxos.TokenConsumeRequest;
import org.ds.paxos.TokenConsumeResponse;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class TokenBucketServer extends TokenBucketGrpc.TokenBucketImplBase {
    private final double capacity;
    private final double refillRatePerMs;

    private double tokens;
    private long lastUpdatedTime;

    public TokenBucketServer(double capacity, double refillRatePerSecond) {
        this.capacity = capacity;
        this.refillRatePerMs = refillRatePerSecond / 1000.0;
        this.tokens = capacity;
        this.lastUpdatedTime = System.currentTimeMillis();
    }

    private synchronized void updateTokens(long currentTime) {
        long deltaTime = currentTime - lastUpdatedTime;
        if (deltaTime > 0) {
            tokens = Math.min(capacity, tokens + deltaTime * refillRatePerMs);
            lastUpdatedTime = currentTime;
        }
    }

    @Override
    public synchronized void getTokenStatus(TokenStatusRequest request, StreamObserver<TokenStatusResponse> responseObserver) {
        long now = System.currentTimeMillis();
        updateTokens(now);

        TokenStatusResponse response = TokenStatusResponse.newBuilder()
                .setLastUpdatedTime(lastUpdatedTime)
                .setTokensAvailable(tokens)
                .build();
        System.out.println("Requested Token Information: lastUpdatedTime = " + lastUpdatedTime + ", tokens = " + tokens);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public synchronized void consumeTokens(TokenConsumeRequest request, StreamObserver<TokenConsumeResponse> responseObserver) {
        long now = System.currentTimeMillis();
        updateTokens(now);

        double required = request.getRequiredTokens();
        boolean granted = tokens >= required;

        if (granted) {
            tokens -= required;
            lastUpdatedTime = now;
        }

        responseObserver.onNext(TokenConsumeResponse.newBuilder()
                .setGranted(granted)
                .setTokensRemaining(tokens)
                .setLastUpdatedTime(lastUpdatedTime)
                .build());
        responseObserver.onCompleted();

        System.out.println("Tokens Requested: " + required + " | Granted: " + granted + " | Tokens left: " + tokens);
    }


    @Override
    public synchronized void updateTokenState(TokenUpdateRequest request, StreamObserver<Empty> responseObserver) {
        this.tokens = request.getTokensRemaining();
        this.lastUpdatedTime = request.getLastUpdatedTime();

        responseObserver.onNext(Empty.newBuilder().build());
        System.out.println("Updated Token: lastUpdatedTime = " + lastUpdatedTime + ", tokens = " + tokens);

        responseObserver.onCompleted();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        TokenBucketServer service = new TokenBucketServer(1000.0, 1000.0); // capacity: 10 tokens, 1 token/sec
        Server server = ServerBuilder.forPort(8500).addService(service).build().start();
        System.out.println("TokenBucketServer started on port 8500");
        server.awaitTermination();
    }
}
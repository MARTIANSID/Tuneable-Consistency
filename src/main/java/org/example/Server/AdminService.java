package org.example.Server;

import org.example.raft.AdminAck;
import org.example.raft.AdminGrpc;
import org.example.raft.AdminStatusReply;
import org.example.raft.AdminStatusRequest;
import org.example.raft.SetDropTrafficRequest;
import org.example.raft.ShutdownRequest;

import io.grpc.stub.StreamObserver;

/**
 * Experiment-harness control surface for a server process. The workload
 * driver runs in a separate process and uses this service for what it used to
 * do on in-process ServerImpl references: find the leader, inject simulated
 * network failures, and tear the node down at the end of a run. Not part of
 * the protocol under study.
 */
public class AdminService extends AdminGrpc.AdminImplBase {

    private final ServerImpl node;
    private final Runnable shutdownAction;

    public AdminService(ServerImpl node, Runnable shutdownAction) {
        this.node = node;
        this.shutdownAction = shutdownAction;
    }

    @Override
    public void getStatus(AdminStatusRequest request, StreamObserver<AdminStatusReply> responseObserver) {
        responseObserver.onNext(AdminStatusReply.newBuilder()
                .setNodeId(node.nodeId())
                .setRole(node.status.name())
                .setTrafficDropped(node.isDropAllServerNetworkTraffic())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void setDropTraffic(SetDropTrafficRequest request, StreamObserver<AdminAck> responseObserver) {
        node.setDropAllServerNetworkTraffic(request.getDrop());
        System.out.printf("Admin: inter-server traffic %s on server %d%n",
                request.getDrop() ? "DROPPED" : "restored", node.nodeId());
        responseObserver.onNext(AdminAck.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<AdminAck> responseObserver) {
        responseObserver.onNext(AdminAck.getDefaultInstance());
        responseObserver.onCompleted();
        // Off the gRPC thread: the shutdown drains the very server this RPC
        // arrived on, so it must run after this handler returns.
        new Thread(shutdownAction, "admin-shutdown").start();
    }
}

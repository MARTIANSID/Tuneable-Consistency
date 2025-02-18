package org.example.Server;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Timer.CustomTimer;
import java.security.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.ds.paxos.RaftGrpc.*;

import org.example.Utility.Log;
import org.example.Utility.ServerStatus.*;

public class ServerImpl extends RaftGrpc.RaftImplBase {
    AtomicInteger currentTerm;
    AtomicInteger votedFor;
    ConcurrentLinkedDeque<Log> log;
    AtomicInteger commitIndex;
    AtomicInteger lastApplied;
    AtomicIntegerArray nextIndex;
    AtomicIntegerArray matchIndex;

    CustomTimer electionTimer;

    int serverId;

    List<RaftStub> peers;
    RaftStub[] stubs;

    ServerCurrentStatus status;
    public ServerImpl(int serverId) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new AtomicInteger(-1);
        this.log = new ConcurrentLinkedDeque<>();
        this.commitIndex = new AtomicInteger();
        this.lastApplied = new AtomicInteger();
        this.nextIndex = new AtomicIntegerArray(5);
        this.matchIndex = new AtomicIntegerArray(5);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(),2000, TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.stubs = new RaftStub[5];
        this.status = ServerCurrentStatus.FOLLOWER;

        // setting the peers list
        for(int i = 0; i < 5; i ++) {
            if(i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000 + i).usePlaintext().build();
                stubs[i] = RaftGrpc.newStub(channel);
            }
        }
        // starting the election timer
        this.electionTimer.start();
    }
    @Override
    public void appendEntries(AppendEntriesArgument request, StreamObserver<AppendEntriesResult> responseObserver) {
    }

    @Override
    public void requestVote(RequestVoteArguments request, StreamObserver<RequestVoteResult> responseObserver) {
        super.requestVote(request, responseObserver);
    }

    @Override
    public void sendTransaction(ClientMessage request, StreamObserver<Empty> responseObserver) {
        super.sendTransaction(request, responseObserver);
    }

    private void updateTerm() {
       int cT = currentTerm.get() + 1;
       currentTerm.set(cT);
    }

    private int getLastLogIndex() {
        if(log.size() > 0) {
            return -1;
        } else {
            return log.peekLast().index;
        }
    }

    private int getLastLogTerm() {
        if(log.size() > 0) {
            return -1;
        } else {
            return log.peekLast().term;
        }
    }

    private RequestVoteArguments getRequestVoteArgumentsObject() {
       return RequestVoteArguments.newBuilder().setCandidateId(this.serverId).setCandidatesTerm(this.currentTerm.get()).setLastLogTerm(this.getLastLogTerm()).setLastLogIndex(this.getLastLogIndex()).build();
    }

    private void requestForVotes() {
       RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

       for(int i = 0; i < 5; i++) {
           stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
               @Override
               public void onNext(RequestVoteResult requestVoteResult) {
                   
               }

               @Override
               public void onError(Throwable throwable) {

               }

               @Override
               public void onCompleted() {

               }
           });
       }
    }

    public void startElection() {
        // Starting Election
        System.out.println("Starting Election");

        // first update the term
        updateTerm();

        requestForVotes();
    }
}

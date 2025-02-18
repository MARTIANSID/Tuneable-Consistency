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

    AtomicInteger votes;

    ServerCurrentStatus status;
    public ServerImpl(int serverId) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new AtomicInteger(-1);
        this.log = new ConcurrentLinkedDeque<>();
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.nextIndex = new AtomicIntegerArray(5);
        this.matchIndex = new AtomicIntegerArray(5);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(),2000, TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.stubs = new RaftStub[5];
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);





        // setting the peers list
        for(int i = 0; i < 5; i ++) {
            //setting up the nextIndex and matchIndex

            nextIndex.set(i, 0);
            matchIndex.set(i, -1);

            // setting up the stubs
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

    private void handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if(requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            // this node is not upto date
            currentTerm.set(requestVoteResult.getCurrentTerm());
        } else if(requestVoteResult.getIsVoteGranted()) {
            // vote granted
            int votesTillNow = votes.get();
            votes.set(votesTillNow + 1);
        }
    }

    private void requestForVotes() {
       RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

        CountDownLatch latch = new CountDownLatch(4);

        for(int i = 0; i < 5; i++) {
           stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
               @Override
               public void onNext(RequestVoteResult requestVoteResult) {
                    handleRequestVoteResult(requestVoteResult);
                    latch.countDown();
               }

               @Override
               public void onError(Throwable throwable) {

               }

               @Override
               public void onCompleted() {

               }
           });

       }
        try {
            // Wait for up to 30ms for responses
            boolean success = latch.await(30, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendAppendEntries() {
        // send appendEntries

        for(int i = 0; i < 5; i++) {

            // this is the condtion that we need to just sendHeartBeat
            if ((log.size() == 0) || (matchIndex.get(i) == log.size() - 1)) {
                
            } else {

            }
        }

    }

    public void reinitialiseIndexes() {
        for(int i = 0; i < 5; i++) {
            nextIndex.set(i, 0);
            matchIndex.set(i, -1);
        }
    }

    public void startElection() {
        // Starting Election
        System.out.println("Starting Election");
        // first update the term
        updateTerm();
        // resetting the votes
        votes.set(0);
        requestForVotes();

        if(votes.get() >= 2) {
            // reinitialise state
            reinitialiseIndexes();

            // this node becomes the leader
            this.status = ServerCurrentStatus.LEADER;
            // start sending AppendEntries
            sendAppendEntries();
        } else {
            startTheElectionTimer();
        }
    }
    private void startTheElectionTimer() {
        this.electionTimer.reset();
        this.electionTimer.start();
    }
}

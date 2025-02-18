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


    ConcurrentHashMap<Integer, Integer> votedFor; // term : candidateId
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

    AtomicBoolean isElectionOver;

    int currentLeader;
    public ServerImpl(int serverId) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new ConcurrentHashMap<>();
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
        this.isElectionOver = new AtomicBoolean(false);


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

    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogTerm();

        // deny vote condition
        if((lastLogTermOfCurrentNode > lastLogTermOfCandidate) || ((lastLogTermOfCurrentNode == lastLogTermOfCandidate) && (lastLogIndexOfCurrentNode > lastLogIndexOfCandidate))) {
            return false;
        }

        return true;
    }

    @Override
    public void requestVote(RequestVoteArguments requestVoteArguments, StreamObserver<RequestVoteResult> responseObserver) {
        int currentTermOfTheCandidate = requestVoteArguments.getCandidatesTerm(), lastLogIndexOfCandidate = requestVoteArguments.getLastLogIndex(), lastLogTermOfCandidate = requestVoteArguments.getLastLogIndex();

        boolean isVoteGranted = true;

        if(votedFor.containsKey(this.currentTerm) || (this.currentTerm.get() > currentTermOfTheCandidate) || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
            // reply false here
            isVoteGranted = false;
        }

        RequestVoteResult requestVoteResult = RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted).setCurrentTerm(currentTerm.get()).build();
        responseObserver.onNext(requestVoteResult);
        responseObserver.onCompleted();
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

    private boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if(requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            // this node is not upto date
            currentTerm.set(requestVoteResult.getCurrentTerm());
            // now it cannot become the leader
            return false;
        } else if(requestVoteResult.getIsVoteGranted()) {
            // vote granted
            int votesTillNow = votes.get();
            votes.set(votesTillNow + 1);

            // voite granted
            return true;
        }
        return true;
    }

    private void requestForVotes() {
       RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

        CountDownLatch latch = new CountDownLatch(4);

        for(int i = 0; i < 5; i++) {
           stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
               @Override
               public void onNext(RequestVoteResult requestVoteResult) {
                   if(isElectionOver.get()) {
                       return;
                   }
                    boolean isSuccessful = handleRequestVoteResult(requestVoteResult);

                    // it is not successful when the currentTerm of the leader is not upto date
                    if(!isSuccessful) {
                        votes.set(-(int)1e9);
                        while(latch.getCount() > 0) {
                            latch.countDown();
                        }
                    } else {
                        latch.countDown();
                    }
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
            // now election is over cannot receive more responses
            isElectionOver.set(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendAppendEntries(boolean isHeartBeat) {
        for(int i = 0; i < 5; i++) {

        }
    }
    private void sendAppendEntries() {
        // send appendEntries

        for(int i = 0; i < 5; i++) {
            // this is the condtion that we need to just sendHeartBeat
            if ((log.size() == 0) || (matchIndex.get(i) == log.size() - 1)) {
                sendAppendEntries(true);
            } else {
                sendAppendEntries(false);
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
        // this node becomes a candidate
        this.status = ServerCurrentStatus.CANDIDATE;
        // first update the term
        updateTerm();
        // resetting the votes
        votes.set(0);
        isElectionOver.set(false);
        requestForVotes();

        if(votes.get() >= 2) {
            // reinitialise state
            reinitialiseIndexes();
            // this node becomes the leader
            this.status = ServerCurrentStatus.LEADER;
            this.currentLeader = this.serverId;
            // start sending AppendEntries
            sendAppendEntries();
        } else {
            this.status = ServerCurrentStatus.FOLLOWER;
            startTheElectionTimer();
        }
    }
    private void startTheElectionTimer() {
        this.electionTimer.reset();
        this.electionTimer.start();
    }
}

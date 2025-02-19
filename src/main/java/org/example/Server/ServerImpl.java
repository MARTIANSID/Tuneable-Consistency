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
import org.example.Utility.RaftLog;
import org.example.Utility.ServerStatus.*;

public class ServerImpl extends RaftGrpc.RaftImplBase {
    AtomicInteger currentTerm;


    ConcurrentHashMap<Integer, Integer> votedFor; // term : candidateId

    RaftLog log;

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
        this.log = new RaftLog();
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.nextIndex = new AtomicIntegerArray(5);
        this.matchIndex = new AtomicIntegerArray(5);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(),new Random().nextInt(201) + 200, TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.stubs = new RaftStub[5];
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);


        // setting the peers list
        for(int i = 0; i < 5; i ++) {
            //setting up the nextIndex and matchIndex

            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
            // setting up the stubs
            if(i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i+1)).usePlaintext().build();
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
        int currentTermOfTheCandidate = requestVoteArguments.getCandidatesTerm(), lastLogIndexOfCandidate = requestVoteArguments.getLastLogIndex(), lastLogTermOfCandidate = requestVoteArguments.getLastLogTerm(), candidateId = requestVoteArguments.getCandidateId();

        boolean isVoteGranted = true;

        if(currentTermOfTheCandidate > currentTerm.get()) {
            currentTerm.set(currentTermOfTheCandidate);
            // the node must become a follower
            if(status == ServerCurrentStatus.LEADER) {
                startTheElectionTimer();
            }
            status = ServerCurrentStatus.FOLLOWER;
        }

        // all the necessary conditions to check for denying vote
        if(votedFor.containsKey(this.currentTerm.get()) || (this.currentTerm.get() > currentTermOfTheCandidate) || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
            // reply false here
            isVoteGranted = false;
        }
        if(isVoteGranted) {
            votedFor.put(currentTerm.get(), candidateId);
            startTheElectionTimer();
        }


        RequestVoteResult requestVoteResult = RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted).setCurrentTerm(currentTerm.get()).build();
        responseObserver.onNext(requestVoteResult);
        responseObserver.onCompleted();
    }

    @Override
    public void sendTransaction(ClientMessage request, StreamObserver<Empty> responseObserver) {
        super.sendTransaction(request, responseObserver);
    }


    private int getLastLogIndex() {
        if(log.size() == 0) {
            return -1;
        } else {
            return log.getLastLogEntry().index;
        }
    }

    private int getLastLogTerm() {
        if(log.size() == 0) {
            return -1;
        } else {
            return log.getLastLogEntry().term;
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
            votes.incrementAndGet();
            // voite granted
            return true;
        }
        return true;
    }

    private void endLatchHold(CountDownLatch latch) {
        while(latch.getCount() > 0) {
            latch.countDown();
        }
    }

    private void requestForVotes() {
       RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

        CountDownLatch latch = new CountDownLatch(4);

        for(int i = 0; i < 5; i++) {

            if(i == serverId) continue;

            stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
               @Override
               public void onNext(RequestVoteResult requestVoteResult) {
                   if(isElectionOver.get()) {
                       return;
                   }
                    boolean isSuccessful = handleRequestVoteResult(requestVoteResult);

                    // it is not successful when the currentTerm of the leader is not up-to date
                    if(!isSuccessful) {
                        votes.set(-(int)1e9);
                        endLatchHold(latch);
                    } else if(votes.get() >= 2) {
                        // majority is reached here
                        endLatchHold(latch);
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
            boolean success = latch.await(100, TimeUnit.MILLISECONDS);
            // now election is over cannot receive more responses
            isElectionOver.set(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private void sendAppendEntries() {
        // send appendEntries
        for(int i = 0; i < 5; i++) {
            if(i == serverId) continue;

            // index to send from
            int indexToSendFrom = nextIndex.get(i);
            Log prevEntry = log.get(indexToSendFrom - 1);
            List<Log> entries = log.logEntriesFromIndex(indexToSendFrom);
            
        }
    }
    public void reinitialiseIndexes() {
        for(int i = 0; i < 5; i++) {
            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
        }
    }
    public void startElection() {
        // Starting Election
        System.out.println("Starting Election");
        // this node becomes a candidate
        this.status = ServerCurrentStatus.CANDIDATE;
        // first update the term
        currentTerm.incrementAndGet();
        // reset the timer
        startTheElectionTimer();
        // resetting the votes
        votes.set(0);
        isElectionOver.set(false);
        requestForVotes();

        if(votes.get() >= 2) {
            System.out.println(serverId +" " + "Became the leader");
            // stop the election timer
            electionTimer.stop();
            // reinitialise state
            reinitialiseIndexes();
            // this node becomes the leader
            this.status = ServerCurrentStatus.LEADER;
            this.currentLeader = this.serverId;
            // start sending AppendEntries
            sendAppendEntries();
        } else {
            this.status = ServerCurrentStatus.FOLLOWER;
        }
    }
    private void startTheElectionTimer() {
        this.electionTimer.reset();
        this.electionTimer.start();
    }
}

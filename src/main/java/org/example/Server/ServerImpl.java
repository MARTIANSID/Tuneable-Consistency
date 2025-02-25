package org.example.Server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Timer.CustomTimer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.ds.paxos.RaftGrpc.*;

import org.example.Utility.LogEntry;
import org.example.Utility.RaftLog;
import org.example.Utility.ServerStatus.*;

public class ServerImpl extends RaftGrpc.RaftImplBase {
    AtomicInteger currentTerm;

    // can optimize the votedFor logic
    ConcurrentHashMap<Integer, Integer> votedFor; // term : candidateId

    RaftLog log;

    AtomicInteger commitIndex;
    AtomicInteger lastApplied;
    AtomicIntegerArray nextIndex;
    AtomicIntegerArray matchIndex;


    ConcurrentHashMap<Integer, Integer> totalAcks;

    CustomTimer electionTimer;


    int serverId;

    List<RaftStub> peers;
    RaftStub[] stubs;
    RaftBlockingStub[] blockingStubs;

    AtomicInteger votes;

    ServerCurrentStatus status;

    AtomicBoolean isElectionOver;

    boolean doesLeaderHasHighestTerm;

    int currentLeader;

    AtomicInteger ackIndex;

    ConcurrentHashMap<Integer, Integer> matchIndexCount;



    public ServerImpl(int serverId) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new ConcurrentHashMap<>();
        this.log = new RaftLog();
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.nextIndex = new AtomicIntegerArray(5);
        this.matchIndex = new AtomicIntegerArray(5);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(), (new Random().nextInt(200) + 300), TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.stubs = new RaftStub[5];
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);
        this.doesLeaderHasHighestTerm = false;
        this.blockingStubs = new RaftBlockingStub[5];
        this.ackIndex = new AtomicInteger(-1);
        this.totalAcks = new ConcurrentHashMap<>();
        this.matchIndexCount = new ConcurrentHashMap<>();

        // setting the peers list
        for (int i = 0; i < 5; i++) {
            //setting up the nextIndex and matchIndex

            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
            // setting up the stubs
            if (i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i + 1)).usePlaintext().build();
                stubs[i] = RaftGrpc.newStub(channel);
                blockingStubs[i] = RaftGrpc.newBlockingStub(channel);
            }
        }
        // starting the election timer
        this.electionTimer.start();
    }

    @Override
    public void appendEntries(AppendEntriesArgument appendEntriesArgument, StreamObserver<AppendEntriesResult> responseObserver) {
        int leadersTerm = appendEntriesArgument.getLeadersTerm(), prevLogIndex = appendEntriesArgument.getPrevLogIndex(), prevLogTerm = appendEntriesArgument.getPrevLogTerm(), leadersCommitIndex = appendEntriesArgument.getLeadersCommit(), leaderId = appendEntriesArgument.getLeadersId(), leadersAckIndex = appendEntriesArgument.getAckIndex();

        if (leadersTerm >= currentTerm.get()) {
            currentLeader = leaderId;
            currentTerm.set(leadersTerm);
            if (status == ServerCurrentStatus.LEADER) {
                startTheElectionTimer();
            }
            status = ServerCurrentStatus.FOLLOWER;
        }

        if (leadersTerm < currentTerm.get() || !log.checkIfPrevLogIndexHasPrevLogTerm(prevLogIndex, prevLogTerm)) {
            responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get()).setIsSuccessFull(false).setFollowerId(serverId).build());
            responseObserver.onCompleted();
            return;
        }

        Log leadersEntries = appendEntriesArgument.getEntriesToAppend();
        // now first clear the entries starting from prevLogIndex + 1
        log.truncateAfter(prevLogIndex + 1);

        // appending leaders entries
        log.appendEntries(leadersEntries);

        // updating commit index of follower
        if (leadersCommitIndex > commitIndex.get()) {
            commitIndex.set(Math.min(leadersCommitIndex, log.size() - 1));
        }

        // reset the election timer
        startTheElectionTimer();

        responseObserver.onNext(AppendEntriesResult.newBuilder().setIsSuccessFull(true).setFollowerId(serverId).build());
        responseObserver.onCompleted();
    }

    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogTerm();

        // deny vote condition
        if ((lastLogTermOfCurrentNode > lastLogTermOfCandidate) || ((lastLogTermOfCurrentNode == lastLogTermOfCandidate) && (lastLogIndexOfCurrentNode > lastLogIndexOfCandidate))) {
            return false;
        }

        return true;
    }

    @Override
    public void requestVote(RequestVoteArguments requestVoteArguments, StreamObserver<RequestVoteResult> responseObserver) {
        int currentTermOfTheCandidate = requestVoteArguments.getCandidatesTerm(), lastLogIndexOfCandidate = requestVoteArguments.getLastLogIndex(), lastLogTermOfCandidate = requestVoteArguments.getLastLogTerm(), candidateId = requestVoteArguments.getCandidateId();

        boolean isVoteGranted = true;

        if (currentTermOfTheCandidate > currentTerm.get()) {
            currentTerm.set(currentTermOfTheCandidate);
            // the node must become a follower
            if (status == ServerCurrentStatus.LEADER) {
                startTheElectionTimer();
            }
            status = ServerCurrentStatus.FOLLOWER;
        }

        // all the necessary conditions to check for denying vote
        if (votedFor.containsKey(this.currentTerm.get()) || (this.currentTerm.get() > currentTermOfTheCandidate) || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
            // reply false here
            isVoteGranted = false;
        }
        if (isVoteGranted) {
            votedFor.put(currentTerm.get(), candidateId);
            startTheElectionTimer();
        }

        RequestVoteResult requestVoteResult = RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted).setCurrentTerm(currentTerm.get()).build();
        responseObserver.onNext(requestVoteResult);
        responseObserver.onCompleted();
    }

    @Override
    public void sendTransaction(ClientMessage clientMessage, StreamObserver<Empty> responseObserver) {
        // check if the current node is leader or not, if not forward request to leader
        if (serverId != currentLeader) {
            // can use a blocking stub here
            blockingStubs[currentLeader].sendTransaction(clientMessage);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        System.out.println("Got the transaction!");
        Transaction t = clientMessage.getT();
        int writeConcern = clientMessage.getWriteConcern();
        int index = log.size() + 1;
        log.append(new LogEntry(log.size(), currentTerm.get(), t, writeConcern));
        totalAcks.put(index, 1);
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }


    private int getLastLogIndex() {
        if (log.size() == 0) {
            return -1;
        } else {
            return log.getLastLogEntry().index;
        }
    }

    private int getLastLogTerm() {
        if (log.size() == 0) {
            return -1;
        } else {
            return log.getLastLogEntry().term;
        }
    }

    private RequestVoteArguments getRequestVoteArgumentsObject() {
        return RequestVoteArguments.newBuilder().setCandidateId(this.serverId).setCandidatesTerm(this.currentTerm.get()).setLastLogTerm(this.getLastLogTerm()).setLastLogIndex(this.getLastLogIndex()).build();
    }

    private boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if (requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            // this node is not upto date
            currentTerm.set(requestVoteResult.getCurrentTerm());
            // now it cannot become the leader
            return false;
        } else if (requestVoteResult.getIsVoteGranted()) {
            // vote granted
            votes.incrementAndGet();
            // voite granted
            return true;
        }
        return true;
    }

    private void endLatchHold(CountDownLatch latch) {
        while (latch.getCount() > 0) {
            latch.countDown();
        }
    }

    private void requestForVotes() {
        RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

        CountDownLatch latch = new CountDownLatch(4);

        for (int i = 0; i < 5; i++) {

            if (i == serverId) continue;

            stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
                @Override
                public void onNext(RequestVoteResult requestVoteResult) {
                    if (isElectionOver.get()) {
                        return;
                    }
                    boolean isSuccessful = handleRequestVoteResult(requestVoteResult);
                    // it is not successful when the currentTerm of the leader is not up-to date
                    if (!isSuccessful) {
                        votes.set(Integer.MIN_VALUE);
                        endLatchHold(latch);
                    } else if (votes.get() >= 2) {
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
            boolean success = latch.await(50, TimeUnit.MILLISECONDS);
            // now election is over cannot receive more responses
            isElectionOver.set(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private List<LogEntryProto> convertLogEntryToProto(List<LogEntry> entries) {
        List<LogEntryProto> result = new ArrayList<>();

        for (LogEntry entry : entries) {
            result.add(LogEntryProto.newBuilder().setLogIndex(entry.index).setT(entry.t).setTerm(entry.term).build());
        }
        return result;
    }

    private boolean handleAppendEntriesResult(AppendEntriesResult appendEntriesResult, int matchIndexOfFollower) {
        boolean result = appendEntriesResult.getIsSuccessFull();
        int termOfFollower = appendEntriesResult.getCurrentTerm(), idOfFollower = appendEntriesResult.getFollowerId();

        if (termOfFollower > currentTerm.get()) {
            // become follower
            currentTerm.set(termOfFollower);
            status = ServerCurrentStatus.FOLLOWER;
            startTheElectionTimer();
            return false;
        }
        if (!result) {
            nextIndex.decrementAndGet(idOfFollower);
        } else {
            matchIndex.set(idOfFollower, matchIndexOfFollower);
            matchIndexCount.put(matchIndexOfFollower, matchIndexCount.getOrDefault(matchIndexOfFollower, 0) + 1);
            nextIndex.set(idOfFollower, matchIndexOfFollower + 1);

            //checking here if we need to update the commitIndex of the leader
            if((matchIndexOfFollower > commitIndex.get()) && (matchIndexCount.get(matchIndexOfFollower) >= 2 && (log.get(matchIndexOfFollower).term == currentTerm.get())) ) {
                commitIndex.set(matchIndexOfFollower);
                System.out.println("The commit index of leader updated to -- " + commitIndex.get());
            }
        }
        return true;
    }

    private void sendAppendEntries() {
        // send appendEntries

        while (status == ServerCurrentStatus.LEADER) {

            for (int i = 0; i < 5; i++) {

                if (i == serverId) continue;

                // index to send from
                int indexToSendFrom = nextIndex.get(i);
                LogEntry prevEntry = log.get(indexToSendFrom - 1);
                List<LogEntryProto> entries = convertLogEntryToProto(log.logEntriesFromIndex(indexToSendFrom));
                Log l = Log.newBuilder().addAllLog(entries).build();
                AppendEntriesArgument appendEntriesArgument = AppendEntriesArgument.newBuilder().setLeadersTerm(currentTerm.get()).setLeadersId(serverId).setLeadersCommit(commitIndex.get()).setPrevLogIndex(prevEntry.index).setPrevLogTerm(prevEntry.term).setEntriesToAppend(l).build();
                int matchIndexForFollower = log.size() - 1;

                stubs[i].appendEntries(appendEntriesArgument, new StreamObserver<AppendEntriesResult>() {
                    @Override
                    public void onNext(AppendEntriesResult appendEntriesResult) {
                        doesLeaderHasHighestTerm = handleAppendEntriesResult(appendEntriesResult, matchIndexForFollower);

                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
                if (!doesLeaderHasHighestTerm) break;
            }
            if (!doesLeaderHasHighestTerm) break;

            try {
                // in every 15 milliseconds send appendEntries
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void reinitialiseIndexes() {
        for (int i = 0; i < 5; i++) {
            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
        }
    }

    public void startElection() {
        // Starting Election
        System.out.println(serverId + " is " + "Starting Election");
        // this node becomes a candidate
        this.status = ServerCurrentStatus.CANDIDATE;
        // first update the term
        currentTerm.incrementAndGet();
        // reset the timer
        startTheElectionTimer();
        // resetting the votes
        votes.set(0);
        // vote self
        votedFor.put(currentTerm.get(), serverId);
        isElectionOver.set(false);
        requestForVotes();

        if (votes.get() >= 2 && status != ServerCurrentStatus.FOLLOWER) {

            doesLeaderHasHighestTerm = true;

            System.out.println(serverId + " " + "Became the leader" + "The term is" + currentTerm.get());
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

    @Override
    public void printLog(Empty request, StreamObserver<Empty> responseObserver) {
        System.out.println("The commit index of this node is -- " + commitIndex.get() );
        log.printLog();
    }

    private void startTheElectionTimer() {
        this.electionTimer.reset();
        this.electionTimer.start();
    }



}

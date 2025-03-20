package org.example.Server;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Timer.CustomTimer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ds.paxos.RaftGrpc.*;

import org.example.Utility.HybridClock;
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

    ConcurrentHashMap<String, Integer> tIdToLogIndex; // id : logIndex

    ConcurrentHashMap<String, Long> timeAtWhichTransactionWasReceived;

    ConcurrentHashMap<String, Double> clientBalancesMajorityCommitted;

    ConcurrentHashMap<String, Double> clientBalancesLatest;

    ConcurrentHashMap<HybridClock.TimeStamp, Integer> timeStampsInLog;

    AtomicLong totalLatency;

    ConcurrentHashMap<String, Boolean> ackSent;
    RaftStub clientStub;
    AtomicLong ackTransactionCount;

    ReadWriteLock lock;

    HybridClock hybridClock;


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
        this.tIdToLogIndex = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.ackSent = new ConcurrentHashMap<>();
        this.timeAtWhichTransactionWasReceived = new ConcurrentHashMap<>();
        this.totalLatency = new AtomicLong(0);
        this.ackTransactionCount = new AtomicLong(0);
        this.hybridClock = new HybridClock();
        this.clientBalancesMajorityCommitted = new ConcurrentHashMap<>();
        this.clientBalancesLatest = new ConcurrentHashMap<>();
        this.timeStampsInLog = new ConcurrentHashMap<>();


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
        // setting up the client stub
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        clientStub = RaftGrpc.newStub(channel);

        // starting the election timer
        this.electionTimer.start();
    }

    @Override
    public void appendEntries(AppendEntriesArgument appendEntriesArgument, StreamObserver<AppendEntriesResult> responseObserver) {
        int leadersTerm = appendEntriesArgument.getLeadersTerm(),
                prevLogIndex = appendEntriesArgument.getPrevLogIndex(),
                prevLogTerm = appendEntriesArgument.getPrevLogTerm(),
                leadersCommitIndex = appendEntriesArgument.getLeadersCommit(),
                leaderId = appendEntriesArgument.getLeadersId(),
                leadersAckIndex = appendEntriesArgument.getAckIndex();

        TimeStampProto leadersTimeStamp = appendEntriesArgument.getTimeStamp();

        lock.writeLock().lock();

        try {
            // update clock of follower using leaders clock
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(leadersTimeStamp));

            // Check if the leader's term is valid
            if (leadersTerm >= currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                if (status == ServerCurrentStatus.LEADER) {
                    startTheElectionTimer();
                }
                status = ServerCurrentStatus.FOLLOWER;
            }


            // If the leader's term is outdated or log mismatch, respond with failure
            if (leadersTerm < currentTerm.get() || !log.checkIfPrevLogIndexHasPrevLogTerm(prevLogIndex, prevLogTerm)) {
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get()).setIsSuccessFull(false).setFollowerId(serverId).build());
                responseObserver.onCompleted();
                return;
            }
            // Proceed with appending the entries
            Log leadersEntries = appendEntriesArgument.getEntriesToAppend();

            // reverting the latest balances
            rollbackTillIndex(prevLogIndex + 1);
            log.truncateAfter(prevLogIndex + 1);  // Clear entries after prevLogIndex
            log.appendEntries(leadersEntries);  // Append new entries


            // adding the entries in tIdToLogIndex, for quick access to check duplicates from client side
            // updating the latest balances
            for (LogEntryProto logEntry : leadersEntries.getLogList()) {
                tIdToLogIndex.put(logEntry.getT().getId(), logEntry.getLogIndex());
                timeStampsInLog.put(HybridClock.TimeStamp.convertToTimeStamp(logEntry.getTimeStamp()), logEntry.getLogIndex());
                updateBalances(logEntry.getT(), clientBalancesLatest);
            }


            // Update commit index
            if (leadersCommitIndex > commitIndex.get()) {
                int prevCommitIndex = commitIndex.get();

                commitIndex.set(Math.min(leadersCommitIndex, log.size() - 1));
                // update majority committed ap
                for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                    updateBalances(log.get(i).t, clientBalancesMajorityCommitted);
                }
            }

            // Reset the election timer as the leader is active
            startTheElectionTimer();
            // Send success response

            // current time of follower
            HybridClock.TimeStamp currentTimeOfFollower = hybridClock.now();

            responseObserver.onNext(AppendEntriesResult.newBuilder().setIsSuccessFull(true).setFollowerId(serverId).setTimeStamp(HybridClock.TimeStamp.convertToProto(currentTimeOfFollower)).build());

            responseObserver.onCompleted();

//            System.out.println("Got Append Entries for server -- " + serverId + " Time --- " +(System.currentTimeMillis()));

        } finally {
            lock.writeLock().unlock();
        }
    }


    // inside write lock
    private void updateBalances(Transaction t, ConcurrentHashMap<String, Double> balances) {
        String sender = t.getSender(), receiver = t.getReceiver(), id = t.getId();

        double amount = t.getAmount();

        balances.put(sender, balances.getOrDefault(sender, 100.0) - amount);
        balances.put(receiver, balances.getOrDefault(receiver, 100.0) + amount);
    }

    // inside write lock
    private void rollbackTillIndex(int logIndex) {
        // remove the entries from the logIndex till the end

        for (int i = logIndex; i < log.size(); i++) {
            Transaction t = log.get(logIndex).t;

            String sender = t.getSender(), receiver = t.getReceiver(), id = t.getId();

            double amount = t.getAmount();

            // revert the transaction, and the rollback will be performed in the clientBalancesLatest
            clientBalancesLatest.put(sender, clientBalancesLatest.get(sender) + amount);
            clientBalancesLatest.put(receiver, clientBalancesLatest.get(receiver) - amount);

            // remove the entries from tIdToLogIndex
            tIdToLogIndex.remove(id);


            // remove the timestamps
            timeStampsInLog.remove(log.get(i).timeStamp);

        }

    }


    // in lock
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

        lock.writeLock().lock();

        try {
            if (currentTermOfTheCandidate > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, currentTermOfTheCandidate));
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

        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isWriteConcernStatisfied() {
        return true;
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

        Transaction t = clientMessage.getT();

        String id = t.getId();

//        if (tIdToLogIndex.containsKey(id)) {
//            int logIndex = tIdToLogIndex.get(id);
//            if (commitIndex.get() >= logIndex) {
//                // already committed so just send the ack
//            } else if (isWriteConcernStatisfied()) {
//            }
//            // now here if the writeConcernStatisfied does not have the data basically it is a replica then the replica will update the writeConcern done on its end using appendEntries
//            responseObserver.onNext(Empty.newBuilder().build());
//            responseObserver.onCompleted();
//            return;
//        }
        int writeConcern = clientMessage.getWriteConcern();

        int index = -1;
        // we want it to be synchronized in order to get the correct index, and not allow multiple threads to get same index
        // log.size() takes in only read lock hence we need synchronized
        lock.writeLock().lock();
        try {
            System.out.println("Got the transaction!");
            index = log.size();

            if (clientMessage.hasTimeStamp()) {
                // updating the clock of leader using the time stamp sent by the client
                hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(clientMessage.getTimeStamp()));
            }

            HybridClock.TimeStamp currentTimeStamp = hybridClock.now();

            log.append(new LogEntry(index, currentTerm.get(), t, writeConcern, currentTimeStamp));
            updateBalances(t, clientBalancesLatest);
            timeStampsInLog.put(currentTimeStamp, index);
            log.updateWriteConcern(index);
            ackSent.put(id, false);
            timeAtWhichTransactionWasReceived.put(id, System.nanoTime());
            if (log.get(index).writeConcern == 0) {
                List<LogEntry> entry = new ArrayList<>();
                entry.add(log.get(index));
                sendAckForEntries(entry);
            }
            System.out.println(index);
        } finally {
            lock.writeLock().unlock();
        }

        totalAcks.put(index, 1);
        tIdToLogIndex.put(id, index);
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

    // do not think that I need a lock here as all are atomic variables
    private boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if (requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            // making this thread safe and the term is only updated to max value
            currentTerm.updateAndGet(term -> Math.max(term, requestVoteResult.getCurrentTerm()));
            return false;
        } else if (requestVoteResult.getIsVoteGranted()) {
            // vote granted
            votes.incrementAndGet();
            // vote granted
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
            // Wait for up to 50ms for responses
            boolean success = latch.await(50, TimeUnit.MILLISECONDS);
            // now election is over cannot receive more responses
            isElectionOver.set(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // inside read lock
    private List<LogEntryProto> convertLogEntryToProto(List<LogEntry> entries) {
        List<LogEntryProto> result = new ArrayList<>();

        for (LogEntry entry : entries) {
            result.add(LogEntryProto.newBuilder().setLogIndex(entry.index).setT(entry.t).setTerm(entry.term).build());
        }
        return result;
    }


    // it is inside write lock
    public boolean checkIfIndexIsAValidCommitIndex(int index) {

        if (index <= commitIndex.get()) return false;

        // check if majority of the servers are at-least at this index
        int cnt = 0;
        for (int i = 0; i < 5; i++) {
            if (matchIndex.get(i) >= index) {
                cnt++;
            }
        }
        // here I have used >= 2 because we don't actually update the match index of the leader
        return (cnt >= 2 && log.get(index).term == currentTerm.get());
    }


    private void sendAckForEntries(List<LogEntry> entriesToBeAck) {

        List<AckMessage> ackMessages = new ArrayList<>();

        for (LogEntry entry : entriesToBeAck) {
            String id = entry.t.getId();
            if (ackSent.containsKey(id) && !ackSent.get(id)) {
                // send ack for this entry
                System.out.println("sending ack");
                long latency = (System.nanoTime() - timeAtWhichTransactionWasReceived.get(id)) / 1_000_000;
                System.out.println("The latency is ---" + latency);
                totalLatency.addAndGet(latency);
                System.out.println("The total latency variable is -- " + totalLatency.get());
                ackTransactionCount.incrementAndGet();
                ackMessages.add(AckMessage.newBuilder().setT(entry.t).setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCurrentLeader(serverId).build());
                ackSent.put(id, true);
            }
        }
        clientStub.sendAckToClient(Ack.newBuilder().addAllAckMessage(ackMessages).build(), new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
    }

    private boolean handleAppendEntriesResult(AppendEntriesResult appendEntriesResult, int matchIndexOfFollower, int prevNextIndex) {
        boolean result = appendEntriesResult.getIsSuccessFull();
        int termOfFollower = appendEntriesResult.getCurrentTerm(), idOfFollower = appendEntriesResult.getFollowerId();

        TimeStampProto followersTimeStamp = appendEntriesResult.getTimeStamp();

        // Lock for reading and writing shared state
        lock.writeLock().lock();  // Lock to ensure exclusive write access for updating `nextIndex`, `matchIndex`, etc.
        try {

            // updating the clock of leader
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(followersTimeStamp));

            if (termOfFollower > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, termOfFollower));
                // Become follower
                status = ServerCurrentStatus.FOLLOWER;
                startTheElectionTimer();
                return false;
            }

            if (!result) {
                if (nextIndex.get(idOfFollower) >= prevNextIndex) {
                    nextIndex.decrementAndGet(idOfFollower);
                }
            } else {
                // Update matchIndex and nextIndex
                if (matchIndex.get(idOfFollower) < matchIndexOfFollower) {
                    int prevMatchIndex = matchIndex.get(idOfFollower);
                    matchIndex.set(idOfFollower, matchIndexOfFollower);
                    nextIndex.set(idOfFollower, matchIndexOfFollower + 1);

                    int prevCommitIndex = commitIndex.get();
                    int candidateCommitIndex = matchIndex.get(idOfFollower);
                    // Check if we need to update the commitIndex of the leader
                    if (checkIfIndexIsAValidCommitIndex(candidateCommitIndex)) {
                        if (prevCommitIndex != candidateCommitIndex) {
                            commitIndex.updateAndGet(index -> Math.max(index, candidateCommitIndex)); // Update commitIndex

                            // update the majority committed map
                            for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                                updateBalances(log.get(i).t, clientBalancesMajorityCommitted);
                            }

                            System.out.println("The commit index of leader updated to -- " + commitIndex.get());
                            System.out.println("Log size of leader is---" + log.size());

                            // Send acknowledgements from [(prevCommitIndex + 1), commitIndex] if needed
                            List<LogEntry> entriesToBeAck = log.getEntries(prevCommitIndex + 1, commitIndex.get());
                            sendAckForEntries(entriesToBeAck);
                        }
                    }
                }
                checkIfWriteConcernsAreSatisfied(matchIndex.get(idOfFollower), idOfFollower);
            }
        } finally {
            lock.writeLock().unlock(); // Unlock after modifying shared state
        }
        return true;
    }

    // inside lock
    private void checkIfWriteConcernsAreSatisfied(int newMatchIndexOfFollower, int idOfFollower) {
        List<LogEntry> entries = new ArrayList<>();

        // optional check, need to confirm if we this is necessary
        if (log.get(newMatchIndexOfFollower).term != currentTerm.get()) return;

        for (int i = commitIndex.get() + 1; i <= newMatchIndexOfFollower; i++) {
            String id = log.get(i).t.getId();
            if (ackSent.containsKey(id) && !ackSent.get(id)) {
                System.out.println("This follower --" + idOfFollower + "is updating the writeConcern of" + log.get(i).t);
                log.updateWriteConcern(i);
                if (log.get(i).writeConcern == 0) {
                    entries.add(log.get(i));
                }
            }
        }

        // entries array size should be greater than zero
        if (entries.size() > 0) {
            sendAckForEntries(entries);
        }
    }

    private void sendAppendEntries() {
        // send appendEntries

        while (status == ServerCurrentStatus.LEADER) {

            for (int i = 0; i < 5; i++) {

                if (i == serverId) continue;

                int matchIndexForFollower = -1, indexToSendFrom = log.size() - 1;

                AppendEntriesArgument appendEntriesArgument = null;
                lock.readLock().lock();
                try {
                    indexToSendFrom = nextIndex.get(i);
                    LogEntry prevEntry = log.get(indexToSendFrom - 1);
                    List<LogEntryProto> entries = convertLogEntryToProto(log.logEntriesFromIndex(indexToSendFrom));
                    Log l = Log.newBuilder().addAllLog(entries).build();
                    appendEntriesArgument = AppendEntriesArgument.newBuilder().setLeadersTerm(currentTerm.get()).setLeadersId(serverId).setLeadersCommit(commitIndex.get()).setPrevLogIndex(prevEntry.index).setPrevLogTerm(prevEntry.term).setEntriesToAppend(l).setTimeStamp(HybridClock.TimeStamp.convertToProto(hybridClock.now())).build();
                    matchIndexForFollower = entries.size() + indexToSendFrom - 1;
                } finally {
                    lock.readLock().unlock();
                }
                // index to send from
                int matchIndexFollowerTemp = matchIndexForFollower;
                int nextIndexTemp = indexToSendFrom;


                stubs[i].appendEntries(appendEntriesArgument, new StreamObserver<AppendEntriesResult>() {
                    @Override
                    public void onNext(AppendEntriesResult appendEntriesResult) {
                        doesLeaderHasHighestTerm = handleAppendEntriesResult(appendEntriesResult, matchIndexFollowerTemp, nextIndexTemp);

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
        lock.writeLock().lock();  // Acquire the write lock for the entire election process
        try {
            System.out.println(serverId + " is " + "Starting Election" + "Time in milli Seconds" + System.currentTimeMillis());
            // This node becomes a candidate
            this.status = ServerCurrentStatus.CANDIDATE;
            // First update the term
            currentTerm.incrementAndGet();
            // Reset the timer
            startTheElectionTimer();
            // Resetting the votes
            votes.set(0);
            // Vote for self
            votedFor.put(currentTerm.get(), serverId);
            isElectionOver.set(false);
        } finally {
            lock.writeLock().unlock();  // Release the lock after the initial election setup
        }

        requestForVotes();

        // Now that we have finished the election setup, we can check for the election result
        lock.writeLock().lock();  // Acquire the lock to ensure that no other thread modifies the shared state while we transition
        try {
            if (votes.get() >= 2 && status != ServerCurrentStatus.FOLLOWER) {
                doesLeaderHasHighestTerm = true;
                System.out.println(serverId + " " + "Became the leader" + " The term is " + currentTerm.get());
                // Stop the election timer
                electionTimer.stop();
                // Reinitialize state
                reinitialiseIndexes();
                // This node becomes the leader
                this.status = ServerCurrentStatus.LEADER;
                this.currentLeader = this.serverId;
            } else {
                this.status = ServerCurrentStatus.FOLLOWER;
            }
        } finally {
            lock.writeLock().unlock();  // Release the lock after modifying shared state
            // Start sending AppendEntries outside the critical section
            if (votes.get() >= 2 && status == ServerCurrentStatus.LEADER) {
                sendAppendEntries();
            }
        }
    }


    // need to review the logic
    @Override
    public void sendReadRequest(ReadRequest readRequest, StreamObserver<Balance> responseObserver) {
        ReadConcern readConcern = readRequest.getReadConcern();

        String accName = readRequest.getAccName();

        if (readRequest.hasTimeStamp()) {
            // causal consistency
            HybridClock.TimeStamp timeStampRequestedByClient = HybridClock.TimeStamp.convertToTimeStamp(readRequest.getTimeStamp());

            if (!timeStampsInLog.containsKey(timeStampRequestedByClient)) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // I am checking here again because we might wait 30 ms and still not get the timestamp

            if (!timeStampsInLog.containsKey(timeStampRequestedByClient)) {
                // send failure, we wait approximately for 2 appendEntries
                responseObserver.onNext(Balance.newBuilder().setFailure(true).build());
                responseObserver.onCompleted();
            } else {
                // The node was able to catch up till the timestamp and the data will be returned based on the read concern now
                responseObserver.onNext(getBalanceBasedOnReadConcern(readConcern, accName));
                responseObserver.onCompleted();
            }

        } else if (readConcern == ReadConcern.LINEARIZABLE) {
            // this readRequest should go to leader
            // here we check if election is happening or not
            // if election is happening send failure, client can try again

            lock.readLock().lock();
            try {
                if (isElectionTakingPlace()) {
                    // election is taking place so linearizability cannot be guaranteed
                    responseObserver.onNext(Balance.newBuilder().setFailure(true).build());
                    responseObserver.onCompleted();
                } else {
                    if (currentLeader != serverId) {

                        // send request to leader
                        stubs[currentLeader].sendReadRequest(readRequest, new StreamObserver<Balance>() {
                            @Override
                            public void onNext(Balance balance) {
                                responseObserver.onNext(balance);
                            }

                            @Override
                            public void onError(Throwable throwable) {

                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        });
                    } else {
                        // this is the leader so return the Majority committed data
                        responseObserver.onNext(Balance.newBuilder().setBalance(clientBalancesMajorityCommitted.get(accName)).setFailure(false).setAccName(accName).build());
                        responseObserver.onCompleted();
                    }
                }

            } finally {
                lock.readLock().unlock();
            }
        } else {
            responseObserver.onNext(getBalanceBasedOnReadConcern(readConcern, accName));
            responseObserver.onCompleted();
        }
    }

    // inside read lock
    private boolean isElectionTakingPlace() {
        if (status == ServerCurrentStatus.CANDIDATE) return true;
        if (serverId == currentLeader && status == ServerCurrentStatus.FOLLOWER) return true;

        return false;
    }
    private Balance getBalanceBasedOnReadConcern(ReadConcern readConcern, String accName) {

        lock.readLock().lock();
        try {
            if(readConcern == ReadConcern.LOCAL) {
                return Balance.newBuilder().setBalance(clientBalancesLatest.get(accName)).build();
            } else {
                // here readConcern will be majority
                return Balance.newBuilder().setBalance(clientBalancesMajorityCommitted.get(accName)).build();
            }
        } finally {
            lock.readLock().unlock();
        }
    }
    @Override
    public void printLog(Empty request, StreamObserver<Empty> responseObserver) {
        System.out.println("The commit index of this node is -- " + commitIndex.get());
        System.out.println("The size of log is ---" + log.size());
        System.out.println("The majority committed map -- " + clientBalancesMajorityCommitted);
        System.out.println("The latest map -- " + clientBalancesLatest);

//        System.out.println(totalLatency.get());
//        System.out.println("The latency of the system is in ms----" + (totalLatency.get() / (ackTransactionCount.get())));
//        System.out.println("The current clock time of this node is----" + hybridClock.now());
//        log.printLog();
    }
    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }
}
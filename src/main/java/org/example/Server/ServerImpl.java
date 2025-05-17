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

import org.example.TokenBucket.TokenBucketImpl;
import org.example.Utility.*;
import org.example.Utility.ServerStatus.*;
import org.example.TokenBucket.TokenBucketImpl.TokenBucketData;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.prefs.PreferenceChangeEvent;
import java.util.stream.Collectors;

public class ServerImpl extends RaftGrpc.RaftImplBase {
    int NUM_OF_SERVERS;
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

    AtomicBoolean doesLeaderHasHighestTerm;

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

    ReadWriteLock ackLock;

    AtomicInteger totalTransactions;

    // this is used to calculate the throughput of the system
    ConcurrentLinkedQueue<Long> ackTransactionsTimeStamps;

    private final Object systemWideThroughput;

    private final Object writeConcernThroughput;

    private final Object writeConcernLatency;

    ReadWriteLock redisLock;

    ReentrantLock batchLock;
    ReentrantLock electionLock;

    TokenBucketImpl tokenBucket;

    // this helps in avoiding race conditions, this will batch transactions for 20ms
    Queue<ClientMessage> batchOfTransactions;

    // this will execute the batch in every 20 ms
    ScheduledExecutorService batchProcessor;
    private ScheduledFuture<?> batchProcessingTask;

    ConcurrentHashMap<Integer, Double> writeConcernCosts;

    ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Long>> ackTransactionTimeStampsForAllWriteConcerns;

    ConcurrentHashMap<Integer, Long> writeConcernLatencies;

    ConcurrentHashMap<Integer, Double> smoothedLatencies;


    // all the parameters for knapsack
    private static final int BATCH_INTERVAL_MS = 200;
    private boolean backlogExists = false;

//    private static final double COST_W1 = 1;
//    private static final double COST_MAJORITY = 2.0;
    private static final int MIN_REQUIRED_THROUGHPUT = 150; // this is in seconds

    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new ConcurrentHashMap<>();
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(), (new Random().nextInt(400) + 200), TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);
        this.doesLeaderHasHighestTerm = new AtomicBoolean(false);
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
        this.ackLock = new ReentrantReadWriteLock();
        this.NUM_OF_SERVERS = NUM_OF_SERVERS;
        this.stubs = new RaftStub[NUM_OF_SERVERS];
        this.nextIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.matchIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.blockingStubs = new RaftBlockingStub[NUM_OF_SERVERS];
        this.log = new RaftLog(NUM_OF_SERVERS);
        this.totalTransactions = new AtomicInteger(0);
        // this is used to calculate the throughput of the system
        this.ackTransactionsTimeStamps = new ConcurrentLinkedQueue<>();
        this.systemWideThroughput = new Object();
        this.writeConcernThroughput = new Object();
        this.writeConcernLatency = new Object();
        this.redisLock = new ReentrantReadWriteLock();
        this.tokenBucket = new TokenBucketImpl("127.0.0.1", 6379);
        this.batchOfTransactions = new LinkedList<>();
        this.writeConcernCosts = new ConcurrentHashMap<>();
        // only one thread is required because I run a periodic batch job every 20ms
        this.batchProcessor = Executors.newScheduledThreadPool(1);
        this.batchProcessingTask = null;
        // since in the batch we are only writing data, we need only the writeLock
        this.batchLock = new ReentrantLock();
        this.electionLock = new ReentrantLock();
        this.ackTransactionTimeStampsForAllWriteConcerns = new ConcurrentHashMap<>();
        this.writeConcernLatencies = new ConcurrentHashMap<>();
        this.smoothedLatencies = new ConcurrentHashMap<>();

        // setting the peers list
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            //setting up the nextIndex and matchIndex

            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
            // setting up the stubs
            if (i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i + 1)).usePlaintext().build();
                stubs[i] = RaftGrpc.newStub(channel);
                blockingStubs[i] = RaftGrpc.newBlockingStub(channel);
            }

            int majorityLevel = ((NUM_OF_SERVERS / 2) + 1);

            //setting up of the queues for calculating the throughput of each writeConcern
            if(i > 0 && i <= majorityLevel) {
                ackTransactionTimeStampsForAllWriteConcerns.put(i, new ConcurrentLinkedQueue<>());
                // initially we might want to set the write concerns costs as 1.0 but as the throughput is calculated they are adjusted
                writeConcernCosts.put(i,1.0);
                writeConcernLatencies.put(i, (long)0);
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
            // this is added to replicate network call behaviour
            Thread.sleep(new Random().nextInt(20) + 5);
            // update clock of follower using leaders clock, if the follower is behind it can catchup
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(leadersTimeStamp));

            // Check if the leader's term is valid
            if (leadersTerm > currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                becomeFollower();
            }

            if(leadersTerm == currentTerm.get()) {
                currentLeader = leaderId;
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
            log.appendEntries(leadersEntries, serverId);  // Append new entries I also update the writeConcern here because this particular needs to update the writeConcern data on its end

            // updating the latest balances
            for (LogEntryProto logEntry : leadersEntries.getLogList()) {
                // adding the entries in tIdToLogIndex, for quick access to check duplicates from client side
                tIdToLogIndex.put(logEntry.getT().getId(), logEntry.getLogIndex());
                // we need the time stamps in log to provide causal consistency
                timeStampsInLog.put(HybridClock.TimeStamp.convertToTimeStamp(logEntry.getTimeStamp()), logEntry.getLogIndex());
                updateBalances(logEntry.getT(), clientBalancesLatest);
            }

            // Update commit index
            if (leadersCommitIndex > commitIndex.get()) {
                int prevCommitIndex = commitIndex.get();

                // updating the commitIndex of this follower
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

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
                // the term of this follower is updated because it will now vote in this updated term
                currentTerm.updateAndGet(term -> Math.max(term, currentTermOfTheCandidate));
                // the node must become a follower
                becomeFollower();
            }
            // all the necessary conditions to check for denying vote
            if (votedFor.containsKey(this.currentTerm.get()) || (this.currentTerm.get() > currentTermOfTheCandidate) || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
                // reply false here
                isVoteGranted = false;
            }
            if (isVoteGranted) {
                // vote for this term, this ideally can be optimised no need to map
                votedFor.put(currentTerm.get(), candidateId);
                // resetting the election timer
                startTheElectionTimer();
            }

            RequestVoteResult requestVoteResult = RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted).setCurrentTerm(currentTerm.get()).build();
            responseObserver.onNext(requestVoteResult);
            responseObserver.onCompleted();

        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void sendTransaction(ClientMessage clientMessage, StreamObserver<Empty> responseObserver) {
        // check if the current node is leader or not, if not forward request to leader, this might fail if election is going on
        if (serverId != currentLeader) {
            // here I have kept the call blocking for now (with a timeout of 1 second), later we can move it to async,
            blockingStubs[currentLeader].withDeadlineAfter(1, TimeUnit.SECONDS).sendTransaction(clientMessage);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }
        // I send the ack back, to resolve the above blocking call immediately once the message is received
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
        // here I add the transactions in batch
        batchLock.lock();
        try {
            batchOfTransactions.add(clientMessage);
        } finally {
            batchLock.unlock();
        }
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
        lock.readLock().lock();
        try {
            return RequestVoteArguments.newBuilder().setCandidateId(this.serverId).setCandidatesTerm(this.currentTerm.get()).setLastLogTerm(this.getLastLogTerm()).setLastLogIndex(this.getLastLogIndex()).build();
        } finally {
            lock.readLock().unlock();
        }
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
            return true;
        }
        return true;
    }

    private synchronized void endLatchHold(CountDownLatch latch) {
        while (latch.getCount() > 0) {
            latch.countDown();
        }
    }

    private void requestForVotes() {
        // this function uses readLock so it is thread safe
        RequestVoteArguments requestVoteArguments = getRequestVoteArgumentsObject();

        // here I have deducted one because obviously the server requesting for votes, will not be responding to requestVote rpc
        // also we expect response from total servers - 1
        CountDownLatch latch = new CountDownLatch(NUM_OF_SERVERS - 1);

        for (int i = 0; i < NUM_OF_SERVERS; i++) {

            if (i == serverId) continue;


            stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
                @Override
                public void onNext(RequestVoteResult requestVoteResult) {

                    // latch.countDown() is atomic operation
                    // additional not compulsory
                    if(isElectionOver.get()) {
                        latch.countDown();
                        return;
                    }
                    boolean isSuccessful = handleRequestVoteResult(requestVoteResult);
                    // it is not successful when the currentTerm of the leader is not up-to date
                    // I am handling everything in call backs to make raft election thread safe
                    // if isElectionOver was already false then do not change shared data
                    // since compareAndSet is atomic we protect against the double transitions due to race conditions
                    if (!isSuccessful) {
                        // if the current term is less then this node has to become a follower
                        if(isElectionOver.compareAndSet(false, true)) {
                            lock.writeLock().lock();
                            try {
                               becomeFollower();
                            } finally {
                                lock.writeLock().unlock();
                            }
                            endLatchHold(latch);
                        }
                    } else if (votes.get() > (NUM_OF_SERVERS / 2)) {

                        if(isElectionOver.compareAndSet(false, true)) {
                            // majority is reached here, no need to continue the election
                            // I call the becomeLeader() here to make thread safe!
                            becomeLeader();
                            //no need to wait further
                            endLatchHold(latch);
                        }
                    } else {
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    // in case of error as well we kind of want to terminate early
                    latch.countDown();
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
            isElectionOver.compareAndSet(false, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // inside read lock
    private List<LogEntryProto> convertLogEntryToProto(List<LogEntry> entries) {
        List<LogEntryProto> result = new ArrayList<>();

        for (LogEntry entry : entries) {
            result.add(LogEntryProto.newBuilder().setLogIndex(entry.index).setT(entry.t).setTerm(entry.term).addAllServersThatReplicatedThisEntry(entry.serversThatReplicatedThisEntry).setWriteConcern(entry.writeConcern).setTimeStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCopyOfWriteConcern(entry.copyOfWriteConcern).build());
        }
        return result;
    }


    // inside write lock
    private int getCommitIndexIfPossible() {
        // we can sort the array 5*log5 roughly equal to 11.6 so it is fine
        int[] sortedMatchIndex = new int[NUM_OF_SERVERS];
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            sortedMatchIndex[i] = matchIndex.get(i);
        }

        // n*log(n)
        Arrays.sort(sortedMatchIndex);

        int index = NUM_OF_SERVERS - 1;


        while (index >= 0) {
            // traverse through all the indexes which are equal to this current index
            int currentIndex = sortedMatchIndex[index], cnt = 0, val = index;

            // currentIndex can be < 0, that means we don't have matchIndex for this follower
            if (currentIndex == -1) return -1;


            while (index >= 0 && sortedMatchIndex[index] == currentIndex) {
                cnt++;
                index--;
            }

            // (4 - val) is there because the indexes on the right hand side of the current index support this index if not equal to -1
            if ((cnt + ((NUM_OF_SERVERS - 1) - val)) >= (NUM_OF_SERVERS / 2) && log.get(currentIndex).term == currentTerm.get()) {
                // we return because we want the best index (array is sorted), that is the biggest index
                return currentIndex;
            }
        }
        // if no commitIndex is possible
        return -1;
    }

    // should be inside a lock, this method is kind of better because we do not need to sort the matchIndex array and we can get the commit index in roughly O(n) time because usually the log of follower and leader is off by 2-3 entries
    private int getCommitIndexIfPossibleEarlyExitMethod() {
        // Find the max matchIndex
        int maxMatchIndex = -1;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            maxMatchIndex = Math.max(maxMatchIndex, matchIndex.get(i));
        }

        // Try from maxMatchIndex down to 0
        for (int idx = maxMatchIndex; idx > commitIndex.get(); idx--) {
            int count = 0;

            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (matchIndex.get(i) >= idx) {
                    count++;
                }
            }

            // check majority and term
            if (count >= (NUM_OF_SERVERS / 2) && log.get(idx).term == currentTerm.get()) {
                return idx;
            }
        }
        return -1;
    }

    // it is not in log
    private void sendAckForEntries(List<LogEntry> entriesToBeAck, CountDownLatch latch) {

        List<AckMessage> ackMessages = new ArrayList<>();

        // maybe we can acquire a read lock (but to optimise it we can keep this lock specific to the sendAck logic to avoid sending multiple ack
        // this is just about minimising repeated acks, not compulsory to add it, now for the calculation of the metrics this is strictly required

        Long timeStampOfTransaction = System.currentTimeMillis();

        for (LogEntry entry : entriesToBeAck) {
            String id = entry.t.getId();

            // this field is seperate for each thread, so there will be no race conditions for this
            boolean firstAck = false;


            // need to add lock because lot of shared variables are being accessed here
            synchronized (systemWideThroughput) {
                if (ackSent.containsKey(id) && !ackSent.get(id)) {

                    firstAck = true;
                    // marking it as sent, if it fails the client can retry from its end
                    ackSent.put(id, true);
                    // send ack for this entry
//                    System.out.println("sending ack");
//                    System.out.println("The transaction id --" + id + " Replicated to ----" + entry.serversThatReplicatedThisEntry);
                    // here I have implemented the logic of rolling throughput
                    // remove the old transactions from the queue, we maintain a window of 1 seconds
                    // *** this is system wide throughput calculation ***
                    recordThroughput(ackTransactionsTimeStamps, timeStampOfTransaction, true);
                    // this latency is in ms
                    long latency = (timeStampOfTransaction - timeAtWhichTransactionWasReceived.get(id));
                    int currentTotalTransactions = totalTransactions.incrementAndGet();
                    long currentTotalLatency = totalLatency.addAndGet(latency);
//                    System.out.println("Current throughput of the system--" + (double) ((currentTotalTransactions * 1000)) / currentTotalLatency);
                    ackTransactionCount.incrementAndGet();
                    ackMessages.add(AckMessage.newBuilder().setT(entry.t).setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCurrentLeader(serverId).build());

                }
            }
            // if we are sending the ack of this transaction again we do not want to process the writeConcernThroughput
            if(!firstAck) continue;
            // *** this is the calculation of writeConcernThroughput ***
            synchronized (writeConcernThroughput) {
                int writeConcernOfThisTransaction = entry.copyOfWriteConcern;
                ConcurrentLinkedQueue<Long> writeConcernSpecificAckTransactionsTimeStamps = ackTransactionTimeStampsForAllWriteConcerns.get(writeConcernOfThisTransaction);
                if(writeConcernSpecificAckTransactionsTimeStamps != null && timeStampOfTransaction != null) {
                    recordThroughput(writeConcernSpecificAckTransactionsTimeStamps, timeStampOfTransaction, true);
                }
            }
            // *** this is the calculation of writeConcernLatency ***
            synchronized (writeConcernLatency) {
                // System.out.println("This is the writeConcern--" + entry.copyOfWriteConcern +" replication---" + entry.serversThatReplicatedThisEntry);
                int writeConcernOfThisTransaction = entry.copyOfWriteConcern;
               Long arrivalTimeOfThisEntryOnLeader = entry.timeOfArrivalAtLeader;

               // this timeStampOfTransaction is the current time taken at the time of sending ack
               Long currentLatency = (timeStampOfTransaction - arrivalTimeOfThisEntryOnLeader);
               Long prevLatency = writeConcernLatencies.get(writeConcernOfThisTransaction);

               recordLatency(writeConcernOfThisTransaction, currentLatency);

               // taking average of latencies, later on maybe we can use Exponential Moving Average (which more weightage to the recent latencies)
               writeConcernLatencies.put(writeConcernOfThisTransaction, prevLatency == 0 ? currentLatency : ((prevLatency + currentLatency) / 2));

            }

        }

        // this is the case when ack was sent earlier because of lesser writeConcern but now it is being sent again on committing
        if (ackMessages.isEmpty()) {
            latch.countDown();
            return;
        }

        // refresh the context, not sure if this is required need to research a but
        RaftGrpc.RaftStub refreshContextClientStub = clientStub.withDeadlineAfter(2, TimeUnit.SECONDS);
        refreshContextClientStub.sendAckToClient(Ack.newBuilder().addAllAckMessage(ackMessages).build(), new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
//                System.out.println("Error in sending ack--" + throwable);

                // implement retry logic
            }

            @Override
            public void onCompleted() {
                // this where we receive the message done
//                System.out.println("Ack Sent successfully");
                 // not sure if this is required or not
//                latch.countDown();
            }
        });

    }
    private void recordThroughput(ConcurrentLinkedQueue<Long> queue, long timeStampOfTransaction, boolean addTimeStamp) {
        while (!queue.isEmpty() && timeStampOfTransaction - queue.peek() >= 1000L) {
            queue.poll();
        }
        // during processing the batch we do not want to add the timestamp
        if(addTimeStamp)
        queue.add(timeStampOfTransaction);
    }

    // we can optimize the write lock here
    private boolean handleAppendEntriesResult(AppendEntriesResult appendEntriesResult, int matchIndexOfFollower, int prevNextIndex) {
        boolean result = appendEntriesResult.getIsSuccessFull();
        int termOfFollower = appendEntriesResult.getCurrentTerm(), idOfFollower = appendEntriesResult.getFollowerId();

        TimeStampProto followersTimeStamp = appendEntriesResult.getTimeStamp();

        List<LogEntry> committedEntriesAck = new ArrayList<>();
        List<LogEntry> eventualEntriesAck = new ArrayList<>();

        // Lock for reading and writing shared state
        lock.writeLock().lock();  // Lock to ensure exclusive write access for updating `nextIndex`, `matchIndex`, etc.
        try {
            // this makes the function idempotent
            if(status != ServerCurrentStatus.LEADER) {
                return false;
            }
            // updating the clock of leader, if its behind it can catchup
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(followersTimeStamp));

            if (termOfFollower > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, termOfFollower));
                // Become follower
                becomeFollower();
                return false;
            }

            if (!result) {
                // added just to ensure that nextIndex does not decrement twice (maybe it can happen, need to think of this situation again)
                if (nextIndex.get(idOfFollower) >= prevNextIndex) {
                    nextIndex.decrementAndGet(idOfFollower);
                }
            } else {
                // Update matchIndex and nextIndex
                if (matchIndex.get(idOfFollower) < matchIndexOfFollower) {

                    int prevMatchIndex = matchIndex.get(idOfFollower);

                    // updating the nextIndex and matchIndex of the follower
                    matchIndex.set(idOfFollower, matchIndexOfFollower);
                    nextIndex.set(idOfFollower, matchIndexOfFollower + 1);

                    int prevCommitIndex = commitIndex.get();
                    // Check if we need to update the commitIndex of the leader, we get the new commitIndex
//                    int candidateCommitIndex = getCommitIndexIfPossible();
                    int candidateCommitIndex = getCommitIndexIfPossibleEarlyExitMethod();

                    if (candidateCommitIndex > commitIndex.get()) {
                        commitIndex.updateAndGet(index -> Math.max(index, candidateCommitIndex)); // Update commitIndex

                        // update the majority committed map
                        for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                            updateBalances(log.get(i).t, clientBalancesMajorityCommitted);
                        }
//                        System.out.println("The commit index of leader updated to -- " + commitIndex.get());
//                        System.out.println("Log size of leader is---" + log.size());

                        // Send acknowledgements from [(prevCommitIndex + 1), commitIndex] if needed
                        committedEntriesAck = log.getEntries(prevCommitIndex + 1, commitIndex.get());
                    }
                    // the leader see what all entries have been replicated by the replica, and decrements the appendEntries for those
                    eventualEntriesAck = checkIfWriteConcernsAreSatisfied(prevMatchIndex, matchIndex.get(idOfFollower), idOfFollower);
                }
            }
        } finally {
            lock.writeLock().unlock(); // Unlock after modifying shared state

            // sending ack logic is kept outside the lock to reduce the contention
            CountDownLatch latch = new CountDownLatch(2);
            // after releasing the lock maybe I can wait for a certain time till all the acks are sent?
            if (!committedEntriesAck.isEmpty()) {
                sendAckForEntries(committedEntriesAck, latch);
            } else {
                latch.countDown();
            }

            if (!eventualEntriesAck.isEmpty()) {
                sendAckForEntries(eventualEntriesAck, latch);
            } else {
                latch.countDown();
            }

            // now we have to make the thread kind of wait till both the acks are send, obviously we will keep a certain timeout
            try {
                latch.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    // inside write lock
    private List<LogEntry> checkIfWriteConcernsAreSatisfied(int prevMatchIndexOfFollower, int newMatchIndexOfFollower, int idOfFollower) {
        List<LogEntry> entries = new ArrayList<>();

        // optional check, need to confirm if we this is necessary
//        if (log.get(newMatchIndexOfFollower).term != currentTerm.get()) return;

        for (int i = Math.max(commitIndex.get() + 1, prevMatchIndexOfFollower + 1); i <= newMatchIndexOfFollower; i++) {
            String id = log.get(i).t.getId();
            if (ackSent.containsKey(id) && !ackSent.get(id)) {
//                System.out.println("This follower --" + idOfFollower + "is updating the writeConcern of" + log.get(i).t);
                // updateWriteConcern handles all the necessary conditions so that the same node does update the write concern of the same log entry again
                log.updateWriteConcern(i, idOfFollower);
                if (log.get(i).writeConcern == 0) {
                    entries.add(log.get(i));
                }
            }
        }
        return entries;
    }

    private void sendAppendEntries() {
        // send appendEntries

        while (status == ServerCurrentStatus.LEADER) {

            for (int i = 0; i < NUM_OF_SERVERS; i++) {

                if (i == serverId) continue;

                int matchIndexForFollower = -1, indexToSendFrom = log.size() - 1;

                AppendEntriesArgument appendEntriesArgument = null;
                lock.readLock().lock();
                try {
                    // nextIndex tells us the nextIndex from which we need the entries
                    indexToSendFrom = nextIndex.get(i);
                    // prevEntry needed for comparison at the follower end
                    LogEntry prevEntry = log.get(indexToSendFrom - 1);
                    // all entries after this index
                    List<LogEntryProto> entries = convertLogEntryToProto(log.logEntriesFromIndex(indexToSendFrom));
                    // making the proto log object
                    Log l = Log.newBuilder().addAllLog(entries).build();
                    // making appendEntries proto object
                    appendEntriesArgument = AppendEntriesArgument.newBuilder().setLeadersTerm(currentTerm.get()).setLeadersId(serverId).setLeadersCommit(commitIndex.get()).setPrevLogIndex(prevEntry.index).setPrevLogTerm(prevEntry.term).setEntriesToAppend(l).setTimeStamp(HybridClock.TimeStamp.convertToProto(hybridClock.now())).build();
                    // if update of the followers log is successful what will be the new matchIndex of follower
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
                        // this doesLeaderHasHighestTerm tells us if the follower has a higher term than this leader, if it is true then it will become follower
                            doesLeaderHasHighestTerm.compareAndSet(true, handleAppendEntriesResult(appendEntriesResult, matchIndexFollowerTemp, nextIndexTemp));
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
                if (!doesLeaderHasHighestTerm.get()) break;
            }
            if (!doesLeaderHasHighestTerm.get()) break;

            try {
                // in every 15 milliseconds send appendEntries
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    // already in writeLock
    public void reinitialiseIndexes() {

        // we traverse from commitIndex to log end to see if the writeConcern of the entries have been updated by this leader or not, because the log data is sent from the old leader so most likely it will not be updated
        // I have removed this code because we can add it when the follower receives these entries
//        for (int i = (commitIndex.get() + 1); i < log.size(); i++) {
//            LogEntry logEntry = log.get(i);
//            if (!logEntry.serversThatReplicatedThisEntry.get(serverId)) {
//                log.updateWriteConcern(i, serverId);
//            }
//        }
        // reinitialise the nextIndex and matchIndex
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
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
            // the current server votes it self
            votes.set(1);
            // Vote for self
            votedFor.put(currentTerm.get(), serverId);
            isElectionOver.set(false);
        } finally {
            lock.writeLock().unlock();  // Release the lock after the initial election setup
        }
        requestForVotes();
    }
    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            // if it is not candidate that is might have become follower in between in requestVote RPC we do not want to make the node leader in this case
            if(status != ServerCurrentStatus.CANDIDATE) return;
            doesLeaderHasHighestTerm.set(true);
            System.out.println(serverId + " " + "Became the leader" + " The term is " + currentTerm.get());
            // Stop the election timer
            electionTimer.stop();
            // Reinitialize state
            reinitialiseIndexes();
            // This node becomes the leader
            this.status = ServerCurrentStatus.LEADER;
            this.currentLeader = this.serverId;
        } finally {
           lock.writeLock().unlock();
        }
        batchProcessingTask = batchProcessor.scheduleAtFixedRate(this::processBatch, 0, BATCH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        sendAppendEntries();
    }

    // should be inside write lock
    private void becomeFollower() {

        // the status changes to follower
        status = ServerCurrentStatus.FOLLOWER;
        // we have to start the election timer because now it is a follower

        startTheElectionTimer();

        // cancelling the batch job
        if (batchProcessingTask != null && !batchProcessingTask.isCancelled()) {
            batchProcessingTask.cancel(false);  // false = don't interrupt if running
            batchProcessingTask = null;
        }

    }
    
    private void processBatch() {
        // here the logic to process the current batch of transaction will come
        List<ClientMessage> currentBatch = new ArrayList<>();
        // remove and add the current batch of transactions to currentBatch List


        batchLock.lock();
        try {
            currentBatch.addAll(batchOfTransactions);
            batchOfTransactions.clear();
        } finally {
            batchLock.unlock();
        }
        // no need for processing if current batch is empty
        if(currentBatch.isEmpty()) return;

       List<ClientMessage> transactionsToExecute =  handleTokenBucket(currentBatch);

       // we need add back the transactions which we were not able to execute

        // first I create a hashset of all the transactions id which are going to be executed
        // this part can be optimised a bit
        // we can add parallelize this stream if needed
        Set<String> idsOfTransactionsWhichCanBeExecuted = transactionsToExecute.stream()
                .map(cm -> cm.getT().getId())
                .collect(Collectors.toSet());

        int backLog = 0;

        // adding back it in the queue
        batchLock.lock();
        try {
            for(ClientMessage clientMessage : currentBatch) {
                if(!idsOfTransactionsWhichCanBeExecuted.contains(clientMessage.getT().getId())) {
                    backLog++;
                    batchOfTransactions.add(clientMessage);
                }
            }
        } finally {
            batchLock.unlock();
        }

        backlogExists = backLog>0;

        System.out.println("The backlog is--" + backLog);

        // this list is used to ack transactions with w:1
        List<LogEntry> entry = new ArrayList<>();
       // here append all these transactions in the raft log
        lock.writeLock().lock();
        try {

            for(ClientMessage clientMessage : transactionsToExecute) {
                int index = log.size();

                if (clientMessage.hasTimeStamp()) {
                    // updating the clock of leader using the time stamp sent by the client
                    hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(clientMessage.getTimeStamp()));
                }

                Transaction t = clientMessage.getT();
                int writeConcern = clientMessage.getWriteConcern();
                String id = t.getId();

                // we do update the clock of leader using followers
                HybridClock.TimeStamp currentTimeStamp = hybridClock.now();
                // appending the entry in log
                log.append(new LogEntry(index, currentTerm.get(), t, writeConcern, currentTimeStamp, NUM_OF_SERVERS, System.currentTimeMillis()));
                updateBalances(t, clientBalancesLatest);
                // we need this to implement causal consistency
                timeStampsInLog.put(currentTimeStamp, index);
                // since this is in write lock only updated entry will be sent to the followers, as this entire thing is atomic
                log.updateWriteConcern(index, serverId);
                ackSent.put(id, false);
                timeAtWhichTransactionWasReceived.put(id, System.currentTimeMillis());
                // if write concern becomes 0 we will send ack to client
                if (log.get(index).writeConcern == 0) {
                    entry.add(log.get(index));
                }
            }
        } finally {
            lock.writeLock().unlock();
            CountDownLatch latch = new CountDownLatch(1);
            if (!entry.isEmpty()) {
                // this sends Ack for all transactions together
                sendAckForEntries(entry, latch);
            } else {
                latch.countDown();
            }

            try {
                latch.await(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void printAllWriteConernsThroughputAndLatencies() {
        System.out.println("Printing Throughputs");
        for(int i = 1; i <= ((NUM_OF_SERVERS / 2) + 1); i++) {
            System.out.println(getWriteConcernThroughput(i));
        }
        synchronized (writeConcernLatency) {
            System.out.println("Printing Latencies");
            for(int i = 1; i <= ((NUM_OF_SERVERS / 2) + 1); i++) {
                System.out.println(writeConcernLatencies.get(i));
            }
        }
    }

    // 1) Add a fixed-size history buffer for each WC
    private static final int LATENCY_HISTORY_SIZE = 100;
    private final Map<Integer, Deque<Long>> latencyHistory = new ConcurrentHashMap<>();

    // Call this whenever a txn with write-concern wc finishes:
    private void recordLatency(int wc, long latencyMs) {
        Deque<Long> h = latencyHistory
        .computeIfAbsent(wc, __ -> new ArrayDeque<>(LATENCY_HISTORY_SIZE));
        if (h.size() == LATENCY_HISTORY_SIZE) h.removeFirst();
        h.addLast(latencyMs);
    }
    private void adjustTokenCostsBasedOnLatency() {
        final int MIN_COST = 1;
        final double STEP_FACTOR = 0.5;  // half the fastest-latency
    
        // 1) Build per-WC average from your history buffers
        Map<Integer, Double> avgLatency = new HashMap<>();
        for (var e : latencyHistory.entrySet()) {
            Deque<Long> hist = e.getValue();
            double avg = hist.stream()
                             .mapToLong(x -> x)
                             .average()
                             .orElse(0.0);
            avgLatency.put(e.getKey(), avg);
        }
        if (avgLatency.isEmpty()) return;
    
        // 2) Find the fastest non-zero average to serve as baseline
        double minLatency = avgLatency.values().stream()
                                      .filter(l -> l > 0)
                                      .min(Double::compare)
                                      .orElse(1.0);
        // avoid an absurdly small step
        if (minLatency < 1.0) minLatency = 1.0;
    
        // 3) Granularity step: a fraction of the fastest path
        double step = minLatency * STEP_FACTOR;
    
        synchronized (writeConcernLatency) {
            for (var entry : avgLatency.entrySet()) {
                int wc  = entry.getKey();
                double lat = entry.getValue();
    
                // 4) Map latency → integer cost: the slower you are, the more multiples of 'step' you consume
                int tokenCost = (int)Math.ceil(lat / step);
                tokenCost = Math.max(tokenCost, MIN_COST);
    
                writeConcernCosts.put(wc, (double)tokenCost);
                System.out.printf(
                    "[Cost Adjust] WC=%d | avgLatency=%.1fms | step=%.1f → cost=%d%n",
                    wc, lat, step, tokenCost
                );
            }
        }
    }
    
    
    
    
    

// private void adjustTokenCostsBasedOnLatency() {
//     // static cost map for write-concerns 1..5
//     Map<Integer, Double> staticCosts = Map.of(
//         1, 1.0,   // cheapest, minimal consistency
//         2, 1.2,   // small bump
//         3, 1.5,   // moderate
//         4, 2.0,   // stronger
//         5, 3.0    // strongest
//     );

//     // write them into your shared cost table
//     synchronized (writeConcernLatency) {
//         for (Map.Entry<Integer, Double> e : staticCosts.entrySet()) {
//             writeConcernCosts.put(e.getKey(), e.getValue());
//             System.out.printf("[Cost Adjust STATIC] WC=%d → cost=%.2f%n", 
//                               e.getKey(), e.getValue());
//         }
//     }
// }
    



    private List<ClientMessage> handleTokenBucket(List<ClientMessage> currentBatch) {

        redisLock.writeLock().lock();
        try {

            // for testing
//            printAllWriteConernsThroughputAndLatencies();
            adjustTokenCostsBasedOnLatency();

            // current metrics
            double currentTps = getSystemWideThroughput();
            TokenBucketData tb = tokenBucket.getCurrentTokenBucketData();
            double  currentTokens = tb.getTokenCount();
            long lastUpdate = tb.getLastUpdateTime();

            // see if the current throughput is lesser than what is required, if it is then we do not want to upgrade any transaction
            boolean throughputLow = currentTps < MIN_REQUIRED_THROUGHPUT;
            boolean backlogMode = backlogExists;

            int n = currentBatch.size();

            // this tells us the minimum number of transactions that we must process to maintain the throughput, we divide by 1000 as BATCH_INTERVAL_MS is in milli seconds
            int transactionsToBeProcessedToMaintainThreshold = (int)Math.ceil(MIN_REQUIRED_THROUGHPUT * BATCH_INTERVAL_MS / 1000.0);

            // the optimal number of transactions we can process
            int minNumberOfTransactionsToBeProcessed = Math.min(transactionsToBeProcessedToMaintainThreshold, n);

            // I convert the clientMessage protobuf object into java object, so that we can use java functions directly on it
            List<TransactionOption> currentBatchInTransactionOption = TransactionOption.convertToTransactionOption(currentBatch);

            // this is scale converts the cost W:1 and W:Majority into int, because we will be using these costs in our dp (and array indexes cannot be float)
            double scale = 10;

            ProcessResult result;

            if(throughputLow || backlogMode) {
                if (backlogMode){
                    result =  processForThroughput(currentBatchInTransactionOption, currentTokens, n, scale);

                }else{
                    result =  processForThroughput(currentBatchInTransactionOption, currentTokens, minNumberOfTransactionsToBeProcessed, scale);

                }
            } else {
                result = processForProfit(currentBatchInTransactionOption, currentTokens, minNumberOfTransactionsToBeProcessed, scale);
            }
            // updating the token count here (updating in redis)
            tokenBucket.updateTokens((currentTokens - (result.tokensUsed)), lastUpdate);
            System.out.printf(
                    "\uD83D\uDE80 [Batch Result] Profit: %.2f | Current TPS: %.2f | Current Tokens: %.2f | Tokens Used: %.2f | Transactions Upgraded : %d%n",
                    result.profit,
                    currentTps,
                    (currentTokens - (result.tokensUsed)),
                    result.tokensUsed,
                    result.transactionsUpgraded
            );
            return result.messages;
        } finally {
            redisLock.writeLock().unlock();
        }
    }

    // use this when throughput is lower than expected
    private ProcessResult processForThroughput(List<TransactionOption> transactions, double currentTokens, int minTransactions, double scale) {
        // n*log(n)
        Collections.sort(transactions, (a,b)->{
            // if consistency is same select transaction with higher profit
            if(a.minRequiredConsistency == b.minRequiredConsistency) return Double.compare(b.baseProfit, a.baseProfit);

            // select the lower consistency transaction
            return (a.minRequiredConsistency - b.minRequiredConsistency);
        });

        List<ClientMessage> selected = new ArrayList<>();

        double usedTokens = 0.0, profit = 0;

        int index = 0;

        for(TransactionOption t : transactions) {
            int minConsistencyOfTransaction = t.minRequiredConsistency;
            double profitForMinConsistency = t.baseProfit, tokenCostOfTransaction = tokenCost(minConsistencyOfTransaction);

            if(Double.compare(tokenCostOfTransaction + usedTokens, currentTokens) <= 0) {
                usedTokens += tokenCostOfTransaction;
                ClientMessage.Builder cmBuilder = t.clientMessage.toBuilder();
                // I update the writeConcern of the transaction
                cmBuilder.setWriteConcern(minConsistencyOfTransaction);

                profit += profitForMinConsistency;
                selected.add(cmBuilder.build());
                // added this because we do not want the current batch to consume all the tokens, we want to get the required throughput
                // we might want to process more transactions if lets say more w:1 are left?? because w:1 is pretty cheap we can use some tokens for it
                if(selected.size() >= minTransactions && (index + 1) < transactions.size() && transactions.get(index + 1).minRequiredConsistency != 1) return new ProcessResult(selected, (usedTokens ), profit, 0);
            } else {
                // we break here because now the token cost will increase because consistency levels are only going to increase
                break;
            }
            index++;
        }
        // here no transactions are upgraded so I simply pass 0
        return new ProcessResult(selected, usedTokens , profit, 0);
    }


    // all the token costs are stored in a map, we use scale to convert them into integer, as we can't store double in DP
    private double tokenCost(int consistency) {
        return Math.ceil(writeConcernCosts.getOrDefault(consistency, 0.0));
    }

    // use this when throughput is higher or equal to of the expected value
    private ProcessResult processForProfit(List<TransactionOption> transactions, double currentTokens, int minTransactions, double scale) {
        int n = transactions.size();
        int maxTokens = (int) Math.ceil(currentTokens );
        int majorityLevel = (NUM_OF_SERVERS / 2) + 1;

        class State {
            double profit;
            int count;
            int prevT;
            int consistency;
            boolean taken;

            State(double profit, int count, int prevT, int consistency, boolean taken) {
                this.profit = profit;
                this.count = count;
                this.prevT = prevT;
                this.consistency = consistency;
                this.taken = taken;
            }
        }

        // **** we can reduce the complexity of this DP by tuning the batch timing ****
        // complexity of this is (no of transaction in the batch) * (max tokens allowed)

        State[][] dp = new State[n + 1][maxTokens + 1];
        dp[0][0] = new State(0, 0, -1, 0, false);

        int maxExecutedTransactions = 0;

        for (int i = 0; i < n; i++) {
            TransactionOption tx = transactions.get(i);

            for (int t = 0; t <= maxTokens; t++) {
                if (dp[i][t] == null) continue;

                // this the case where we skip the transaction
                if (dp[i + 1][t] == null || dp[i + 1][t].profit < dp[i][t].profit) {
                    dp[i + 1][t] = new State(dp[i][t].profit, dp[i][t].count, t, 0, false);
                }

                // we try all possible writeConcern
                for (int wc : writeConcernCosts.keySet()) {
                    int cost = (int) tokenCost(wc);

                    if (wc < tx.minRequiredConsistency) continue;

                    double profit = tx.baseProfit;

                    if (wc >= majorityLevel) {
                        profit += tx.extraMajorityProfit;
                    } else if (wc > tx.minRequiredConsistency) {
                        profit += tx.extraIntermediateProfit * (wc - tx.minRequiredConsistency);
                    }

                    // obviously it should not exceed the maxTokens limit
                    if (t + cost <= maxTokens) {
                        int nt = t + cost;
                        double newProfit = dp[i][t].profit + profit;
                        int newCount = dp[i][t].count + 1;

                        if (dp[i + 1][nt] == null || dp[i + 1][nt].profit < newProfit) {
                            dp[i + 1][nt] = new State(newProfit, newCount, t, wc, true);
                            // we calculate the max number of transactions we can execute
                            maxExecutedTransactions = Math.max(maxExecutedTransactions, newCount);
                        }
                    }
                }
            }
        }

        double bestProfit = -1;
        int bestT = -1, transactionsUpgraded = 0;

        for (int t = 0; t <= maxTokens; t++) {
            // we use Math.min because the minTransactions could be higher than the number of transactions that we can execute
            if (dp[n][t] != null && dp[n][t].count >= Math.min(maxExecutedTransactions, minTransactions)) {
                if (dp[n][t].profit > bestProfit) {
                    bestProfit = dp[n][t].profit;
                    bestT = t;
                }
            }
        }

        if (bestT == -1) return new ProcessResult(new ArrayList<>(), 0.0, bestProfit, 0);

        List<ClientMessage> result = new ArrayList<>();
        int t = bestT;

        for (int i = n; i >= 1; i--) {
            State s = dp[i][t];
            if (s == null) break;

            if (s.taken) {
                TransactionOption tx = transactions.get(i - 1);
                ClientMessage.Builder builder = tx.clientMessage.toBuilder();
                // set the writeConcern for this particular transaction
                builder.setWriteConcern(s.consistency);

                // check if we have upgraded the consistency of this particular transaction
                if (s.consistency > tx.minRequiredConsistency) {
                    transactionsUpgraded++;
                }
                result.add(builder.build());
            }
            t = s.prevT;
        }

        Collections.reverse(result);
        double tokensUsed = bestT ;

        return new ProcessResult(result, tokensUsed, bestProfit, transactionsUpgraded);
    }


    // this gives me the current rolling throughput, at the time of ack I add the transactions timestamp
    private double getSystemWideThroughput() {
        // this can be accessed while sending ack also so we want to ensure that only thread enters
        synchronized (systemWideThroughput) {
            Long currentTimeStamp = System.currentTimeMillis();
            recordThroughput(ackTransactionsTimeStamps, currentTimeStamp, false);
            return ackTransactionsTimeStamps.size();
        }
    }

    private double getWriteConcernThroughput(int writeConcern) {
        synchronized (writeConcernThroughput) {
            Long currentTimeStamp = System.currentTimeMillis();
            ConcurrentLinkedQueue<Long> writeConcernSpecificTimeStamps = ackTransactionTimeStampsForAllWriteConcerns.get(writeConcern);
            if(writeConcernSpecificTimeStamps != null) {
                recordThroughput(writeConcernSpecificTimeStamps, currentTimeStamp, false);
            }
            return writeConcernSpecificTimeStamps.size();
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
                    // I wait for 30 ms because I send appendEntries in 15 ms so we are kind of considering one missed appendEntry here (we may want to reduce the time here )
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
                // but here also there are two things that, if the readConcern is majority then we need to wait till the commit point passes or equals to the expected log entry
                if (readConcern == ReadConcern.MAJORITY) {
                    int logIndexOfRequestedEntry = timeStampsInLog.get(timeStampRequestedByClient);
                    // the requested logIndex should be safely committed
                    // if the commitIndex is less than the logIndexOfRequestEntry then it is not safely committed till now
                    if (commitIndex.get() < logIndexOfRequestedEntry) {
                        // return error, or we can wait for some more time, this is dependent on our design choice
                        responseObserver.onNext(Balance.newBuilder().setFailure(true).build());
                        responseObserver.onCompleted();
                        return;
                    }
                }
                // here the readConcern will be majority and local, in case of majority the requested timestamp would have been safely committed here
                responseObserver.onNext(getBalanceBasedOnReadConcern(readConcern, accName));
                responseObserver.onCompleted();

            }

        } else if (readConcern == ReadConcern.LINEARIZABLE) {
            // this readRequest should go to leader
            // here we check if election is happening or not
            // if election is happening send failure, client can try again
            // this read lock is required because in isElection we are using shared variables and we do not wait for the rpc call to complete before releasing the lock it is async
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
                        responseObserver.onNext(Balance.newBuilder().setBalance(clientBalancesMajorityCommitted.getOrDefault(accName, 0.0)).setFailure(false).setAccName(accName).build());
                        responseObserver.onCompleted();
                    }
                }

            } finally {
                lock.readLock().unlock();
            }
        } else {
            // here the readConcern is just local
            responseObserver.onNext(getBalanceBasedOnReadConcern(readConcern, accName));
            responseObserver.onCompleted();
        }
    }

    // inside read lock
    private boolean isElectionTakingPlace() {
        if (status == ServerCurrentStatus.CANDIDATE) return true;
        // this condition simply means that the current leader has stepped down because a it saw a server with higher term, and currently it does not know the exact leader, other design shown can be setting the current leader = -1 (which means leader is unknown)
        if (serverId == currentLeader && status == ServerCurrentStatus.FOLLOWER) return true;

        return false;
    }
    private Balance getBalanceBasedOnReadConcern(ReadConcern readConcern, String accName) {

        // we need the readLock here because the raft log might be updated with new entries however the clientBalance might not have been, so this lock keeps the raft log and maps in sync
        lock.readLock().lock();
        try {
            if (readConcern == ReadConcern.LOCAL) {
                return Balance.newBuilder().setBalance(clientBalancesLatest.getOrDefault(accName, 0.0)).build();
            } else {
                // here readConcern will be majority
                return Balance.newBuilder().setBalance(clientBalancesMajorityCommitted.getOrDefault(accName, 0.0)).build();
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

    // this resets and starts the timer again
    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }
}
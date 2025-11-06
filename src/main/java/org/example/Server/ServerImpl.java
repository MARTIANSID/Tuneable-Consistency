package org.example.Server;

import io.grpc.*;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Timer.CustomTimer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
    public AtomicInteger currentTerm;

    // can optimize the votedFor logic
    int votedFor;
    RaftLog log;

    AtomicInteger commitIndex;
    AtomicInteger lastApplied;
    AtomicIntegerArray nextIndex;
    AtomicIntegerArray matchIndex;

    ConcurrentHashMap<Integer, Integer> totalAcks;

    CustomTimer electionTimer;


    int serverId;

    List<RaftStub> peers;
    public RaftStub[] stubs;
    RaftBlockingStub[] blockingStubs;

    AtomicInteger votes;

    public ServerCurrentStatus status;

    AtomicBoolean isElectionOver;

    AtomicBoolean doesLeaderHasHighestTerm;

    int currentLeader;

    AtomicInteger ackIndex;

    ConcurrentHashMap<Integer, Integer> matchIndexCount;

    ConcurrentHashMap<String, Integer> tIdToLogIndex; // id : logIndex

    ConcurrentHashMap<String, Long> timeAtWhichTransactionWasReceived;

    ConcurrentHashMap<String, Double> clientBalancesMajorityCommitted;

    ConcurrentHashMap<String, Double> clientBalancesLatest;

    ConcurrentSkipListMap<HybridClock.TimeStamp, Integer> timeStampsInLog;

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

    private final Object ackUpdateLock;

    private final Object writeConcernThroughput;

    private final Object writeConcernLatency;

    private final Object peerData;

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

    ConcurrentHashMap<Integer, Deque<Latency>> writeConcernLatencies;
    ConcurrentHashMap<Integer, Long> writeConcernLatencySum;
    ConcurrentHashMap<Integer, Double> smoothedLatencies;

    ScheduledExecutorService sendAppendEntriesScheduler;
    ScheduledExecutorService causalReadScheduler;

    long[] lastHeartBeatSent;
    long[] lastIndexSent;


    BatchProcessor transactionBatchProcessor;

    // For tracking incoming transactions per second
    private ConcurrentLinkedQueue<Long> incomingTransactionTimestamps;
    private final Object incomingTransactionLock;
    private AtomicLong lastPrintTime;

    //  backlog transactions
    HashSet<String> backLogTransactions;


    // all the parameters for knapsack
    private static final int BATCH_INTERVAL_MS = 5;

    //    private static final double COST_W1 = 1;
//    private static final double COST_MAJORITY = 2.0;
    private static final int MIN_REQUIRED_THROUGHPUT = 3000; // this is in second

    // this based on the adjustedTokenCosts
    public static final double scale = 1;

    public static final int MIN_COST = 1;
    // it smoothnes noisy measurements like latency samples, we don't overreact to short-term spikes
    private static final double ALPHA = 0.20;  // EWMA smoothing

    // if we increase this then our system will become more sensitive to slow repliase, higher writeConcern's cost will rise faster
    private static final double P95_WEIGHT = 0.30;

    // we allow moderate fluctuations to be ignored
    private static final double CHANGE_THRESH = 0.10;  // 10%

    private static final double MAX_STEP_UP = 1.25;
    private static final double MAX_STEP_DOWN = 0.80;

    /*
    If bucket is fully utilized (U ≈ 1), your factor becomes 1 + 0.5 * 1 = 1.5 → token costs are 50% higher.
    If bucket is mostly idle (U ≈ 0), factor ≈ 1 → no extra penalty. This ensures high-cost WCs are penalized when system is busy.
    */

    private static final double UTIL_K = 0.50;  // utilization price slope

    /*
    If throughput is below target, all token costs increase proportionally → system throttles heavier WCs to preserve throughput.
    If TPS ≥ target, factor = 1 → no penalty.
     */
    private static final double TPS_K = 0.50;  // throughput price slope

    private static final long MIN_UPDATE_PERIOD_MS = 20;  // pacing
    private final Map<Integer, Double> ewmaLatency = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastUpdateAt = new ConcurrentHashMap<>();

    private static final double L_HEALTHY_MS = 30.0;   // healthy latency
    private static final double L_BAD_MS = 1000.0;  // bad latency
    private static final double CONVEX_P = 2.0;    // convexity exponent (>1)

    private static final double HEALTHY_TPS_HEADROOM = 1.30;   // allow 30% above TPS_min at healthy latency
    private static final double TPS_EPSILON = 0.20;   // allow up to +20% over TPS_min budget
    private static final double BUCKET_FRACTION_MAX = 0.95;   // max 6% of bucket per request

    double[] followerReadThroughput;

    double combinedThroughputOfFollowers;

    long lastThroughputSentTime;

    double combinedSystemWideThroughputOnFollower;

    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = -1;
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(), (new Random().nextInt(400) + 2000), TimeUnit.MILLISECONDS);
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
        this.timeStampsInLog = new ConcurrentSkipListMap<>();
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
        this.ackUpdateLock = new Object();
        this.peerData = new Object();
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
        this.writeConcernLatencySum = new ConcurrentHashMap<>();
        this.smoothedLatencies = new ConcurrentHashMap<>();
        this.transactionBatchProcessor = new BatchProcessor(NUM_OF_SERVERS);
        // Initialize incoming transaction tracking
        this.incomingTransactionTimestamps = new ConcurrentLinkedQueue<>();
        this.incomingTransactionLock = new Object();
        this.lastPrintTime = new AtomicLong(System.currentTimeMillis());
        this.sendAppendEntriesScheduler = Executors.newScheduledThreadPool(1);
        this.causalReadScheduler = Executors.newScheduledThreadPool(1);
        this.lastHeartBeatSent = new long[NUM_OF_SERVERS];
        this.lastIndexSent = new long[NUM_OF_SERVERS];
        this.backLogTransactions = new HashSet<>();
        this.followerReadThroughput = new double[NUM_OF_SERVERS];
        this.combinedThroughputOfFollowers = 0;
        this.lastThroughputSentTime = 0;
        // setting the peers list
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            //setting up the nextIndex and matchIndex

            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);

            int majorityLevel = ((NUM_OF_SERVERS / 2) + 1);

            //setting up of the queues for calculating the throughput of each writeConcern
            if (i > 0 && i <= majorityLevel) {
                ackTransactionTimeStampsForAllWriteConcerns.put(i, new ConcurrentLinkedQueue<>());
                // initially we might want to set the write concerns costs as 1.0 but as the throughput is calculated they are adjusted
                writeConcernCosts.put(i, 1.0);
                writeConcernLatencies.put(i, new ArrayDeque<>());
                writeConcernLatencySum.put(i, (long) 0);
            }
        }
        // setting up the client stub
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        clientStub = RaftGrpc.newStub(channel);

        // starting the election timer
        this.electionTimer.start();
    }

    public void setUpStubs() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            // setting up the stubs
            if (i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + (i + 1)).enableRetry().usePlaintext().build();
                stubs[i] = RaftGrpc.newStub(channel);
                blockingStubs[i] = RaftGrpc.newBlockingStub(channel);

            }
        }
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
//            if(serverId == (leaderId + 1) % NUM_OF_SERVERS || serverId == (leaderId - 1 + NUM_OF_SERVERS) % NUM_OF_SERVERS || serverId == (leaderId + 2) % NUM_OF_SERVERS){
//                if(log.size() >= 60000 && log.size() < 120000){
//                    Thread.sleep(40);
//                }
//            }
            // this is added to replicate network call behaviour
//            Thread.sleep(new Random().nextInt(10) + 5);
            // update clock of follower using leaders clock, if the follower is behind it can catchup
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(leadersTimeStamp));

            // Check if the leader's term is valid
            if (leadersTerm > currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                becomeFollower();
            }

            if (leadersTerm == currentTerm.get()) {
                currentLeader = leaderId;
            }

            // If the leader's term is outdated
            if (leadersTerm < currentTerm.get()) {
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get()).setIsSuccessFull(false).setFollowerId(serverId).build());
                responseObserver.onCompleted();
                return;
            }
            // resetting the election timer of the follower
            startTheElectionTimer();

            if (appendEntriesArgument.getSystemThroughputIncluded()) {
                combinedSystemWideThroughputOnFollower = appendEntriesArgument.getSystemThroughput();
            }

            // if log mismatch
            if (!log.checkIfPrevLogIndexHasPrevLogTerm(prevLogIndex, prevLogTerm)) {
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
                String id = logEntry.getT().getId();
                // adding the entries in tIdToLogIndex, for quick access to check duplicates from client side
                tIdToLogIndex.put(id, logEntry.getLogIndex());
                // we need the time stamps in log to provide causal consistency
                timeStampsInLog.put(HybridClock.TimeStamp.convertToTimeStamp(logEntry.getTimeStamp()), logEntry.getLogIndex());
                // updating local balances
                updateBalances(logEntry.getT(), clientBalancesLatest);
                // updating ackSent Map
                ackSent.put(id, false);
            }
            // Update commit index
            if (leadersCommitIndex > commitIndex.get()) {
                int prevCommitIndex = commitIndex.get();

                // updating the commitIndex of this follower
                commitIndex.set(Math.min(leadersCommitIndex, log.size() - 1));
                // update majority committed ap
                for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                    updateBalances(log.get(i).t, clientBalancesMajorityCommitted);
                    // updating the ack sent
                    ackSent.put(log.get(i).t.getId(), true);
                }
            }
            // current time of follower
            HybridClock.TimeStamp currentTimeOfFollower = hybridClock.now();
            // Send success response
            AppendEntriesResult.Builder appendEntriesResultBuilder = AppendEntriesResult.newBuilder().setIsSuccessFull(true).setFollowerId(serverId).setTimeStamp(HybridClock.TimeStamp.convertToProto(currentTimeOfFollower));
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastThroughputSentTime >= 200) {
                lastThroughputSentTime = currentTime;
                appendEntriesResultBuilder.setReadThroughputIncluded(true);
                appendEntriesResultBuilder.setFollowerReadThroughput(getSystemWideThroughput());
            }
            responseObserver.onNext(appendEntriesResultBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // inside write lock
    private void updateBalances(Transaction t, ConcurrentHashMap<String, Double> balances) {
        if (t.getIsReadOnly()) return;

        String sender = t.getSender(), receiver = t.getReceiver(), id = t.getId();

        double amount = t.getAmount();

        balances.put(sender, balances.getOrDefault(sender, 100.0) - amount);
        balances.put(receiver, balances.getOrDefault(receiver, 100.0) + amount);
    }

    // inside write lock
    private void rollbackTillIndex(int logIndex) {
        // remove the entries from the logIndex till the end
        for (int i = logIndex; i < log.size(); i++) {
            Transaction t = log.get(i).t;
            String id = t.getId();

            if (!t.getIsReadOnly()) {
                String sender = t.getSender(), receiver = t.getReceiver();

                double amount = t.getAmount();
                // revert the transaction, and the rollback will be performed in the clientBalancesLatest
                clientBalancesLatest.put(sender, clientBalancesLatest.get(sender) + amount);
                clientBalancesLatest.put(receiver, clientBalancesLatest.get(receiver) - amount);
            }

            // remove the entries from tIdToLogIndex
            tIdToLogIndex.remove(id);
            // remove the timestamps
            timeStampsInLog.remove(log.get(i).timeStamp);
        }
    }

    // in lock
    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogIndex();

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
                votedFor = -1;
                // the node must become a follower
                becomeFollower();
            }
            // all the necessary conditions to check for denying vote
            if (votedFor != -1 || (this.currentTerm.get() > currentTermOfTheCandidate) || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
                // reply false here
                isVoteGranted = false;
            }
            if (isVoteGranted) {
                // vote for this term, this ideally can be optimised no need to map
                votedFor = candidateId;
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
        if (serverId != currentLeader && currentLeader != -1) {
            stubs[currentLeader]
                    .sendTransaction(clientMessage, new StreamObserver<Empty>() {
                        @Override
                        public void onNext(Empty value) {
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseObserver.onError(Status.UNAVAILABLE.withDescription("forward failed: " + t).asRuntimeException());
                            System.err.println("Failed to forward transaction to leader: " + t.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            responseObserver.onNext(Empty.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                    });
            return;
        }
        // I send the ack back, to resolve the above blocking call immediately once the message is received

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();

        // Track incoming transaction rate
        trackIncomingTransaction();

        // here I add the transactions in batch
        batchLock.lock();
        try {
            batchOfTransactions.add(clientMessage);
//            System.out.println(batchOfTransactions.size() + "This is the batchSize at the leader end");
        } finally {
            batchLock.unlock();
        }
    }

    /**
     * Tracks incoming transactions and prints the rate per second
     */
    private void trackIncomingTransaction() {
        long currentTime = System.currentTimeMillis();

        synchronized (incomingTransactionLock) {
            // Add current transaction timestamp
            incomingTransactionTimestamps.add(currentTime);

            // Remove timestamps older than 1 second
            while (!incomingTransactionTimestamps.isEmpty() &&
                    currentTime - incomingTransactionTimestamps.peek() >= 1000L) {
                incomingTransactionTimestamps.poll();
            }

            // Print every second (with a small buffer to avoid too frequent prints)
            long lastPrint = lastPrintTime.get();
            if (currentTime - lastPrint >= 1000L && lastPrintTime.compareAndSet(lastPrint, currentTime)) {
                int incomingTPS = incomingTransactionTimestamps.size();
                System.out.printf(
                        "📥 [Incoming Transactions] Server %d | Transactions/sec: %d | Time: %s%n",
                        serverId,
                        incomingTPS,
                        new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(currentTime))
                );
            }
        }
    }

    public int getLastLogIndex() {
        if (log.size() == 0) {
            return -1;
        } else {
            return log.getLastLogEntry().index;
        }
    }

    public int getLastLogTerm() {
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
    public boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if (requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            // making this thread safe and the term is only updated to max value
            currentTerm.updateAndGet(term -> Math.max(term, requestVoteResult.getCurrentTerm()));
            return false;
        }
        if (requestVoteResult.getIsVoteGranted()) {
            // vote granted
            votes.incrementAndGet();
        }
        // if vote is not granted then I decrease the latchCount but do not increase the vote count
        return true;
    }

    private void requestForVotes(RequestVoteArguments requestVoteArguments) {
        // here I have deducted one because obviously the server requesting for votes, will not be responding to requestVote rpc
        // also we expect response from total servers - 1
        CountDownLatch latch = new CountDownLatch(NUM_OF_SERVERS - 1);

        for (int i = 0; i < NUM_OF_SERVERS; i++) {

            if (i == serverId) continue;

            stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
                @Override
                public void onNext(RequestVoteResult requestVoteResult) {
                    boolean shouldBecomeLeader = false;
                    boolean shouldBecomeFollower = false;
                    try {
                        lock.writeLock().lock();

                        if (isElectionOver.get()) {
                            latch.countDown();
                            return;
                        }
                        boolean isSuccessful = handleRequestVoteResult(requestVoteResult);
                        // it is not successful when the currentTerm of the leader is not up-to date
                        // I am handling everything in call backs to make raft election thread safe
                        // if isElectionOver was already false then do not change shared data
                        // since compareAndSet is atomic we protect against the double transitions due to race conditions
                        if (!isSuccessful) {
                            // if the current term is less than this node has to become a follower
                            if (isElectionOver.compareAndSet(false, true)) {
                                shouldBecomeFollower = true;
                                // not waiting further
                            }
                        } else if (votes.get() > (NUM_OF_SERVERS / 2)) {

                            if (isElectionOver.compareAndSet(false, true)) {
                                // majority is reached here, no need to continue the election
                                // I call the becomeLeader() here to make thread safe!
                                shouldBecomeLeader = true;
                            }
                        } else {
                            latch.countDown();
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                    if (shouldBecomeLeader) {
                        becomeLeader();
                        while (latch.getCount() > 0) latch.countDown();
                    } else if (shouldBecomeFollower) {
                        becomeFollower();
                        while (latch.getCount() > 0) latch.countDown();
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
            boolean success = latch.await(400, TimeUnit.MILLISECONDS);
            // if election timed out then we do the below
            try {
                lock.writeLock().lock();
                if (!success && isElectionOver.compareAndSet(false, true) && status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            } finally {
                lock.writeLock().unlock();
            }

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
    private CompletableFuture<Void> sendAckForEntries(List<LogEntry> entriesToBeAck) {
        System.out.println("Preparing to send Ack for entries: " + entriesToBeAck);
        List<AckMessage> ackMessages = new ArrayList<>();

        // maybe we can acquire a read lock (but to optimise it we can keep this lock specific to the sendAck logic to avoid sending multiple ack
        // this is just about minimising repeated acks, not compulsory to add it, now for the calculation of the metrics this is strictly required

        Long timeStampOfTransaction = System.currentTimeMillis();

        for (LogEntry entry : entriesToBeAck) {
            String id = entry.t.getId();

            // this field is seperate for each thread, so there will be no race conditions for this
            boolean firstAck = false;

            // need to add lock because lot of shared variables are being accessed here
            synchronized (ackUpdateLock) {
                if (ackSent.containsKey(id) && !ackSent.get(id)) {
                    firstAck = true;
                    // marking it as sent, if it fails the client can retry from its end
                    ackSent.put(id, true);
                    // send ack for this entry
                    ackTransactionCount.incrementAndGet();
                    if (entry.t.getIsReadOnly()) {
                        ackMessages.add(AckMessage.newBuilder().setT(entry.t).setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCurrentLeader(serverId).setId(id).setBalance(clientBalancesMajorityCommitted.getOrDefault(entry.t.getAccNameToRead(), 0.0)).build());
                    } else {
                        ackMessages.add(AckMessage.newBuilder().setT(entry.t).setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCurrentLeader(serverId).build());
                    }

                }
            }

            // if we are sending the ack of this transaction again we do not want to process the writeConcernThroughput
            if (!firstAck) continue;
            synchronized (systemWideThroughput) {
                recordThroughput(ackTransactionsTimeStamps, timeStampOfTransaction, true);
            }
            // *** this is the calculation of writeConcernLatency ***
            synchronized (writeConcernLatency) {
//                System.out.println("This is the writeConcern--" + entry.copyOfWriteConcern +" replication---" + entry.serversThatReplicatedThisEntry);
                int writeConcernOfThisTransaction = entry.copyOfWriteConcern;
                Long arrivalTimeOfThisEntryOnLeader = entry.timeOfArrivalAtLeader;
                // this timeStampOfTransaction is the current time taken at the time of sending ack
                Long currentLatency = (timeStampOfTransaction - arrivalTimeOfThisEntryOnLeader);
                Deque<Latency> latencies = writeConcernLatencies.get(writeConcernOfThisTransaction);
                while (!latencies.isEmpty() && (timeStampOfTransaction - latencies.peek().timestamp) >= 1000L) {
                    Latency latency = latencies.poll();
                    writeConcernLatencySum.put(writeConcernOfThisTransaction, writeConcernLatencySum.get(writeConcernOfThisTransaction) - latency.latency);
                }
                latencies.add(new Latency(timeStampOfTransaction, currentLatency));
                writeConcernLatencySum.put(writeConcernOfThisTransaction, writeConcernLatencySum.get(writeConcernOfThisTransaction) + currentLatency);

            }
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        // this is the case when ack was sent earlier because of lesser writeConcern but now it is being sent again on committing
        if (ackMessages.isEmpty()) {
            future.complete(null);
            return future;
        }
        System.out.println(ackMessages);
        System.out.println("Sending Ack!!");
        // refresh the context, not sure if this is required need to research a but
        RaftGrpc.RaftStub refreshContextClientStub = clientStub.withDeadlineAfter(2, TimeUnit.SECONDS);
        refreshContextClientStub.sendAckToClient(Ack.newBuilder().addAllAckMessage(ackMessages).build(), new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {
                System.out.println("Ack RPC onNext Recevied");
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                future.complete(null);
            }
        });
        return future;
    }

    public long getAverageLatency(int writeConcern) {
        synchronized (writeConcernLatency) {
            long val = writeConcernLatencySum.get(writeConcern) / Math.max(writeConcernLatencies.get(writeConcern).size(), 1);
            try (FileWriter fw = new FileWriter("avg_latencies.csv", true);
                 PrintWriter out = new PrintWriter(fw)) {
                out.printf("%d,%d%n", writeConcern, val);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return val;
        }
    }

    @Override
    public void sendAckToAllServers(Ack ack, StreamObserver<Empty> responseObserver) {
        List<AckMessage> ackMessages = ack.getAckMessageList();

    }

    // inside lock
    private void recordThroughput(ConcurrentLinkedQueue<Long> queue, long timeStampOfTransaction, boolean addTimeStamp) {
        while (!queue.isEmpty() && timeStampOfTransaction - queue.peek() >= 5000L) {
            queue.poll();
        }
        // during processing the batch we do not want to add the timestamp
        if (addTimeStamp)
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
            if (status != ServerCurrentStatus.LEADER) {
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
            // updating the throughput of follower
            if (appendEntriesResult.getReadThroughputIncluded()) {
                combinedThroughputOfFollowers = combinedThroughputOfFollowers - followerReadThroughput[idOfFollower] + appendEntriesResult.getFollowerReadThroughput();
                followerReadThroughput[idOfFollower] = appendEntriesResult.getFollowerReadThroughput();
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
                        System.out.println(commitIndex.get() + "This is the new commit index---");
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
            // after releasing the lock maybe I can wait for a certain time till all the acks are sent?
            if (!committedEntriesAck.isEmpty()) {
                sendAckForEntries(committedEntriesAck).orTimeout(100, TimeUnit.MILLISECONDS).exceptionally((ex -> {
//                    System.out.println("Ack failed reason: " + ex);
                    return null;
                }));
            }

            if (!eventualEntriesAck.isEmpty()) {
                sendAckForEntries(eventualEntriesAck).orTimeout(100, TimeUnit.MILLISECONDS).exceptionally((ex -> {
//                    System.out.println("Ack failed reason: " + ex);
                    return null;
                }));
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
            if (log.get(i).writeConcern != 0) {
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

        sendAppendEntriesScheduler.scheduleAtFixedRate(() -> {
            if (status != ServerCurrentStatus.LEADER) {
                return;
            }
            for (int i = 0; i < NUM_OF_SERVERS; i++) {

                if (i == serverId) continue;

                int matchIndexForFollower = -1, indexToSendFrom = log.size() - 1;

                AppendEntriesArgument appendEntriesArgument = null;

                lock.readLock().lock();
                try {
                    long now = System.currentTimeMillis();
                    if ((nextIndex.get(i) == log.size() - 1) && ((now - lastHeartBeatSent[i]) < 100) && (lastIndexSent[i] == nextIndex.get(i)))
                        continue;
                    // this check is to avoid race condition where in if leader, becomes follower it should not send the updated term to follower as this old leader is not a leader in the updated term
                    if (status != ServerCurrentStatus.LEADER) return;
                    // nextIndex tells us the nextIndex from which we need the entries
                    indexToSendFrom = nextIndex.get(i);
                    // prevEntry needed for comparison at the follower end
                    LogEntry prevEntry = log.get(indexToSendFrom - 1);
                    // all entries after this index
                    List<LogEntryProto> entries = convertLogEntryToProto(log.logEntriesFromIndex(indexToSendFrom));
                    // making the proto log object
                    Log l = Log.newBuilder().addAllLog(entries).build();
                    // making appendEntries proto object
                    double combinedSystemThroughput = 0;
                    boolean throughputIndcluded = false;
                    if (now - lastThroughputSentTime >= 200) {
                        lastThroughputSentTime = now;
                        combinedSystemThroughput = combinedThroughputOfFollowers + getSystemWideThroughput();
                        throughputIndcluded = true;
                    }
                    appendEntriesArgument = AppendEntriesArgument.newBuilder().setLeadersTerm(currentTerm.get()).setLeadersId(serverId).setLeadersCommit(commitIndex.get()).setPrevLogIndex(prevEntry.index).setPrevLogTerm(prevEntry.term).setEntriesToAppend(l).setTimeStamp(HybridClock.TimeStamp.convertToProto(hybridClock.now())).setSystemThroughputIncluded(throughputIndcluded).setSystemThroughput(combinedSystemThroughput).build();

                    // if update of the followers log is successful what will be the new matchIndex of follower
                    matchIndexForFollower = entries.size() + indexToSendFrom - 1;
                    lastHeartBeatSent[i] = System.currentTimeMillis();
                    lastIndexSent[i] = nextIndex.get(i);
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
        }, 0, 80, TimeUnit.MILLISECONDS);
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
        lock.writeLock().lock();// Acquire the write lock for the entire election process
        RequestVoteArguments requestVoteArguments;
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
            votedFor = serverId;
            isElectionOver.set(false);
            requestVoteArguments = getRequestVoteArgumentsObject();
        } finally {
            lock.writeLock().unlock();  // Release the lock after the initial election setup
        }
        requestForVotes(requestVoteArguments);
    }

    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            // if it is not candidate that is might have become follower in between in requestVote RPC we do not want to make the node leader in this case
            if (status != ServerCurrentStatus.CANDIDATE) return;
            doesLeaderHasHighestTerm.set(true);
            System.out.println(serverId + " " + "Became the leader" + " The term is " + currentTerm.get());
            // Stop the election timer
            electionTimer.stop();
            // Reinitialize state
            reinitialiseIndexes();
            // starting the sendAppendEntriesScheduler
            // This node becomes the leader
            this.status = ServerCurrentStatus.LEADER;
            this.currentLeader = this.serverId;
            if (sendAppendEntriesScheduler == null || sendAppendEntriesScheduler.isShutdown()) {
                sendAppendEntriesScheduler = Executors.newScheduledThreadPool(1);
            }
            if (batchProcessingTask == null || batchProcessor.isShutdown()) {
                batchProcessingTask = batchProcessor.scheduleAtFixedRate(this::processBatch, 0, BATCH_INTERVAL_MS, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.writeLock().unlock();
        }
        sendAppendEntries();
    }

    // should be inside write lock
    private void becomeFollower() {

        if (status == ServerCurrentStatus.LEADER) {
            startTheElectionTimer();
        }
        // the status changes to follower
        status = ServerCurrentStatus.FOLLOWER;
        // we have to start the election timer because now it is a follower

        // cancelling the batch job
        if (batchProcessingTask != null && !batchProcessingTask.isCancelled()) {
            batchProcessingTask.cancel(false);  // false = don't interrupt if running
            batchProcessingTask = null;
        }
        // stop leader’s heartbeat task immediately
        if (sendAppendEntriesScheduler != null && !sendAppendEntriesScheduler.isShutdown()) {
            sendAppendEntriesScheduler.shutdownNow();
            sendAppendEntriesScheduler = null;
        }
    }

    private void processBatch() {
        // here the logic to process the current batch of transaction will come
        List<ClientMessage> currentBatch = new ArrayList<>();
        // remove and add the current batch of transactions to currentBatch List
        int backLog = 0;
        batchLock.lock();
        try {
            currentBatch.addAll(batchOfTransactions);
            if (currentBatch.size() > 0)
                System.out.println(currentBatch.size() + " This is the batch size being processed at leader ");
            batchOfTransactions.clear();
            backLog = backLogTransactions.size();
        } finally {
            batchLock.unlock();
        }

        // no need for processing if current batch is empty
        if (currentBatch.isEmpty()) return;

        List<ClientMessage> transactionsToExecute = handleTokenBucket(currentBatch, backLog);
        System.out.println(transactionsToExecute + " This is the number of transactions which can be executed in this batch ");
        // first I create a hashset of all the transactions id which are going to be executed
        // this part can be optimised a bit
//        // we can add parallelize this stream if needed
        Set<String> idsOfTransactionsWhichCanBeExecuted = transactionsToExecute.stream()
                .map(cm -> cm.getT().getId())
                .collect(Collectors.toSet());

//        // adding back it in the queue
        batchLock.lock();
        try {
            for (ClientMessage clientMessage : currentBatch) {
                String transactionId = clientMessage.getT().getId();
                if (!idsOfTransactionsWhichCanBeExecuted.contains(clientMessage.getT().getId())) {
                    backLogTransactions.add(transactionId);
                    batchOfTransactions.add(clientMessage);
                } else {
                    // it can be executed
                    backLogTransactions.remove(transactionId);
                }
            }
            backLog = backLogTransactions.size();
            try (FileWriter fw = new FileWriter("backlog.csv", true);
                 PrintWriter out = new PrintWriter(fw)) {

                File file = new File("backlog.csv");
                if (file.length() == 0) {
                    out.println("Backlog");
                }
                out.printf("%d%n", backLog);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("The backlog is--" + backLog);
        } finally {
            batchLock.unlock();
        }
////
//        // this list is used to ack transactions with w:1
        List<LogEntry> entry = new ArrayList<>();

        // here append all these transactions in the raft log
        lock.writeLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER) return;

            for (ClientMessage clientMessage : transactionsToExecute) {
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
            if (!entry.isEmpty()) {
                // this sends Ack for all transactions together
                sendAckForEntries(entry).orTimeout(100, TimeUnit.MILLISECONDS).exceptionally(ex -> {
//                    System.out.println("Ack failed reason : " + ex);
                    return null;
                });
            }
        }
    }

    private void printAllWriteConernsThroughputAndLatencies() {
        System.out.println("Printing Throughputs");
        for (int i = 1; i <= ((NUM_OF_SERVERS / 2) + 1); i++) {
            System.out.println(getWriteConcernThroughput(i));
        }
        synchronized (writeConcernLatency) {
            System.out.println("Printing Latencies");
            for (int i = 1; i <= ((NUM_OF_SERVERS / 2) + 1); i++) {
                System.out.println(writeConcernLatencies.get(i));
            }
        }
    }
//    private void adjustTokenCostsBasedOnLatency() {
//        final int MIN_COST = 1;
//        final double TOKEN_CAPACITY = tokenBucket.getMaxTokens();
//        int minTransactions = (int)Math.ceil(MIN_REQUIRED_THROUGHPUT * BATCH_INTERVAL_MS / 1000.0);
//        double tuningFactor = 2.0;
//        final double maxTokenCostPerTxn = TOKEN_CAPACITY / (minTransactions * tuningFactor);
//
//        synchronized (writeConcernLatency) {
//            // Filter out zero latencies and get max
//            long maxLatency = writeConcernLatencies.values().stream()
//                    .mapToLong(l -> l)
//                    .max()
//                    .orElse(1L); // avoid divide by 0
//
//            // Sort write concerns to enforce cost hierarchy
//            List<Integer> writeConcerns = new ArrayList<>(writeConcernLatencies.keySet());
//            Collections.sort(writeConcerns);
//
//
//            double prevCost = MIN_COST;
//            for (int wc : writeConcerns) {
//                long latency = writeConcernLatencies.get(wc);
//                // Apply EMA smoothing (optional)
//                double smoothedLatency = smoothedLatencies.compute(wc,
//                        (k, oldVal) -> oldVal == null ? latency : 0.2 * latency + 0.8 * oldVal);
//
//                // Scale cost proportionally to latency
//                double scaledCost = (smoothedLatency / maxLatency) * maxTokenCostPerTxn;
//                int tokenCost = (int) Math.ceil(scaledCost * scale);
//                tokenCost = Math.max((int) prevCost, tokenCost);  // Enforce hierarchy
//
//                writeConcernCosts.put(wc, (double) tokenCost);
//                prevCost = tokenCost;
//                System.out.printf("[Cost Adjust] WC=%d | Latency=%dms | Cost=%d%n",
//                        wc, latency, tokenCost);
//            }
//        }
//    }

//    private void adjustTokenCostsBasedOnLatency() {
//        final int MIN_COST = 1;
//        final int MAX_COST = 50;
//        final int HEALTHY_LATENCY = 20;
//        final int MAX_LATENCY = 50;
//        final double TOKEN_CAPACITY = tokenBucket.getMaxTokens();
//
//        // Throughput sanity check
//        int minTransactionsPerBatch = (int) Math.ceil(MIN_REQUIRED_THROUGHPUT * BATCH_INTERVAL_MS / 1000.0);
//        if (MAX_COST * minTransactionsPerBatch > TOKEN_CAPACITY) {
//            throw new IllegalStateException("Token capacity too low for minThroughput!");
//        }
//
//        synchronized (writeConcernLatencies) {
//            // Sort write concerns to enforce hierarchy
//            List<Integer> writeConcerns = new ArrayList<>(writeConcernLatencies.keySet());
//            Collections.sort(writeConcerns);
//
//            int prevCost = MIN_COST;
//            for (int wc : writeConcerns) {
//                long latency = writeConcernLatencies.get(wc);
//
//                // Compute base cost (Step 1)
//                double normalizedLatency = Math.min(1.0,
//                        Math.max(0.0, (double) (latency - HEALTHY_LATENCY) / (MAX_LATENCY - HEALTHY_LATENCY)));
//                int tokenCost = MIN_COST + (int) Math.ceil(normalizedLatency * (MAX_COST - MIN_COST));
//                tokenCost = Math.min(MAX_COST, Math.max(MIN_COST, tokenCost));
//
//                // Enforce hierarchy (Step 2)
//                tokenCost = Math.max(prevCost, tokenCost);
//                writeConcernCosts.put(wc, (double) tokenCost);
//                prevCost = tokenCost;
//
//                System.out.printf("WC=%d | Latency=%dms | Cost=%d%n", wc, latency, tokenCost);
//            }
//        }
//    }

    //    private void adjustTokenCostsBasedOnLatency() {
//        final int MIN_COST = 1;
//        final double STEP_FACTOR = 0.3;  // half the fastest-latency
//
//        // 1) Build per-WC average from your history buffers
//        HashMap<Integer, Long> averageLatency = new HashMap<>();
//        synchronized (writeConcernLatency) {
//            double minLatency = 1.0;
//            for (var entry : writeConcernLatencies.entrySet()) {
//                int wc = entry.getKey();
//                Long latency = getAverageLatency(wc);
//                minLatency = Math.max(latency, minLatency);
//                averageLatency.put(wc, Math.max(latency, 1));
//            }
//
//
//            double step = minLatency * STEP_FACTOR;
//
//            try (FileWriter csvWriter = new FileWriter("token_costs.csv", true)) {
//                for (var entry : averageLatency.entrySet()) {
//                    int wc = entry.getKey();
//                    Long lat = entry.getValue();
//
//                    double tokenCost = Math.ceil((double) lat / step);
//                    tokenCost = Math.max(tokenCost, MIN_COST);
//
//                    writeConcernCosts.put(wc, tokenCost);
//
//                    // Print to console
//                    System.out.printf(
//                            "[Cost Adjust] WC=%d | avgLatency=%dms | step=%.1f → cost=%.1f%n",
//                            wc, lat, step, tokenCost
//                    );
//
//                    // Append WC, Latency, TokenCost to CSV
//                    csvWriter.write(String.format("%d,%d,%.1f%n", wc, lat, tokenCost));
//                }
//
//                csvWriter.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//    }
    private double blendedLatencyForWC(int wc) {
        // it is a 1 second window based latency so it works well directly
        return getAverageLatency(wc);
    }

    // Map a smoothed latency to an integer cost via convex anchor curve and guardrails
    private double latencyToCost(double ewmaMs) {
        double tpsMin = Math.max(1.0, MIN_REQUIRED_THROUGHPUT);

        double costHealthy = tokenBucket.getRefillRate() / (tpsMin * HEALTHY_TPS_HEADROOM);

        double costBad = BUCKET_FRACTION_MAX * tokenBucket.getMaxTokens();

        double Lmin = Math.max(1.0, 0.25 * L_HEALTHY_MS);
        if (ewmaMs <= L_HEALTHY_MS) {
            double denom = Math.max(1e-9, L_HEALTHY_MS - Lmin);
            double z = (ewmaMs - Lmin) / denom;
            z = Math.max(0.0, Math.min(1.0, z));
            double y = 1.0 * (1.0 - Math.pow(z, CONVEX_P))
                    + costHealthy * Math.pow(z, CONVEX_P);
            return Math.max(MIN_COST, y);
        }

        double x = ewmaMs >= L_BAD_MS ? 1.0 : (ewmaMs - L_HEALTHY_MS) / (L_BAD_MS - L_HEALTHY_MS);
        double blendedCost = costHealthy * (1.0 - Math.pow(x, CONVEX_P))
                + costBad * Math.pow(x, CONVEX_P);

        return Math.max(MIN_COST, blendedCost);
    }

    // Main adjustment; currentTps provided by caller's sliding window
    private void adjustTokenCostsBasedOnLatency(double currentTps) {
        final double MIN_STEP_EPS = 0.05;

        long now = System.currentTimeMillis();

        synchronized (writeConcernLatency) {
            Long lastAny = lastUpdateAt.values().stream().findAny().orElse(0L);
            if (now - lastAny < MIN_UPDATE_PERIOD_MS) return;
            Map<Integer, Double> smoothed = new HashMap<>();
            double minSmoothed = Double.POSITIVE_INFINITY;

            double prevWriteConcernCost = 1;
            for (int i = 1; i <= (NUM_OF_SERVERS / 2 + 1); i++) {
                int wc = i;
                double blended = Math.max(1.0, blendedLatencyForWC(wc));
                double prev = ewmaLatency.getOrDefault(wc, blended);
                double ewma = ALPHA * blended + (1.0 - ALPHA) * prev;
                ewmaLatency.put(wc, ewma);
                smoothed.put(wc, ewma);
                if (ewma < minSmoothed) minSmoothed = ewma;
            }
            if (!Double.isFinite(minSmoothed) || minSmoothed <= 0.0) minSmoothed = 1.0;

            System.out.println(writeConcernCosts);
            System.out.println(ewmaLatency);

            try (FileWriter csv = new FileWriter("token_costs.csv", true)) {
                for (var e : smoothed.entrySet()) {
                    int wc = e.getKey();
                    double ewmaMs = e.getValue();

                    double oldCost = writeConcernCosts.getOrDefault(wc, (double) MIN_COST);
                    double proposed = latencyToCost(ewmaMs);

                    try (FileWriter fw = new FileWriter("writeconcern.csv", true);
                         PrintWriter out = new PrintWriter(fw)) {
                        File file = new File("writeconcern.csv");
                        if (file.length() == 0) {
                            out.println("WriteConcern,Cost,Latency");
                        }
                        for (Integer level : writeConcernCosts.keySet()) {
                            double cost = writeConcernCosts.get(level);
                            double latency = smoothed.getOrDefault(level, Double.NaN);
                            out.printf("%d,%.4f,%.2f%n", level, cost, latency);
                        }
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }

                    double upCap = oldCost * (1.0 + MAX_STEP_UP);
                    double downCap = oldCost * (1.0 - MAX_STEP_DOWN);
                    double capped = proposed;

                    if (proposed > oldCost) {
                        double minUp = oldCost + MIN_STEP_EPS;
                        capped = Math.min(proposed, Math.max(minUp, upCap));
                    } else if (proposed < oldCost) {
                        double maxDownAbs = oldCost - MIN_STEP_EPS;
                        capped = Math.max(proposed, Math.min(maxDownAbs, downCap));
                    }

                    capped = Math.max(MIN_COST, capped);
                    capped = Math.max(prevWriteConcernCost, capped);
                    prevWriteConcernCost = capped;
                    writeConcernCosts.put(wc, capped);

                    csv.write(String.format(
                            "%d,%.3f,%.3f,%.3f,%.3f,%.3f,%d%n",
                            wc, ewmaMs, minSmoothed, oldCost, proposed, capped, now
                    ));
                    lastUpdateAt.put(wc, now);
                }
                csv.flush();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


    private List<ClientMessage> handleTokenBucket(List<ClientMessage> currentBatch, int backLog) {
        // we can use lua scripts instead of using lock at application level
        redisLock.writeLock().lock();
        try {

            // current metrics
            double currentTps = getSystemWideThroughput();
            adjustTokenCostsBasedOnLatency(currentTps);
            TokenBucketData tb = tokenBucket.getCurrentTokenBucketData();
            double currentTokens = tb.getTokenCount();
            long lastUpdate = tb.getLastUpdateTime();

            int n = currentBatch.size();

            // I convert the clientMessage protobuf object into java object, so that we can use java functions directly on it
            List<TransactionOption> currentBatchInTransactionOption = TransactionOption.convertToTransactionOption(currentBatch);

            ProcessResult result;

            // Using the new hybrid approach: process all transactions first, then upgrade for profit
            result = transactionBatchProcessor.processTransactions(currentBatchInTransactionOption, currentTokens, (currentTps >= MIN_REQUIRED_THROUGHPUT || backLog > 0), new HashMap<>(writeConcernCosts));

            // updating the token count here (updating in redis)
            tokenBucket.updateTokens((currentTokens - (result.tokensUsed)), lastUpdate);
            System.out.printf(
                    "\uD83D\uDE80 [Batch Result] Profit: %.2f | Current TPS: %.2f | Current Tokens: %.2f | Tokens Used: %.2f | Transactions Upgraded : %d%n",
                    result.profit,
                    currentTps,
                    currentTokens,
                    result.tokensUsed,
                    result.transactionsUpgraded
            );
            try (FileWriter fw = new FileWriter("tps.csv", true);
                 PrintWriter out = new PrintWriter(fw)) {

                // Write header only if file is empty
                File file = new File("tps.csv");
                if (file.length() == 0) {
                    out.println("Profit,CurrentTPS,CurrentTokens,TokensUsed,TransactionsUpgraded");
                }
                // Write data row
                out.printf("%.2f,%.2f,%.2f,%.2f,%d%n",
                        result.profit,
                        currentTps,
                        currentTokens,
                        result.tokensUsed,
                        result.transactionsUpgraded);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return result.messages;
        } finally {
            redisLock.writeLock().unlock();
        }
    }

    // this gives me the current rolling throughput, at the time of ack I add the transactions timestamp
    private double getSystemWideThroughput() {
        // this can be accessed while sending ack also so we want to ensure that only thread enters
        synchronized (systemWideThroughput) {
            Long currentTimeStamp = System.currentTimeMillis();
            recordThroughput(ackTransactionsTimeStamps, currentTimeStamp, false);
            return ackTransactionsTimeStamps.size() / 5;
        }
    }

    private double getWriteConcernThroughput(int writeConcern) {
        synchronized (writeConcernThroughput) {
            Long currentTimeStamp = System.currentTimeMillis();
            ConcurrentLinkedQueue<Long> writeConcernSpecificTimeStamps = ackTransactionTimeStampsForAllWriteConcerns.get(writeConcern);
            if (writeConcernSpecificTimeStamps != null) {
                recordThroughput(writeConcernSpecificTimeStamps, currentTimeStamp, false);
            }
            return writeConcernSpecificTimeStamps.size();
        }
    }

    // need to review the logic
    @Override
    public void sendReadRequest(ClientReadRequest readRequest, StreamObserver<Ack> responseObserver) {
        ReadConcern readConcern = readRequest.getReadConcern();

        String accName = readRequest.getAccNameToRead();
        ReadLevel readLevel = readRequest.getReadLevel();
        String id = readRequest.getId();

        int majority = (NUM_OF_SERVERS / 2) + 1;

        if (readConcern == ReadConcern.CAUSAL) {
            // causal consistency
            HybridClock.TimeStamp timeStampRequestedByClient = HybridClock.TimeStamp.convertToTimeStamp(readRequest.getTimeStamp());

            long startTime = System.nanoTime();
            long maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(100);
            Runnable checkLogTask = new Runnable() {
                @Override
                public void run() {
                    if (System.nanoTime() - startTime > maxWaitNanos) {
                        responseObserver.onNext(Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setFailure(true).setAccName(accName).setId(id).build()).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    LogEntry entry = null;
                    if (readLevel == ReadLevel.MAJORITY) {
                        entry = log.get(commitIndex.get());
                    } else {
                        entry = log.get(log.size() - 1);
                    }

                    if (entry != null && entry.timeStamp.compareTo(timeStampRequestedByClient) >= 0) {
                        synchronized (systemWideThroughput) {
                            recordThroughput(ackTransactionsTimeStamps, System.currentTimeMillis(), true);
                        }
                        // log has caught up, safe to read
                        responseObserver.onNext(getBalanceBasedOnReadConcern(readLevel, accName, id));
                        responseObserver.onCompleted();
                    } else {
                        // still behind, schedule again after a short delay
                        causalReadScheduler.schedule(this, 20, TimeUnit.MILLISECONDS); // retry after 10ms
                    }
                }
            };
            causalReadScheduler.execute(checkLogTask);

        } else if (readConcern == ReadConcern.LINEARIZABLE) {
            // this readRequest should go to leader
            // here we check if election is happening or not
            // if election is happening send failure, client can try again
            // this read lock is required because in isElection we are using shared variables and we do not wait for the rpc call to complete before releasing the lock it is async
            lock.readLock().lock();
            try {
                if (status != ServerCurrentStatus.LEADER) {
                    if (currentLeader == -1) {
                        responseObserver.onNext(Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setFailure(true).setAccName(accName).setId(id).build()).build());
                        responseObserver.onCompleted();
                    } else {
                        // redirect to leader
                        stubs[currentLeader].sendReadRequest(readRequest, new StreamObserver<Ack>() {
                            @Override
                            public void onNext(Ack ack) {
                                responseObserver.onNext(ack);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                responseObserver.onNext(Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setFailure(true).setAccName(accName).setId(id).build()).build());
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onCompleted();
                            }
                        });
                    }
                    return;
                }
            } finally {
                lock.readLock().unlock();
            }
            batchLock.lock();
            try {
                batchOfTransactions.add(ClientMessage.newBuilder().setWriteConcern(majority).setT(Transaction.newBuilder().setId(id).setIsReadOnly(true).setWriteConcern(majority).setMinRequiredConsistency(majority).setAccNameToRead(accName).setId(id).build()).build());
                System.out.println(batchOfTransactions);
            } finally {
                batchLock.unlock();
            }
            responseObserver.onNext(Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setFailure(false).setResultNotReady(true).setAccName(accName).setId(id).build()).build());
            responseObserver.onCompleted();
        } else {
            // here the readConcern is just local
            synchronized (systemWideThroughput) {
                recordThroughput(ackTransactionsTimeStamps, System.currentTimeMillis(), true);
            }
            responseObserver.onNext(getBalanceBasedOnReadConcern(readLevel, accName, id));
            responseObserver.onCompleted();
        }
    }

    private Ack getBalanceBasedOnReadConcern(ReadLevel readLevel, String accName, String id) {
        if (readLevel == ReadLevel.LOCAL) {
            return Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setAccName(accName).setBalance(clientBalancesLatest.getOrDefault(accName, 0.0)).setId(id).build()).build();
        } else {
            return Ack.newBuilder().addAckMessage(AckMessage.newBuilder().setAccName(accName).setBalance(clientBalancesMajorityCommitted.getOrDefault(accName, 0.0)).setId(id).build()).build();
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
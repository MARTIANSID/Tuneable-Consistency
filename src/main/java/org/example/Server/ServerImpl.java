package org.example.Server;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import org.example.raft.*;
import org.example.Timer.CustomTimer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.example.raft.RaftGrpc.*;

import org.example.TokenBucket.TokenBucketImpl;
import org.example.Utility.*;
import org.example.Utility.ServerStatus.*;
import org.example.TokenBucket.TokenBucketImpl.TokenBucketData;



// import com.google.protobuf.Empty;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // Replicated KV state machine: local view applied at log append (may roll
    // back), committed view applied at commit-index advance (never rolls back).
    final KvStore kv;

    ConcurrentSkipListMap<HybridClock.TimeStamp, Integer> timeStampsInLog;

    AtomicLong totalLatency;

    ConcurrentHashMap<String, Boolean> ackSent;
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

    private final Object systemWideLatency;

    ConcurrentLinkedQueue<Latency> systemWideLatencies;

    AtomicLong totalSystemWideLatency;
    AtomicLong countOfSystemWideLatencies;

    private

    ReadWriteLock redisLock;

    ReentrantLock batchLock;
    ReentrantLock electionLock;

    TokenBucketImpl tokenBucket;

    // this helps in avoiding race conditions, this will batch transactions for 20ms
    Deque<TransactionOption> batchOfTransactions;

    // this will execute the batch in every 20 ms
    ScheduledExecutorService batchProcessor;
    private ScheduledFuture<?> batchProcessingTask;

    ConcurrentHashMap<Integer, Double> writeConcernCosts;

    ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Long>> ackTransactionTimeStampsForAllWriteConcerns;
    private final Object writeConcernThroughpuLock;
    ConcurrentHashMap<Integer, Object> writeConcernThroughputLocks;

    ConcurrentHashMap<Integer, Deque<Latency>> writeConcernLatencies;
    ConcurrentHashMap<Integer, Long> writeConcernLatencySum;
    ConcurrentHashMap<Integer, Double> smoothedLatencies;
    ConcurrentHashMap<Integer, Double> prevLatencies;
    ConcurrentHashMap<Integer, Object> writeConcernLatencyLocks;

    // Read concern latency tracking (keyed by ReadConcern enum ordinal)
    ConcurrentHashMap<Integer, Deque<Latency>> readConcernLatencies;
    ConcurrentHashMap<Integer, Long> readConcernLatencySum;
    ConcurrentHashMap<Integer, Object> readConcernLatencyLocks;
    ConcurrentHashMap<Integer, Double> prevReadConcernLatencies;

    private static final int RC_KEY_EVENTUAL_ALL = 0;
    private static final int RC_KEY_CAUSAL_LOCAL = 1;
    private static final int RC_KEY_CAUSAL_MAJORITY = 2;
    private static final int RC_KEY_LINEARIZABLE_ALL = 3;

    ScheduledExecutorService sendAppendEntriesScheduler;
    ScheduledExecutorService causalReadScheduler;

    AtomicLongArray lastThroughputSentTime;

    AtomicLongArray lastHeartBeatSent;
    AtomicIntegerArray lastIndexSent;
    // When true, this node stops all Raft inter-server network communication.
    private volatile boolean dropAllServerNetworkTraffic;

    BatchProcessor transactionBatchProcessor;

    Set<String> backLogTransactions;

    ProcessResult previousBatchResult;

    // For tracking incoming transactions per second
    private ConcurrentLinkedQueue<Long> incomingTransactionTimestamps;
    private final Object incomingTransactionLock;
    private AtomicLong lastPrintTime;

    // Batch processing parameters (from config; see ExperimentConfig.serverTuning)
    private static volatile int BATCH_INTERVAL_MS = 20;
    private static final int PROCESS_BATCH_TIME_BUDGET_MS = 15;
    private static volatile int MAX_ITEMS_PER_CYCLE = 2000;

    // Cluster/Redis wiring (from config; see ExperimentConfig.cluster/redis)
    private static volatile String REDIS_HOST = "127.0.0.1";
    private static volatile int REDIS_PORT = 6379;
    private static volatile java.util.List<String> SERVER_HOSTS = java.util.List.of("localhost", "localhost", "localhost");
    private static volatile int SERVER_BASE_PORT = 8000;

    /** Apply cluster, redis, and tuning configuration. Must run before any ServerImpl is constructed. */
    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        BATCH_INTERVAL_MS = config.serverTuning.batchIntervalMs;
        MAX_ITEMS_PER_CYCLE = config.serverTuning.maxItemsPerCycle;
        MAX_BATCH_SIZE = config.serverTuning.maxBatchSize;
        REDIS_HOST = config.redis.host;
        REDIS_PORT = config.redis.port;
        applyClusterSettings(config.cluster.serverHosts, config.cluster.serverBasePort);
    }

    /** Point the peer stubs at a cluster without a full config (tests use this directly). */
    public static void applyClusterSettings(List<String> serverHosts, int basePort) {
        SERVER_HOSTS = List.copyOf(serverHosts);
        SERVER_BASE_PORT = basePort;
    }
    private static final double BACKLOG_DRAIN_RATIO = 1.0;
    private static final int ACK_FUTURE_TIMEOUT_MS = 300;

    // private static final double COST_W1 = 1;
    // private static final double COST_MAJORITY = 2.0;
    private static final int MIN_REQUIRED_THROUGHPUT = 2000; // this is in second

    // this based on the adjustedTokenCosts
    public static final double scale = 1;

    public static final int MIN_COST = 1;
    // it smoothnes noisy measurements like latency samples, we don't overreact to
    // short-term spikes
    private static final double ALPHA = 0.20; // EWMA smoothing

    // if we increase this then our system will become more sensitive to slow
    // repliase, higher writeConcern's cost will rise faster
    private static final double P95_WEIGHT = 0.30;

    // we allow moderate fluctuations to be ignored
    private static final double CHANGE_THRESH = 0.10; // 10%

    private static final double MAX_STEP_UP = 1.25;
    private static final double MAX_STEP_DOWN = 0.80;

    /*
     * If bucket is fully utilized (U ≈ 1), your factor becomes 1 + 0.5 * 1 = 1.5 →
     * token costs are 50% higher.
     * If bucket is mostly idle (U ≈ 0), factor ≈ 1 → no extra penalty. This ensures
     * high-cost WCs are penalized when system is busy.
     */

    private static final double UTIL_K = 0.50; // utilization price slope

    /*
     * If throughput is below target, all token costs increase proportionally →
     * system throttles heavier WCs to preserve throughput.
     * If TPS ≥ target, factor = 1 → no penalty.
     */
    private static final double TPS_K = 0.50; // throughput price slope

    private static final long MIN_UPDATE_PERIOD_MS = 20; // pacing
    private final Map<Integer, Double> ewmaLatency = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastUpdateAt = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, RaftStub> clientStubs;

    private static final double L_HEALTHY_MS = 50.0; // healthy latency
    private static final double L_BAD_MS = 1000.0; // bad latency
    private static final double CONVEX_P = 2.0; // convexity exponent (>1)

    private static final double HEALTHY_TPS_HEADROOM = 1.30; // allow 30% above TPS_min at healthy latency
    private static final double TPS_EPSILON = 0.20; // allow up to +20% over TPS_min budget
    private static final double BUCKET_FRACTION_MAX = 0.95; // max 6% of bucket per request
    private static final double DEFAULT_BUDGET = 10000.0; // default budget for transaction processing
    private static final long FOLLOWER_TPS_STALE_MS = 400L;

    double[] followerReadThroughput;

    double combinedThroughputOfFollowers;

    double combinedSystemWideThroughputOnFollower;

    long lastThroughputSentTimeOnFollower;
    AtomicLong[] lastHeartBeatReceived;
    private ExecutorService executorService;
    private static final Logger LOG = LoggerFactory.getLogger(ServerImpl.class);

    BacklogTracker backlogTracker;
    ScheduledExecutorService backLogScheuler;


    ConcurrentLinkedQueue<TransactionOption> backLogQueue;

    // Every gRPC channel this node opens (peer stubs, client callback stubs),
    // so shutdown() can close them all.
    private final List<ManagedChannel> ownedChannels = Collections.synchronizedList(new ArrayList<>());

    // ===== Consistency toggles =====
    // When false, the server will bypass handleTokenBucket() and execute the incoming
    // batch exactly as received (no upgrades, no deferrals).
    private static volatile boolean UPGRADE_TRANSACTIONS_ENABLED = false;
    // When true, enable pressure-aware processing (token charge moves to batch time).
    private static volatile boolean PRESSURE_MODE_ENABLED = false;
    // When true, charge the token bucket at enqueue time and reject what does not fit.
    // Independent of upgrades; must not be combined with pressure mode (double charging) -
    // enforced by ExperimentConfig validation.
    private static volatile boolean ADMISSION_CONTROL_ENABLED = true;

    public static void setUpgradeTransactionsEnabled(boolean enabled) {
        UPGRADE_TRANSACTIONS_ENABLED = enabled;
    }

    public static void setPressureModeEnabled(boolean enabled) {
        PRESSURE_MODE_ENABLED = enabled;
    }

    public static void setAdmissionControlEnabled(boolean enabled) {
        ADMISSION_CONTROL_ENABLED = enabled;
    }
    public void setDropAllServerNetworkTraffic(boolean enabled) {
        this.dropAllServerNetworkTraffic = enabled;
    }

    public boolean isDropAllServerNetworkTraffic() {
        return dropAllServerNetworkTraffic;
    }

    private boolean shouldDropServerNetworkTraffic() {
        return dropAllServerNetworkTraffic;
    }

    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.dropAllServerNetworkTraffic = false;
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = -1;
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(), (new Random().nextInt(700) + 2000),
                TimeUnit.MILLISECONDS);
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
        this.kv = new KvStore();
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
        this.tokenBucket = new TokenBucketImpl(REDIS_HOST, REDIS_PORT, serverId);
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

        // Initialize read concern latency tracking
        this.readConcernLatencies = new ConcurrentHashMap<>();
        this.readConcernLatencySum = new ConcurrentHashMap<>();
        this.readConcernLatencyLocks = new ConcurrentHashMap<>();
        this.prevReadConcernLatencies = new ConcurrentHashMap<>();
        initializeReadLatencyKey(RC_KEY_EVENTUAL_ALL);
        initializeReadLatencyKey(RC_KEY_CAUSAL_LOCAL);
        initializeReadLatencyKey(RC_KEY_CAUSAL_MAJORITY);
        initializeReadLatencyKey(RC_KEY_LINEARIZABLE_ALL);
        this.transactionBatchProcessor = new BatchProcessor(NUM_OF_SERVERS);
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.lastThroughputSentTimeOnFollower = 0;
        this.writeConcernThroughpuLock = new Object();
        this.writeConcernThroughputLocks = new ConcurrentHashMap<>();
        this.backLogTransactions = ConcurrentHashMap.newKeySet();
        this.systemWideLatency = new Object();
        this.systemWideLatencies = new ConcurrentLinkedQueue<>();
        this.totalSystemWideLatency = new AtomicLong(0);
        this.countOfSystemWideLatencies = new AtomicLong(0);
        this.prevLatencies = new ConcurrentHashMap<>();
        this.writeConcernLatencyLocks = new ConcurrentHashMap<>();
        this.backlogTracker = new BacklogTracker(0.2, 100.0, serverId);

        // Initialize incoming transaction tracking
        this.incomingTransactionTimestamps = new ConcurrentLinkedQueue<>();
        this.incomingTransactionLock = new Object();
        this.lastPrintTime = new AtomicLong(System.currentTimeMillis());
        this.sendAppendEntriesScheduler = Executors.newScheduledThreadPool(1);
        this.causalReadScheduler = Executors.newScheduledThreadPool(1);
        this.lastHeartBeatSent = new AtomicLongArray(NUM_OF_SERVERS);
        this.lastIndexSent = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.followerReadThroughput = new double[NUM_OF_SERVERS];
        this.combinedThroughputOfFollowers = 0;
        this.lastThroughputSentTime = new AtomicLongArray(NUM_OF_SERVERS);
        this.clientStubs = new ConcurrentHashMap<>();
        this.lastHeartBeatReceived = new AtomicLong[NUM_OF_SERVERS];
        this.backLogQueue = new ConcurrentLinkedQueue<>();
        

        // setting the peers list
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            // setting up the nextIndex and matchIndex

            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
            lastHeartBeatReceived[i] = new AtomicLong(0);

            int majorityLevel = ((NUM_OF_SERVERS / 2) + 1);

            // setting up of the queues for calculating the throughput of each writeConcern
            if (i > 0 && i <= majorityLevel) {
                ackTransactionTimeStampsForAllWriteConcerns.put(i, new ConcurrentLinkedQueue<>());
                // initially we might want to set the write concerns costs as 1.0 but as the
                // throughput is calculated they are adjusted
                writeConcernCosts.put(i, 1.0);
                writeConcernLatencies.put(i, new ArrayDeque<>());
                writeConcernLatencySum.put(i, (long) 0);
                writeConcernThroughputLocks.put(i, new Object());
                writeConcernLatencyLocks.put(i, new Object());
            }
        }
        batchProcessingTask = batchProcessor.scheduleWithFixedDelay(this::processBatchWithErrorHandling, 0,
                        BATCH_INTERVAL_MS,
                        TimeUnit.MILLISECONDS);

        // starting the election timer
        this.electionTimer.start();
    }

    public void setUpStubs() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            // setting up the stubs
            if (i != serverId) {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(SERVER_HOSTS.get(i), SERVER_BASE_PORT + (i + 1)).enableRetry()
                        .usePlaintext().build();
                ownedChannels.add(channel);
                stubs[i] = RaftGrpc.newStub(channel);
                blockingStubs[i] = RaftGrpc.newBlockingStub(channel);

            }
        }
    }

    @Override
    public void appendEntries(AppendEntriesArgument appendEntriesArgument,
            StreamObserver<AppendEntriesResult> responseObserver) {
            if (shouldDropServerNetworkTraffic()) {
                responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: appendEntries dropped")
                    .asRuntimeException());
                return;
            }
        int leadersTerm = appendEntriesArgument.getLeadersTerm(),
                prevLogIndex = appendEntriesArgument.getPrevLogIndex(),
                prevLogTerm = appendEntriesArgument.getPrevLogTerm(),
                leadersCommitIndex = appendEntriesArgument.getLeadersCommit(),
                leaderId = appendEntriesArgument.getLeadersId(), leadersAckIndex = appendEntriesArgument.getAckIndex();

        TimeStampProto leadersTimeStamp = appendEntriesArgument.getTimeStamp();

        lock.writeLock().lock();

        try {
         
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(leadersTimeStamp));

            // Check if the leader's term is valid
            if (leadersTerm > currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                becomeFollower();
            }

            if (leadersTerm == currentTerm.get()) {
                currentLeader = leaderId;
                // If we're CANDIDATE or LEADER and receive AppendEntries from valid leader,
                // step down
                if (status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            }

            // If the leader's term is outdated
            if (leadersTerm < currentTerm.get()) {
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get())
                        .setIsSuccessFull(false).setFollowerId(serverId).build());
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
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get())
                        .setIsSuccessFull(false).setFollowerId(serverId).build());
                responseObserver.onCompleted();
                return;
            }
            // Proceed with appending the entries
            Log leadersEntries = appendEntriesArgument.getEntriesToAppend();

            // reverting the latest balances
            rollbackTillIndex(prevLogIndex + 1);
            log.truncateAfter(prevLogIndex + 1); // Clear entries after prevLogIndex
            log.appendEntries(leadersEntries, serverId); // Append new entries I also update the writeConcern here
                                                         // because this particular needs to update the writeConcern
                                                         // data on its end

            // updating the latest balances
            for (LogEntryProto logEntry : leadersEntries.getLogList()) {
                String id = logEntry.getT().getId();
                // adding the entries in tIdToLogIndex, for quick access to check duplicates
                // from client side
                tIdToLogIndex.put(id, logEntry.getLogIndex());
                // we need the time stamps in log to provide causal consistency
                timeStampsInLog.put(HybridClock.TimeStamp.convertToTimeStamp(logEntry.getTimeStamp()),
                        logEntry.getLogIndex());
                // applying to the local (may-roll-back) KV view at append time
                kv.applyLocal(logEntry.getT(), logEntry.getLogIndex());
                // updating ackSent Map
                ackSent.put(id, false);
            }
            // Update commit index
            if (leadersCommitIndex > commitIndex.get()) {
                int prevCommitIndex = commitIndex.get();

                // updating the commitIndex of this follower
                commitIndex.set(Math.min(leadersCommitIndex, log.size() - 1));
                // update the majority-committed KV view
                for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                    kv.applyCommitted(log.get(i).t, i);
                    // updating the ack sent
                    ackSent.put(log.get(i).t.getId(), true);
                }
            }
            // current time of follower
            HybridClock.TimeStamp currentTimeOfFollower = hybridClock.now();
            // Send success response
            AppendEntriesResult.Builder appendEntriesResultBuilder = AppendEntriesResult.newBuilder()
                    .setIsSuccessFull(true).setFollowerId(serverId)
                    .setTimeStamp(HybridClock.TimeStamp.convertToProto(currentTimeOfFollower));
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastThroughputSentTimeOnFollower >= 200) {
                lastThroughputSentTimeOnFollower = currentTime;
                appendEntriesResultBuilder.setReadThroughputIncluded(true);
                appendEntriesResultBuilder.setFollowerReadThroughput(getSystemWideThroughput());
            }
            responseObserver.onNext(appendEntriesResultBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            System.out.println("Exception in appendEntries: " + e.getMessage());
            // throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // inside write lock; logIndex is the first log position being truncated
    private void rollbackTillIndex(int logIndex) {
        if (logIndex >= log.size()) {
            // Nothing is being truncated (the common append-only path).
            return;
        }
        if (logIndex <= commitIndex.get()) {
            // Raft's leader completeness guarantee makes this unreachable: a
            // leader whose consistency check passed can never truncate below
            // the follower's commit index. If it happens, the state machine is
            // corrupt and continuing silently would serve wrong committed data.
            throw new IllegalStateException("attempted to roll back committed entries: truncation at "
                    + logIndex + " but commit index is " + commitIndex.get());
        }
        for (int i = logIndex; i < log.size(); i++) {
            // remove the entries from tIdToLogIndex
            tIdToLogIndex.remove(log.get(i).t.getId());
            // remove the timestamps
            timeStampsInLog.remove(log.get(i).timeStamp);
        }
        // Rebuild the local KV view: committed state plus the uncommitted
        // entries that survive the truncation, replayed in log order.
        kv.rebuildLocal(log.logEntriesFromIndex(commitIndex.get() + 1, logIndex));
    }

    // in lock
    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogIndex();

        // deny vote condition
        if ((lastLogTermOfCurrentNode > lastLogTermOfCandidate) || ((lastLogTermOfCurrentNode == lastLogTermOfCandidate)
                && (lastLogIndexOfCurrentNode > lastLogIndexOfCandidate))) {
            return false;
        }

        return true;
    }

    @Override
    public void requestVote(RequestVoteArguments requestVoteArguments,
            StreamObserver<RequestVoteResult> responseObserver) {
            if (shouldDropServerNetworkTraffic()) {
                responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: requestVote dropped")
                    .asRuntimeException());
                return;
            }
        int currentTermOfTheCandidate = requestVoteArguments.getCandidatesTerm(),
                lastLogIndexOfCandidate = requestVoteArguments.getLastLogIndex(),
                lastLogTermOfCandidate = requestVoteArguments.getLastLogTerm(),
                candidateId = requestVoteArguments.getCandidateId();

        boolean isVoteGranted = true;

        lock.writeLock().lock();

        try {
            if (currentTermOfTheCandidate > currentTerm.get()) {
                // the term of this follower is updated because it will now vote in this updated
                // term
                currentTerm.updateAndGet(term -> Math.max(term, currentTermOfTheCandidate));
                votedFor = -1;
                // the node must become a follower
                becomeFollower();
            }
            // all the necessary conditions to check for denying vote
            if (votedFor != -1 || (this.currentTerm.get() > currentTermOfTheCandidate)
                    || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
                // reply false here
                isVoteGranted = false;
            }
            if (isVoteGranted) {
                // vote for this term, this ideally can be optimised no need to map
                votedFor = candidateId;
                // resetting the election timer
                startTheElectionTimer();
            }

            RequestVoteResult requestVoteResult = RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted)
                    .setCurrentTerm(currentTerm.get()).build();
            responseObserver.onNext(requestVoteResult);
            responseObserver.onCompleted();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
        * Enqueue new transactions while preserving existing queued entries.
     *
     * @return number of transactions accepted into the queue
     */
    public int enqueueWithoutDroppingExisting(List<TransactionOption> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            return 0;
        }

        boolean isLeaderNode;
        lock.readLock().lock();
        try {
            isLeaderNode = (status == ServerCurrentStatus.LEADER);
        } finally {
            lock.readLock().unlock();
        }

        // Followers should only accept follower-safe reads.
        if (!isLeaderNode) {
            for (TransactionOption tx : transactions) {
                if (tx == null || tx.clientMessage == null || !tx.clientMessage.hasT()) {
                    return 0;
                }
                Transaction t = tx.clientMessage.getT();
                if (!t.getIsReadOnly() || t.getReadConcern() == ReadConcern.LINEARIZABLE) {
                    return 0;
                }
            }
        }

        return admitTransactionsIntoQueue(transactions);
    }

    private int admitTransactionsIntoQueue(List<TransactionOption> transactions) {
        int candidateCount = transactions.size();
        if (candidateCount <= 0) {
            return 0;
        }

        int accepted;
        if (tokenBucket == null || !ADMISSION_CONTROL_ENABLED) {
            accepted = candidateCount;
        } else {
            redisLock.writeLock().lock();
            try {
                TokenBucketData data = tokenBucket.getCurrentTokenBucketData();
                double tokensRemaining = data.getTokenCount();
                accepted = 0;

                for (int i = 0; i < candidateCount; i++) {
                    TransactionOption tx = transactions.get(i);
                    double tokenCost = estimateTokenCostForAdmission(tx);
                    if (tokenCost <= tokensRemaining + 1e-9) {
                        tokensRemaining -= tokenCost;
                        accepted++;
                    } else {
                        // Keep admission deterministic: stop admitting once budget is exhausted.
                        break;
                    }
                }

                tokenBucket.updateTokens(Math.max(0.0, tokensRemaining), data.getLastUpdateTime());
            } finally {
                redisLock.writeLock().unlock();
            }
        }

        recordAdmissionFairness(transactions, accepted);

        if (accepted > 0) {
            batchLock.lock();
            try {
                for (int i = 0; i < accepted; i++) {
                    batchOfTransactions.addLast(transactions.get(i));
                }
            } finally {
                batchLock.unlock();
            }
        }
        return accepted;
    }

    // ===== Admission fairness metric =====
    // Per consistency level: how many transactions were offered to this server's
    // queue and how many the admission gate accepted. With admission control off,
    // requested == admitted, which is exactly the baseline for fairness comparison.
    private final ConcurrentHashMap<String, AtomicLong> fairnessRequestedByLevel = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> fairnessAdmittedByLevel = new ConcurrentHashMap<>();
    private final AtomicLong fairnessLastFlushMs = new AtomicLong(0);
    private static final long FAIRNESS_FLUSH_INTERVAL_MS = 1000;

    private static String consistencyLevelLabel(TransactionOption tx) {
        if (!tx.isReadOnly) {
            return "W:" + tx.minRequiredConsistency;
        }
        if (tx.readConcern == ReadConcern.LINEARIZABLE) {
            return "R:LINEARIZABLE";
        }
        if (tx.readConcern == ReadConcern.EVENTUAL) {
            return "R:EVENTUAL";
        }
        return (tx.readLevel == ReadLevel.MAJORITY) ? "R:CAUSAL_MAJORITY" : "R:CAUSAL_LOCAL";
    }

    private void recordAdmissionFairness(List<TransactionOption> transactions, int accepted) {
        // Admission accepts a prefix of the candidate list, so index < accepted
        // is exactly the admitted set.
        for (int i = 0; i < transactions.size(); i++) {
            String level = consistencyLevelLabel(transactions.get(i));
            fairnessRequestedByLevel.computeIfAbsent(level, k -> new AtomicLong()).incrementAndGet();
            if (i < accepted) {
                fairnessAdmittedByLevel.computeIfAbsent(level, k -> new AtomicLong()).incrementAndGet();
            }
        }

        long now = System.currentTimeMillis();
        long last = fairnessLastFlushMs.get();
        if (now - last >= FAIRNESS_FLUSH_INTERVAL_MS && fairnessLastFlushMs.compareAndSet(last, now)) {
            flushAdmissionFairness(now);
        }
    }

    private void flushAdmissionFairness(long now) {
        // Rows carry cumulative totals per level; consumers diff consecutive rows
        // to get per-interval rates. Cumulative keeps the file lossless even if a
        // flush interval is skipped.
        String csvPath = "admission_fairness_" + serverId + ".csv";
        File file = new File(csvPath);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(csvPath, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,Level,RequestedTotal,AdmittedTotal,RejectedTotal");
            }
            for (Map.Entry<String, AtomicLong> e : fairnessRequestedByLevel.entrySet()) {
                long requested = e.getValue().get();
                AtomicLong admittedCounter = fairnessAdmittedByLevel.get(e.getKey());
                long admitted = (admittedCounter == null) ? 0 : admittedCounter.get();
                out.printf("%d,%s,%d,%d,%d%n", now, e.getKey(), requested, admitted, requested - admitted);
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + csvPath + ": " + e.getMessage());
        }
    }

    @Override
    public void sendTransaction(ClientMessage clientMessage, StreamObserver<Empty> responseObserver) {
            if (shouldDropServerNetworkTraffic()) {
                responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: sendTransaction dropped")
                    .asRuntimeException());
                return;
            }
        // check if the current node is leader or not, if not forward request to leader,
        // this might fail if election is going on
        if (serverId != currentLeader && currentLeader != -1) {
            stubs[currentLeader].sendTransaction(clientMessage, new StreamObserver<Empty>() {
                @Override
                public void onNext(Empty value) {
                }

                @Override
                public void onError(Throwable t) {
                    responseObserver
                            .onError(Status.UNAVAILABLE.withDescription("forward failed: " + t).asRuntimeException());
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
        // I send the ack back, to resolve the above blocking call immediately once the
        // message is received

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();

        String clientId = getClientId(clientMessage.getCallbackHost(), clientMessage.getCallbackPort());

        clientStubs.computeIfAbsent(clientId,
                k -> createClientStub(clientMessage.getCallbackHost(), clientMessage.getCallbackPort()));

        TransactionOption option = TransactionOption.fromClientMessage(clientMessage);
        enqueueWithoutDroppingExisting(List.of(option));
    }

    private String getClientId(String host, int port) {
        return host + ":" + port;
    }

    private RaftStub createClientStub(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        ownedChannels.add(channel);
        return RaftGrpc.newStub(channel);
    }

    /**
     * Tracks incoming transactions and prints the rate per second
     */
    private void trackIncomingTransaction() {
        long currentTime = System.currentTimeMillis();

        synchronized (incomingTransactionLock) {
            // Remove timestamps older than 1 second
            while (!incomingTransactionTimestamps.isEmpty()
                    && currentTime - incomingTransactionTimestamps.peek() >= 1000L) {
                incomingTransactionTimestamps.poll();
            }
            incomingTransactionTimestamps.add(currentTime);

            // Print and log to CSV every second
            long lastPrint = lastPrintTime.get();
            if (currentTime - lastPrint >= 1000L && lastPrintTime.compareAndSet(lastPrint, currentTime)) {
                int incomingTPS = incomingTransactionTimestamps.size();
                // System.out.printf("📥 [Incoming Transactions] Server %d | Transactions/sec: %d | Time: %s%n", serverId,
                //         incomingTPS,
                //         new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(currentTime)));

                // Log to CSV file
                try {
                    File file = new File("incoming_transaction_rate_" + serverId + ".csv");
                    boolean writeHeader = !file.exists() || file.length() == 0;
                    try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
                        if (writeHeader) {
                            out.println("Timestamp,IncomingTransactionCount,IncomingTPS");
                        }
                        out.printf("%d,%d,%d%n", currentTime, incomingTPS, incomingTPS);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private int getIncomingTransactionsRate() {
        long currentTime = System.currentTimeMillis();

        synchronized (incomingTransactionLock) {
            // Remove timestamps older than 1 second
            while (!incomingTransactionTimestamps.isEmpty()
                    && currentTime - incomingTransactionTimestamps.peek() >= 1000L) {
                incomingTransactionTimestamps.poll();
            }
            return incomingTransactionTimestamps.size();
        }
    }

    /**
     * Compute incoming transactions per second from the timestamp window.
     */
    private int getIncomingTransactionsPerSecond() {
        long now = System.currentTimeMillis();
        synchronized (incomingTransactionLock) {
            while (!incomingTransactionTimestamps.isEmpty() && now - incomingTransactionTimestamps.peek() >= 1000L) {
                incomingTransactionTimestamps.poll();
            }
            return incomingTransactionTimestamps.size();
        }
    }

    public int getLastLogIndex() {
        if (log.isEmpty()) {
            return -1;
        } else {
            return log.getLastLogEntry().index;
        }
    }

    public int getLastLogTerm() {
        if (log.isEmpty()) {
            return -1;
        } else {
            return log.getLastLogEntry().term;
        }
    }

    private RequestVoteArguments getRequestVoteArgumentsObject() {
        lock.readLock().lock();
        try {
            return RequestVoteArguments.newBuilder().setCandidateId(this.serverId)
                    .setCandidatesTerm(this.currentTerm.get()).setLastLogTerm(this.getLastLogTerm())
                    .setLastLogIndex(this.getLastLogIndex()).build();
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
        // if vote is not granted then I decrease the latchCount but do not increase the
        // vote count
        return true;
    }

    private void requestForVotes(RequestVoteArguments requestVoteArguments) {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }

        // here I have deducted one because obviously the server requesting for votes,
        // will not be responding to requestVote rpc
        // also we expect response from total servers - 1
        CountDownLatch latch = new CountDownLatch(NUM_OF_SERVERS - 1);

        for (int i = 0; i < NUM_OF_SERVERS; i++) {

            if (i == serverId)
                continue;

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
                        // since compareAndSet is atomic we protect against the double transitions due
                        // to race conditions
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
                        while (latch.getCount() > 0)
                            latch.countDown();
                    } else if (shouldBecomeFollower) {
                        becomeFollower();
                        while (latch.getCount() > 0)
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
            boolean success = latch.await(800, TimeUnit.MILLISECONDS);
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

    private List<LogEntryProto> convertLogEntryToProto(List<LogEntry> entries) {
        List<LogEntryProto> result = new ArrayList<>();

        for (LogEntry entry : entries) {
            result.add(LogEntryProto.newBuilder().setLogIndex(entry.index).setT(entry.t).setTerm(entry.term)
                    .addAllServersThatReplicatedThisEntry(entry.serversThatReplicatedThisEntry)
                    .setWriteConcern(entry.writeConcern)
                    .setTimeStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp))
                    .setCopyOfWriteConcern(entry.copyOfWriteConcern).setCallbackHost(entry.clientHost)
                    .setCallbackPort(entry.clientPort).setTimeOfArrivalAtLeader(entry.timeOfArrivalAtLeader).build());
        }
        return result;
    }

    // should be inside a lock, this method is kind of better because we do not need
    // to sort the matchIndex array and we can get the commit index in roughly O(n)
    // time because usually the log of follower and leader is off by 2-3 entries
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

        // System.out.println(entriesToBeAck.size() + "This is the number of entries for which ack is being sent");
        Map<String, List<AckMessage>> ackMessagesPerClient = new HashMap<>();

        // maybe we can acquire a read lock (but to optimise it we can keep this lock
        // specific to the sendAck logic to avoid sending multiple ack
        // this is just about minimising repeated acks, not compulsory to add it, now
        // for the calculation of the metrics this is strictly required

        try {
            Long timeStampOfTransaction = System.currentTimeMillis();

            for (LogEntry entry : entriesToBeAck) {

                String id = entry.t.getId();
                String clientHost = entry.clientHost;
                int clientPort = entry.clientPort;
                String clientId = getClientId(clientHost, clientPort);

                clientStubs.computeIfAbsent(clientId, k -> createClientStub(clientHost, clientPort));

                // this field is seperate for each thread, so there will be no race conditions
                // for this
                boolean firstAck = false;

                // need to add lock because lot of shared variables are being accessed here
                synchronized (ackUpdateLock) {
                    if (ackSent.containsKey(id) && !ackSent.get(id)) {
                        firstAck = true;
                        // marking it as sent, if it fails the client can retry from its end
                        ackSent.put(id, true);
                        // send ack for this entry
                        ackTransactionCount.incrementAndGet();
                        AckMessage.Builder ackBuilder = AckMessage.newBuilder().setT(entry.t)
                                .setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp))
                                .setCurrentLeader(serverId)
                                .setExecutedLevelIncluded(true)
                                // entry.writeConcern is decremented as replication acks arrive;
                                // copyOfWriteConcern preserves the executed level.
                                .setExecutedWriteConcern(entry.copyOfWriteConcern)
                                .setExecutedReadConcern(entry.t.getReadConcern())
                                .setExecutedReadLevel(entry.t.getReadLevel());

                        if (entry.t.getIsReadOnly()) {
                            KvStore.Versioned committedValue = kv.readCommitted(entry.t.getKey());
                            ackBuilder.setId(id)
                                    .setReadValue(committedValue == null ? "" : committedValue.value());
                            recordReadConcernLatency(ReadConcern.LINEARIZABLE, ReadLevel.MAJORITY,
                                entry.timeOfArrivalAtLeader);
                        }

                        AckMessage ackMessage = ackBuilder.build();

                        // grouping the ack messages based on the clientId
                        ackMessagesPerClient.putIfAbsent(clientId, new ArrayList<>());
                        ackMessagesPerClient.get(clientId).add(ackMessage);
                    }
                }
                // if we are sending the ack of this transaction again we do not want to process
                // the writeConcernThroughput
                if (!firstAck)
                    continue;

                synchronized (systemWideLatency) {
                    long latencyOfThisTransaction = timeStampOfTransaction - entry.timeOfArrivalAtLeader;
                    // removing old latencies
                    while (!systemWideLatencies.isEmpty()
                            && (timeStampOfTransaction - systemWideLatencies.peek().timestamp) >= 1000L) {
                        Latency latency = systemWideLatencies.poll();
                        Long systemLatency = totalSystemWideLatency.get();
                        totalSystemWideLatency.set(systemLatency - latency.latency);
                        countOfSystemWideLatencies.decrementAndGet();
                    }
                    systemWideLatencies.add(new Latency(timeStampOfTransaction, latencyOfThisTransaction));
                    Long systemLatency = totalSystemWideLatency.get();
                    totalSystemWideLatency.set(systemLatency + latencyOfThisTransaction);
                    countOfSystemWideLatencies.incrementAndGet();
                }
                synchronized (systemWideThroughput) {
                    recordThroughput(ackTransactionsTimeStamps, timeStampOfTransaction, true);
                }
            }
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, List<AckMessage>> clientEntry : ackMessagesPerClient.entrySet()) {
                String clientId = clientEntry.getKey();
                List<AckMessage> messages = clientEntry.getValue();

                RaftGrpc.RaftStub stub = clientStubs.get(clientId);

                CompletableFuture<Void> future = new CompletableFuture<>();
                futures.add(future);

                stub.sendAckToClient(Ack.newBuilder().addAllAckMessage(messages).build(), new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(null);
                    }
                });
            }

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        } catch (Exception e) {
            // System.out.println("Exception occurred while sending acks to clients: " + e.getMessage());
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }

    public long getAverageLatency(int writeConcern) {
        synchronized (writeConcernLatencyLocks.get(writeConcern)) {
            long val = writeConcernLatencySum.get(writeConcern)
                    / Math.max(writeConcernLatencies.get(writeConcern).size(), 1);
            if (val == 0) {
                return prevLatencies.getOrDefault(writeConcern, 0.0).longValue();
            } else {
                prevLatencies.put(writeConcern, (double) val);
            }
            return val;
        }
    }

    // inside lock
    private void recordThroughput(ConcurrentLinkedQueue<Long> queue, long timeStampOfTransaction,
            boolean addTimeStamp) {
        while (!queue.isEmpty() && timeStampOfTransaction - queue.peek() >= 5000L) {
            queue.poll();
        }
        // during processing the batch we do not want to add the timestamp
        if (addTimeStamp)
            queue.add(timeStampOfTransaction);
    }

    private double getFreshFollowerReadThroughputSum(long nowMs) {
        lock.readLock().lock();
        try {
            double sum = 0.0;
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (i == serverId) {
                    continue;
                }
                long lastSeen = lastHeartBeatReceived[i].get();
                if (lastSeen > 0 && (nowMs - lastSeen) <= FOLLOWER_TPS_STALE_MS) {
                    sum += followerReadThroughput[i];
                }
            }
            return sum;
        } finally {
            lock.readLock().unlock();
        }
    }

    // we can optimize the write lock here
    private boolean handleAppendEntriesResult(AppendEntriesResult appendEntriesResult, int matchIndexOfFollower,
            int prevNextIndex) {
        boolean result = appendEntriesResult.getIsSuccessFull();
        int termOfFollower = appendEntriesResult.getCurrentTerm(), idOfFollower = appendEntriesResult.getFollowerId();

        TimeStampProto followersTimeStamp = appendEntriesResult.getTimeStamp();

        List<LogEntry> committedEntriesAck = new ArrayList<>();
        List<LogEntry> eventualEntriesAck = new ArrayList<>();

        boolean stillLeader = true;

        // Lock for reading and writing shared state
        lock.writeLock().lock(); // Lock to ensure exclusive write access for updating `nextIndex`, `matchIndex`,
                                 // etc.
        try {
            // this makes the function idempotent
            if (status != ServerCurrentStatus.LEADER) {
                stillLeader = false;
                return false;
            }
            // updating the clock of leader, if its behind it can catchup
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(followersTimeStamp));

            if (termOfFollower > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, termOfFollower));
                // Become follower
                becomeFollower();
                stillLeader = false;
                return false;
            }
            // updating the throughput of follower
            if (appendEntriesResult.getReadThroughputIncluded()) {
                combinedThroughputOfFollowers = combinedThroughputOfFollowers - followerReadThroughput[idOfFollower]
                        + appendEntriesResult.getFollowerReadThroughput();
                followerReadThroughput[idOfFollower] = appendEntriesResult.getFollowerReadThroughput();
            }

            if (!result) {
                // added just to ensure that nextIndex does not decrement twice (maybe it can
                // happen, need to think of this situation again)
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
                    // Check if we need to update the commitIndex of the leader, we get the new
                    // commitIndex
                    // int candidateCommitIndex = getCommitIndexIfPossible();
                    int candidateCommitIndex = getCommitIndexIfPossibleEarlyExitMethod();

                    if (candidateCommitIndex > commitIndex.get()) {
                        commitIndex.updateAndGet(index -> Math.max(index, candidateCommitIndex)); // Update commitIndex
                        // System.out.println(commitIndex.get() + "This is the new commit index---");
                        // update the majority committed map
                        for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {

                            // this updates the tps of w:majority
                            synchronized (writeConcernThroughputLocks.get((NUM_OF_SERVERS / 2) + 1)) {
                                ConcurrentLinkedQueue<Long> queue = ackTransactionTimeStampsForAllWriteConcerns
                                        .get((NUM_OF_SERVERS / 2) + 1);
                                Long currentTime = System.currentTimeMillis();
                                while (!queue.isEmpty() && currentTime - queue.peek() >= 5000L) {
                                    queue.poll();
                                }
                                queue.add(currentTime);
                            }
                            synchronized (writeConcernLatencyLocks.get((NUM_OF_SERVERS / 2) + 1)) {
                                int writeConcernOfThisTransaction = (NUM_OF_SERVERS / 2) + 1;
                                Long timeStampOfTransaction = System.currentTimeMillis();
                                Long arrivalTimeOfThisEntryOnLeader = log.get(i).timeOfArrivalAtLeader;

                                Long currentLatency = (timeStampOfTransaction - arrivalTimeOfThisEntryOnLeader);
                                Deque<Latency> latencies = writeConcernLatencies.get(writeConcernOfThisTransaction);
                                while (!latencies.isEmpty()
                                        && (timeStampOfTransaction - latencies.peek().timestamp) >= 5000L) {
                                    Latency latency = latencies.poll();
                                    writeConcernLatencySum.put(writeConcernOfThisTransaction,
                                            writeConcernLatencySum.get(writeConcernOfThisTransaction)
                                                    - latency.latency);
                                }
                                latencies.add(new Latency(timeStampOfTransaction, currentLatency));
                                writeConcernLatencySum.put(writeConcernOfThisTransaction,
                                        writeConcernLatencySum.get(writeConcernOfThisTransaction) + currentLatency);
                            }

                            kv.applyCommitted(log.get(i).t, i);
                        }
                        // System.out.println("The commit index of leader updated to -- " +
                        // commitIndex.get());
                        // System.out.println("Log size of leader is---" + log.size());

                        // Send acknowledgements from [(prevCommitIndex + 1), commitIndex] if needed
                        committedEntriesAck = new ArrayList<>(log.getEntries(prevCommitIndex + 1, commitIndex.get()));
                    }
                    // the leader see what all entries have been replicated by the replica, and
                    // decrements the appendEntries for those
                        // System.out.println("This is the prevMatchIndex of follower --" + prevMatchIndex
                        //         + " and this is the new match index of follower --" + matchIndex.get(idOfFollower));
                    eventualEntriesAck = checkIfWriteConcernsAreSatisfied(prevMatchIndex, matchIndex.get(idOfFollower),
                            idOfFollower);
                }
            }
        } catch (Exception e) {
                    // System.out.println("Exception occurred while handling AppendEntriesResult: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            lock.writeLock().unlock(); // Unlock after modifying shared state

            if (stillLeader) {
                // sending ack logic is kept outside the lock to reduce the contention
                // after releasing the lock maybe I can wait for a certain time till all the
                // acks are sent?
                if (!committedEntriesAck.isEmpty()) {
                    // System.out.println(committedEntriesAck.size()
                    //         + "This is the number of committed entries for which ack is being sent");
                    sendAckForEntries(committedEntriesAck).orTimeout(ACK_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS).exceptionally((ex -> {
                        // System.out.println("Ack failed reason: " + ex);
                        return null;
                    }));
                }

                if (!eventualEntriesAck.isEmpty()) {
                    sendAckForEntries(eventualEntriesAck).orTimeout(ACK_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS).exceptionally((ex -> {
                        // System.out.println("Ack failed reason: " + ex);
                        return null;
                    }));
                }
                lastHeartBeatReceived[idOfFollower].set(System.currentTimeMillis());
            }
        }
        return true;
    }

    // inside write lock
    private List<LogEntry> checkIfWriteConcernsAreSatisfied(int prevMatchIndexOfFollower, int newMatchIndexOfFollower,
            int idOfFollower) {
        List<LogEntry> entries = new ArrayList<>();

        // optional check, need to confirm if we this is necessary
        // if (log.get(newMatchIndexOfFollower).term != currentTerm.get()) return;

        for (int i = Math.max(commitIndex.get() + 1, prevMatchIndexOfFollower + 1); i <= newMatchIndexOfFollower; i++) {
            String id = log.get(i).t.getId();
            boolean canBeAdded = log.get(i).writeConcern != 0;
            // updateWriteConcern handles all the necessary conditions so that the same node
            // does update the write concern of the same log entry again
            log.updateWriteConcern(i, idOfFollower, ackTransactionTimeStampsForAllWriteConcerns,
                    writeConcernThroughputLocks, writeConcernLatencyLocks, writeConcernLatencies,
                    writeConcernLatencySum);
            if (log.get(i).writeConcern == 0 && canBeAdded) {
                entries.add(log.get(i));
            }
        }
        return entries;
    }

    private void sendAppendEntries() {

        final int MAX_ENTRIES_PER_RPC = 7000;

        sendAppendEntriesScheduler.scheduleAtFixedRate(() -> {

            if (shouldDropServerNetworkTraffic()) {
                return;
            }

            if (status != ServerCurrentStatus.LEADER)
                return;

            for (int i = 0; i < NUM_OF_SERVERS; i++) {

                if (i == serverId)
                    continue;
                final int followerId = i;

                executorService.submit(() -> {

                    int indexToSendFrom;
                    int endIndex;
                    int matchIndexForFollower;
                    LogEntry prevEntry;
                    boolean includeThroughput;
                    double combinedSystemThroughput = 0;
                    long now = System.currentTimeMillis();
                    // Taken before the RPC is sent: any response to this round
                    // proves leadership only for confirmations registered
                    // before this instant (ReadIndex).
                    final long sendStartNanos = System.nanoTime();

                    // ================= SNAPSHOT UNDER READ LOCK =================
                    lock.readLock().lock();
                    try {
                        if (status != ServerCurrentStatus.LEADER)
                            return;

                        // Heartbeat suppression
                        // if (nextIndex.get(followerId) == log.size() - 1 &&
                        // (now - lastHeartBeatSent[followerId]) < 100 &&
                        // lastIndexSent[followerId] == nextIndex.get(followerId)) {
                        // return;
                        // }

                        indexToSendFrom = nextIndex.get(followerId);

                        prevEntry = log.get(indexToSendFrom - 1);

                        endIndex = Math.min(
                                log.size(),
                                indexToSendFrom + MAX_ENTRIES_PER_RPC);

                        matchIndexForFollower = endIndex - 1;

                        includeThroughput = (now - lastThroughputSentTime.get(followerId) >= 200);
                        if (includeThroughput) {
                            lastThroughputSentTime.set(followerId, now);
                            double freshFollowerTps = getFreshFollowerReadThroughputSum(now);
                            combinedSystemThroughput = freshFollowerTps + getSystemWideThroughput();
                        }

                        lastHeartBeatSent.set(followerId, now);
                        lastIndexSent.set(followerId, indexToSendFrom);

                    } finally {
                        lock.readLock().unlock();
                    }

                    List<LogEntry> entriesSnapshot = log.logEntriesFromIndex(indexToSendFrom, endIndex);

                    List<LogEntryProto> protoEntries = convertLogEntryToProto(entriesSnapshot);

                    Log logProto = Log.newBuilder()
                            .addAllLog(protoEntries)
                            .build();

                    AppendEntriesArgument request = AppendEntriesArgument.newBuilder()
                            .setLeadersTerm(currentTerm.get())
                            .setLeadersId(serverId)
                            .setLeadersCommit(commitIndex.get())
                            .setPrevLogIndex(prevEntry.index)
                            .setPrevLogTerm(prevEntry.term)
                            .setEntriesToAppend(logProto)
                            .setTimeStamp(
                                    HybridClock.TimeStamp.convertToProto(hybridClock.now()))
                            .setSystemThroughputIncluded(includeThroughput)
                            .setSystemThroughput(combinedSystemThroughput)
                            .build();

                            if (shouldDropServerNetworkTraffic()) {
                            return;
                            }

                    stubs[followerId].appendEntries(
                            request,
                            new StreamObserver<AppendEntriesResult>() {

                                @Override
                                public void onNext(AppendEntriesResult result) {
                                    doesLeaderHasHighestTerm.compareAndSet(
                                            true,
                                            handleAppendEntriesResult(
                                                    result,
                                                    matchIndexForFollower,
                                                    indexToSendFrom));
                                    // ReadIndex: a response whose term does not
                                    // exceed ours means this follower accepted
                                    // our authority for this round.
                                    if (result.getCurrentTerm() <= request.getLeadersTerm()) {
                                        recordLeadershipAck(followerId, sendStartNanos, request.getLeadersTerm());
                                    }
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    LOG.warn(
                                            "AppendEntries RPC failed for follower {}",
                                            followerId,
                                            throwable);
                                }

                                @Override
                                public void onCompleted() {
                                }
                            });
                });
            }

        }, 0, 30, TimeUnit.MILLISECONDS);
    }

    // already in writeLock
    public void reinitialiseIndexes() {

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
        }
    }

    public void startElection() {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }

        lock.writeLock().lock();// Acquire the write lock for the entire election process
        RequestVoteArguments requestVoteArguments;
        try {
            // System.out.println(
            //         serverId + " is " + "Starting Election" + "Time in milli Seconds" + System.currentTimeMillis());
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
            lock.writeLock().unlock(); // Release the lock after the initial election setup
        }
        requestForVotes(requestVoteArguments);
    }

    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            // if it is not candidate that is might have become follower in between in
            // requestVote RPC we do not want to make the node leader in this case
            if (status != ServerCurrentStatus.CANDIDATE)
                return;
            doesLeaderHasHighestTerm.set(true);
            // System.out.println(serverId + " " + "Became the leader" + " The term is " + currentTerm.get());
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
        } finally {
            lock.writeLock().unlock();
        }
        sendAppendEntries();
    }

    private void processBatchWithErrorHandling() {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }
        long startNs = System.nanoTime();
        try {
            processBatch();
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
            // throw new RuntimeException("Error processing batch", e);
        } finally {
            long elapsedNs = System.nanoTime() - startNs;
            logProcessBatchDuration(elapsedNs / 1_000_000.0);
        }
    }

    private void logProcessBatchDuration(double durationMs) {
        String csvPath = "process_batch_duration_" + serverId + ".csv";
        long now = System.currentTimeMillis();
        try (FileWriter fw = new FileWriter(csvPath, true); PrintWriter out = new PrintWriter(fw)) {
            File file = new File(csvPath);
            if (file.length() == 0) {
                out.println("Timestamp,DurationMs");
            }
            out.printf("%d,%.4f%n", now, durationMs);
        } catch (IOException e) {
            System.err.println("Failed to write process batch duration: " + e.getMessage());
        }
    }

    // should be inside write lock
    private void becomeFollower() {
        // Restart election timer when stepping down from LEADER or CANDIDATE
        if (status == ServerCurrentStatus.LEADER || status == ServerCurrentStatus.CANDIDATE) {
            startTheElectionTimer();
        }
        // A node that is no longer leader can never confirm leadership.
        failPendingLeadershipConfirmations();
        // the status changes to follower
        status = ServerCurrentStatus.FOLLOWER;
        // we have to start the election timer because now it is a follower
        votedFor = -1;

        // cancelling the batch job
        // if (batchProcessingTask != null && !batchProcessingTask.isCancelled()) {
        //     batchProcessingTask.cancel(false); // false = don't interrupt if running
        //     batchProcessingTask = null;
        // }
        // stop leader’s heartbeat task immediately
        if (sendAppendEntriesScheduler != null && !sendAppendEntriesScheduler.isShutdown()) {
            sendAppendEntriesScheduler.shutdownNow();
            sendAppendEntriesScheduler = null;
        }
        // stop backlog tracking task
        if (backLogScheuler != null && !backLogScheuler.isShutdown()) {
            backLogScheuler.shutdownNow();
            backLogScheuler = null;
        }
    }

    private static volatile int MAX_BATCH_SIZE = 20000;

    private static final double TOKEN_COST_WRITE = 17.33;
    private static final double TOKEN_COST_READ_EVENTUAL = 1.0;
    private static final double TOKEN_COST_READ_CAUSAL_LOCAL = 1.24;
    private static final double TOKEN_COST_READ_CAUSAL_MAJORITY = 1.63;
    private static final double TOKEN_COST_READ_LINEARIZABLE = 16.25;

    private static final class TokenBucketSnapshot {
        final double currentServerThroughput;
        final double currentTps;
        final double currentTokens;
        final long lastUpdate;
        final int majority;
        final HashMap<Integer, Double> wcTpsMap;
        final HashMap<Integer, Double> wcLatencyMap;
        final HashMap<Integer, Double> readLatencyByKey;
        final double currentLatency;
        final boolean isLeader;

        private TokenBucketSnapshot(double currentServerThroughput,
                                   double currentTps,
                                   double currentTokens,
                                   long lastUpdate,
                                   int majority,
                                   HashMap<Integer, Double> wcTpsMap,
                                   HashMap<Integer, Double> wcLatencyMap,
                                   HashMap<Integer, Double> readLatencyByKey,
                                   double currentLatency,
                                   boolean isLeader) {
            this.currentServerThroughput = currentServerThroughput;
            this.currentTps = currentTps;
            this.currentTokens = currentTokens;
            this.lastUpdate = lastUpdate;
            this.majority = majority;
            this.wcTpsMap = wcTpsMap;
            this.wcLatencyMap = wcLatencyMap;
            this.readLatencyByKey = readLatencyByKey;
            this.currentLatency = currentLatency;
            this.isLeader = isLeader;
        }
    }

    private double estimateTokensUsedAtOriginalConsistency(List<TransactionOption> batch) {
        if (transactionBatchProcessor != null) {
            return transactionBatchProcessor.estimateTokenCostAtOriginalConsistency(batch);
        }

        double total = 0.0;
        for (TransactionOption option : batch) {
            if (option == null) {
                continue;
            }
            if (!option.isReadOnly) {
                total += TOKEN_COST_WRITE;
                continue;
            }
            ReadConcern rc = option.readConcern;
            if (rc == ReadConcern.CAUSAL) {
                ReadLevel rl = option.readLevel;
                if (rl == ReadLevel.MAJORITY) {
                    total += TOKEN_COST_READ_CAUSAL_MAJORITY;
                } else {
                    total += TOKEN_COST_READ_CAUSAL_LOCAL;
                }
            } else if (rc == ReadConcern.LINEARIZABLE) {
                total += TOKEN_COST_READ_LINEARIZABLE;
            } else {
                total += TOKEN_COST_READ_EVENTUAL;
            }
        }
        return total;
    }

    private double estimateTokenCostForAdmission(TransactionOption option) {
        if (option == null) {
            return 0.0;
        }
        if (transactionBatchProcessor != null) {
            return transactionBatchProcessor.estimateTokenCostAtOriginalConsistency(option);
        }
        if (!option.isReadOnly) {
            return TOKEN_COST_WRITE;
        }

        ReadConcern rc = option.readConcern;
        if (rc == ReadConcern.CAUSAL) {
            return option.readLevel == ReadLevel.MAJORITY
                    ? TOKEN_COST_READ_CAUSAL_MAJORITY
                    : TOKEN_COST_READ_CAUSAL_LOCAL;
        }
        if (rc == ReadConcern.LINEARIZABLE) {
            return TOKEN_COST_READ_LINEARIZABLE;
        }
        return TOKEN_COST_READ_EVENTUAL;
    }

    private TokenBucketSnapshot collectAndLogTokenBucketMetrics(List<TransactionOption> currentBatch,
                                                               double totalFollowerTps) {
        double currentServerThroughput = getSystemWideThroughput();
        double currentTps = getSystemWideThroughput() + totalFollowerTps;

        TokenBucketData tokenBucketData = tokenBucket.getCurrentTokenBucketData();
        double currentTokens = tokenBucketData.getTokenCount();
        long lastUpdate = tokenBucketData.getLastUpdateTime();

        int majority = (NUM_OF_SERVERS / 2) + 1;
        HashMap<Integer, Double> wcTpsMap = new HashMap<>();
        for (int wc = 1; wc <= majority; wc++) {
            wcTpsMap.put(wc, getWriteConcernTPS(wc));
        }

        HashMap<Integer, Double> wcLatencyMap = new HashMap<>();
        for (int wc = 1; wc <= majority; wc++) {
            wcLatencyMap.put(wc, blendedLatencyForWC(wc));
            // System.out.printf("Current blended latency for WC=%d is %.2f ms%n", wc, wcLatencyMap.get(wc));
        }

        // Store writeConcern latency data in avg_latencies_<sid>.csv
        try (FileWriter csvWriter = new FileWriter("avg_latencies_" + serverId + ".csv", true)) {
            for (int wc = 1; wc <= majority; wc++) {
                double avgLatency = wcLatencyMap.get(wc);
                csvWriter.write(String.format("%d,%.2f\n", wc, avgLatency));
            }
            csvWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read latency by key (used by BatchProcessor)
        HashMap<Integer, Double> readLatencyByKey = new HashMap<>();
        readLatencyByKey.put(RC_KEY_EVENTUAL_ALL,
                (double) getAverageReadConcernLatency(ReadConcern.EVENTUAL, ReadLevel.LOCAL));
        readLatencyByKey.put(RC_KEY_CAUSAL_LOCAL,
                (double) getAverageReadConcernLatency(ReadConcern.CAUSAL, ReadLevel.LOCAL));
        readLatencyByKey.put(RC_KEY_CAUSAL_MAJORITY,
                (double) getAverageReadConcernLatency(ReadConcern.CAUSAL, ReadLevel.MAJORITY));
        readLatencyByKey.put(RC_KEY_LINEARIZABLE_ALL,
                (double) getAverageReadConcernLatency(ReadConcern.LINEARIZABLE, ReadLevel.MAJORITY));

        // Store read latency breakdown in read_latencies_<sid>.csv
        try (FileWriter csvWriter = new FileWriter("read_latencies_" + serverId + ".csv", true)) {
            File file = new File("read_latencies_" + serverId + ".csv");
            if (file.length() == 0) {
                csvWriter.write("Timestamp,ReadLatencyKey,AvgLatencyMs\n");
            }
            long ts = System.currentTimeMillis();
            csvWriter.write(String.format("%d,%s,%.4f\n", ts, "EVENTUAL_ALL", readLatencyByKey.get(RC_KEY_EVENTUAL_ALL)));
            csvWriter.write(String.format("%d,%s,%.4f\n", ts, "CAUSAL_LOCAL", readLatencyByKey.get(RC_KEY_CAUSAL_LOCAL)));
            csvWriter.write(String.format("%d,%s,%.4f\n", ts, "CAUSAL_MAJORITY", readLatencyByKey.get(RC_KEY_CAUSAL_MAJORITY)));
            csvWriter.write(String.format("%d,%s,%.4f\n", ts, "LINEARIZABLE_ALL", readLatencyByKey.get(RC_KEY_LINEARIZABLE_ALL)));
            csvWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Record writeConcern TPS in CSV (single row per batch)
        try (FileWriter fw = new FileWriter("writeconcern_tps_" + serverId + ".csv", true);
             PrintWriter out = new PrintWriter(fw)) {
            File file = new File("writeconcern_tps_" + serverId + ".csv");
            if (file.length() == 0) {
                StringBuilder header = new StringBuilder("Timestamp");
                for (int wc = 1; wc <= majority; wc++) {
                    header.append(",WC").append(wc).append("_TPS");
                }
                header.append(",SystemTPS");
                out.println(header);
            }
            StringBuilder row = new StringBuilder();
            row.append(System.currentTimeMillis());
            for (int wc = 1; wc <= majority; wc++) {
                row.append(",").append(String.format("%.2f", wcTpsMap.get(wc)));
            }
            row.append(",").append(String.format("%.2f", currentTps));
            out.println(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
        double currentLatency;
        synchronized(systemWideLatency) {
            currentLatency = ((double) totalSystemWideLatency.get() / Math.max(1, countOfSystemWideLatencies.get()));
        }
        try (FileWriter sysWriter = new FileWriter("system_latency_" + serverId + ".csv", true);
             PrintWriter sysOut = new PrintWriter(sysWriter)) {
            File file = new File("system_latency_" + serverId + ".csv");
            if (file.length() == 0) {
                sysOut.println("Timestamp,SystemLatency");
            }
            sysOut.printf("%d,%.4f\n", System.currentTimeMillis(), currentLatency);
        } catch (IOException e) {
            System.err.println("Failed to write system latency to system_latency.csv: " + e.getMessage());
        }

        boolean isLeader = (currentLeader == serverId);
        return new TokenBucketSnapshot(
                currentServerThroughput,
                currentTps,
                currentTokens,
                lastUpdate,
                majority,
                wcTpsMap,
                wcLatencyMap,
                readLatencyByKey,
                currentLatency,
                isLeader);
    }

    private ProcessResult buildResultWithoutUpgrade(List<TransactionOption> currentBatch, double tokensUsed) {
        List<ClientMessage> messages = new ArrayList<>(currentBatch.size());
        double profit = 0.0;
        for (TransactionOption option : currentBatch) {
            if (option == null || option.clientMessage == null) {
                continue;
            }
            messages.add(option.clientMessage);
            if (option.clientMessage.hasT()) {
                profit += option.clientMessage.getT().getBaseProfit();
            }
        }
        return new ProcessResult(messages, tokensUsed, profit, 0, List.of(), tokensUsed);
    }

    private void recordSystemTpsIfLeader(double currentTps) {
        if (status != ServerCurrentStatus.LEADER) {
            return;
        }
        try (FileWriter fw = new FileWriter("system_tps_global.csv", true);
             PrintWriter out = new PrintWriter(fw)) {
            File file = new File("system_tps_global.csv");
            if (file.length() == 0) {
                out.println("Timestamp,SystemTPS,LeaderId");
            }
            out.printf("%d,%.2f,%d%n", System.currentTimeMillis(), currentTps, serverId);
        } catch (IOException e) {
            System.err.println("Failed to write leader system TPS: " + e.getMessage());
        }
    }

    private ProcessResult handleTokenBucketMetricsOnly(List<TransactionOption> currentBatch, double totalFollowerTps) {
        try {
            double currentServerThroughput = getSystemWideThroughput();
            double currentTps = currentServerThroughput + totalFollowerTps;
            recordSystemTpsIfLeader(currentTps);

            double currentLatency;
            synchronized (systemWideLatency) {
                currentLatency = ((double) totalSystemWideLatency.get()
                        / Math.max(1, countOfSystemWideLatencies.get()));
            }
            try (FileWriter sysWriter = new FileWriter("system_latency_" + serverId + ".csv", true);
                 PrintWriter sysOut = new PrintWriter(sysWriter)) {
                File file = new File("system_latency_" + serverId + ".csv");
                if (file.length() == 0) {
                    sysOut.println("Timestamp,SystemLatency");
                }
                sysOut.printf("%d,%.4f%n", System.currentTimeMillis(), currentLatency);
            } catch (IOException e) {
                System.err.println("Failed to write system latency to system_latency.csv: " + e.getMessage());
            }

            // Upgrades are disabled: execute exactly as received and do not touch token bucket state.
            double tokensUsed = estimateTokensUsedAtOriginalConsistency(currentBatch);
            ProcessResult result = buildResultWithoutUpgrade(currentBatch, tokensUsed);
            previousBatchResult = result;

            try (FileWriter fw = new FileWriter("tps_" + serverId + ".csv", true);
                 PrintWriter out = new PrintWriter(fw)) {
                File file = new File("tps_" + serverId + ".csv");
                if (file.length() == 0) {
                    out.println("Timestamp,Profit,CurrentTPS,CurrentTokens,TokensUsed,TransactionsUpgraded");
                }
                out.printf("%d,%.2f,%.2f,%s,%.2f,%d%n",
                        System.currentTimeMillis(),
                        result.profit,
                        currentServerThroughput,
                        "NA",
                        tokensUsed,
                        result.transactionsUpgraded);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private TransactionOption pollFreshTransaction() {
        batchLock.lock();
        try {
            return batchOfTransactions.poll();
        } finally {
            batchLock.unlock();
        }
    }

    private void processBatch() {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }

        // here the logic to process the current batch of transaction will come
        List<TransactionOption> currentBatch = new ArrayList<>();
        double totalFollowerTps = 0;

        final int cycleItemBudget = Math.min(MAX_BATCH_SIZE, MAX_ITEMS_PER_CYCLE);
        final long cycleStartNs = System.nanoTime();
        final long timeBudgetNs = TimeUnit.MILLISECONDS.toNanos(PROCESS_BATCH_TIME_BUDGET_MS);

        int count = 0;
        int backlogTaken = 0;
        int freshTaken = 0;

        System.out.println("Backlog size at start of batch processing: " + backLogQueue.size());

        while (count < cycleItemBudget) {
            // if ((System.nanoTime() - cycleStartNs) >= timeBudgetNs) {
            //     break;
            // }

            int targetBacklogByNow = (int) Math.ceil((count + 1) * BACKLOG_DRAIN_RATIO);
            boolean preferBacklog = backlogTaken < targetBacklogByNow;

            TransactionOption next = null;
            if (preferBacklog) {
                next = backLogQueue.poll();
                if (next != null) {
                    backlogTaken++;
                } else {
                    next = pollFreshTransaction();
                    if (next != null) {
                        freshTaken++;
                    }
                }
            } else {
                next = pollFreshTransaction();
                if (next != null) {
                    freshTaken++;
                } else {
                    next = backLogQueue.poll();
                    if (next != null) {
                        backlogTaken++;
                    }
                }
            }

            if (next == null) {
                break;
            }

            currentBatch.add(next);
            count++;
        }

        totalFollowerTps = getFreshFollowerReadThroughputSum(System.currentTimeMillis());

        // no need for processing if current batch is empty
        if (currentBatch.isEmpty())
            return;
        
        ProcessResult result = UPGRADE_TRANSACTIONS_ENABLED
            ? handleTokenBucket(currentBatch, totalFollowerTps)
            : handleTokenBucketMetricsOnly(currentBatch, totalFollowerTps);
        List<ClientMessage> transactionsToExecute = result.messages;

        // Record before/after upgrade transaction mix for this batch.
        recordBatchMixBeforeAfterUpgrade(currentBatch, transactionsToExecute);

        // Deferred transactions are already pushed to backlog queue inside BatchProcessor.
        List<ClientMessage> readTransactions = new ArrayList<>();
        List<ClientMessage> writeTransactions = new ArrayList<>();
        for (ClientMessage cm : transactionsToExecute) {
            if (cm.getT().getIsReadOnly() && cm.getT().getReadConcern() != ReadConcern.LINEARIZABLE) {
                readTransactions.add(cm);
            } else {
                writeTransactions.add(cm);
            }
        }

        // Process reads on a separate thread — EVENTUAL reads don't need Raft write lock,
        // CAUSAL reads just schedule an async task, LINEARIZABLE re-enqueues under batchLock.
        // This runs in PARALLEL with the write-lock section below.
        CompletableFuture<Map<String, List<AckMessage>>> readFuture = CompletableFuture.supplyAsync(() -> {
            Map<String, List<AckMessage>> readAcks = new HashMap<>();
            for (ClientMessage cm : readTransactions) {
                Ack readAck = processReadRequest(cm);
                if (readAck != null) {
                    // EVENTUAL read — collect ack messages
                    String clientId = getClientId(cm.getCallbackHost(), cm.getCallbackPort());
                    clientStubs.computeIfAbsent(clientId,
                            k -> createClientStub(cm.getCallbackHost(), cm.getCallbackPort()));
                    readAcks.computeIfAbsent(clientId, k -> new ArrayList<>())
                            .addAll(readAck.getAckMessageList());
                }
            }
            return readAcks;
        }, executorService);

        // Simultaneously append writes to the Raft log under write lock
        List<LogEntry> entry = new ArrayList<>();

        // for followers return directly
        if(status != ServerCurrentStatus.LEADER) {
            return;
        }

        lock.writeLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER)
                return;

            for (ClientMessage clientMessage : writeTransactions) {
                int index = log.size();

                if (clientMessage.hasTimeStamp()) {
                    hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(clientMessage.getTimeStamp()));
                }

                Transaction t = clientMessage.getT();
                int writeConcern = clientMessage.getWriteConcern();
                String host = clientMessage.getCallbackHost();
                int port = clientMessage.getCallbackPort();
                String id = t.getId();
                HybridClock.TimeStamp currentTimeStamp = hybridClock.now();
                log.append(new LogEntry(index, currentTerm.get(), t, writeConcern, currentTimeStamp, NUM_OF_SERVERS,
                        t.getTransactionArrivalTimeOnLeader(), host, port));

                kv.applyLocal(t, index);
                timeStampsInLog.put(currentTimeStamp, index);
                log.updateWriteConcern(index, serverId, ackTransactionTimeStampsForAllWriteConcerns,
                        writeConcernThroughputLocks, writeConcernLatencyLocks, writeConcernLatencies,
                        writeConcernLatencySum);
                ackSent.put(id, false);
                timeAtWhichTransactionWasReceived.put(id, System.currentTimeMillis());
                if (log.get(index).writeConcern == 0) {
                    entry.add(log.get(index));
                }
            }
        } catch (Exception e) {
            // System.out.println("Exception occurred while processing batch: " + e.getMessage());
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();

            // Wait for read processing to finish, then send all acks on background thread
            Map<String, List<AckMessage>> readAcksPerClient;
            try {
                readAcksPerClient = readFuture.join();
            } catch (Exception e) {
                // System.out.println("Error joining read processing: " + e.getMessage());
                readAcksPerClient = new HashMap<>();
            }

            final Map<String, List<AckMessage>> finalReadAcks = readAcksPerClient;
            executorService.submit(() -> {
                // Send write w:1 acks
                if (!entry.isEmpty()) {
                    // System.out.println("Sending ack for transactions with w:1, batch size: " + entry.size());
                    sendAckForEntries(entry).orTimeout(ACK_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS).exceptionally(ex -> {
                        return null;
                    });
                }
                // Send batched read EVENTUAL acks
                for (Map.Entry<String, List<AckMessage>> readEntry : finalReadAcks.entrySet()) {
                    String clientId = readEntry.getKey();
                    List<AckMessage> ackMessages = readEntry.getValue();
                    RaftStub stub = clientStubs.get(clientId);
                    if (stub != null && !ackMessages.isEmpty()) {
                        stub.sendAckToClient(Ack.newBuilder().addAllAckMessage(ackMessages).build(),
                                new StreamObserver<Empty>() {
                                    @Override public void onNext(Empty value) {}
                                    @Override public void onError(Throwable t) {}
                                    @Override public void onCompleted() {}
                                });
                    }
                }
            });
        }
    }

    private static final class BatchTransactionMix {
        int total;
        int totalReads;
        int eventualReads;
        int causalLocalReads;
        int causalMajorityReads;
        int linearizableReads;
        int totalWrites;
        int writeConcernOther;
        final int[] writeConcernCounts;

        BatchTransactionMix(int majority) {
            this.writeConcernCounts = new int[Math.max(majority + 1, 2)];
        }
    }

    private BatchTransactionMix summarizeClientMessages(List<ClientMessage> transactions, int majority) {
        BatchTransactionMix mix = new BatchTransactionMix(majority);
        for (ClientMessage cm : transactions) {
            if (cm == null || !cm.hasT()) {
                continue;
            }
            addTransactionToMix(mix, cm);
        }
        return mix;
    }

    private BatchTransactionMix summarizeTransactionOptions(List<TransactionOption> transactions, int majority) {
        BatchTransactionMix mix = new BatchTransactionMix(majority);
        for (TransactionOption tx : transactions) {
            if (tx == null || tx.clientMessage == null || !tx.clientMessage.hasT()) {
                continue;
            }
            addTransactionToMix(mix, tx.clientMessage);
        }
        return mix;
    }

    private void addTransactionToMix(BatchTransactionMix mix, ClientMessage cm) {
        mix.total++;

        Transaction t = cm.getT();
        if (t.getIsReadOnly()) {
            mix.totalReads++;
            ReadConcern rc = t.getReadConcern();
            ReadLevel rl = t.getReadLevel();

            if (rc == ReadConcern.CAUSAL) {
                if (rl == ReadLevel.MAJORITY) {
                    mix.causalMajorityReads++;
                } else {
                    mix.causalLocalReads++;
                }
            } else if (rc == ReadConcern.LINEARIZABLE) {
                mix.linearizableReads++;
            } else {
                mix.eventualReads++;
            }
            return;
        }

        mix.totalWrites++;
        int wc = cm.getWriteConcern();
        if (wc >= 1 && wc < mix.writeConcernCounts.length) {
            mix.writeConcernCounts[wc]++;
        } else {
            mix.writeConcernOther++;
        }
    }

    private void recordBatchMixBeforeAfterUpgrade(List<TransactionOption> beforeUpgrade,
            List<ClientMessage> afterUpgrade) {
        int majority = (NUM_OF_SERVERS / 2) + 1;
        BatchTransactionMix before = summarizeTransactionOptions(beforeUpgrade, majority);
        BatchTransactionMix after = summarizeClientMessages(afterUpgrade, majority);

        String csvPath = "batch_mix_before_after_" + serverId + ".csv";
        try (FileWriter fw = new FileWriter(csvPath, true);
                PrintWriter out = new PrintWriter(fw)) {
            File file = new File(csvPath);
            if (file.length() == 0) {
                StringBuilder header = new StringBuilder("Timestamp");
                appendMixHeader(header, "Before", majority);
                appendMixHeader(header, "After", majority);
                out.println(header);
            }

            StringBuilder row = new StringBuilder();
            row.append(System.currentTimeMillis());
            appendMixValues(row, before, majority);
            appendMixValues(row, after, majority);
            out.println(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void appendMixHeader(StringBuilder sb, String prefix, int majority) {
        sb.append(",")
                .append(prefix).append("Total")
                .append(",").append(prefix).append("Reads")
                .append(",").append(prefix).append("ReadEventual")
                .append(",").append(prefix).append("ReadCausalLocal")
                .append(",").append(prefix).append("ReadCausalMajority")
                .append(",").append(prefix).append("ReadLinearizable")
                .append(",").append(prefix).append("Writes");

        for (int wc = 1; wc <= majority; wc++) {
            sb.append(",").append(prefix).append("WriteWC").append(wc);
        }
        sb.append(",").append(prefix).append("WriteWCOther");
    }

    private void appendMixValues(StringBuilder sb, BatchTransactionMix mix, int majority) {
        sb.append(",").append(mix.total)
                .append(",").append(mix.totalReads)
                .append(",").append(mix.eventualReads)
                .append(",").append(mix.causalLocalReads)
                .append(",").append(mix.causalMajorityReads)
                .append(",").append(mix.linearizableReads)
                .append(",").append(mix.totalWrites);

        for (int wc = 1; wc <= majority; wc++) {
            sb.append(",").append(mix.writeConcernCounts[wc]);
        }
        sb.append(",").append(mix.writeConcernOther);
    }

    private double blendedLatencyForWC(int wc) {
        // it is a 5 second window based latency so it works well directly
        return getAverageLatency(wc);
    }

    private ProcessResult handleTokenBucket(List<TransactionOption> currentBatch,
            double totalFollowerTps) {
        // we can use lua scripts instead of using lock at application level
        redisLock.writeLock().lock();
        try {
            TokenBucketSnapshot snapshot = collectAndLogTokenBucketMetrics(currentBatch, totalFollowerTps);
            recordSystemTpsIfLeader(snapshot.currentTps);

            ProcessResult result;
            if (PRESSURE_MODE_ENABLED) {
                result = transactionBatchProcessor.processWithLatencyApplicationBasedHeuristic(
                        currentBatch,
                        snapshot.currentLatency,
                        snapshot.wcLatencyMap,
                        snapshot.wcTpsMap,
                        getIncomingTransactionsRate(),
                        backLogTransactions,
                        backlogTracker.isIncreasing(),
                        snapshot.currentTokens,
                        snapshot.readLatencyByKey,
                        snapshot.isLeader,
                        backLogQueue);
            } else {
                result = transactionBatchProcessor.processWithLatencyApplicationBasedHeuristicNoPressure(
                        currentBatch,
                        snapshot.currentLatency,
                        snapshot.wcLatencyMap,
                        snapshot.wcTpsMap,
                        getIncomingTransactionsRate(),
                        backLogTransactions,
                        backlogTracker.isIncreasing(),
                        snapshot.currentTokens,
                        snapshot.readLatencyByKey,
                        snapshot.isLeader,
                        backLogQueue);
            }

            // Save result for next RL prediction
            previousBatchResult = result;

            // Update the token count in Redis after batch processing
            double remainingTokens = Math.max(0, snapshot.currentTokens - result.tokensUsed);
            tokenBucket.updateTokens(remainingTokens, snapshot.lastUpdate);

            // System.out.printf(
            //         "\uD83D\uDE80 [Batch Result] Profit: %.2f | Current TPS: %.2f | Current Tokens: %.2f | Tokens Used: %.2f | Remaining Tokens: %.2f | Transactions Upgraded : %d%n",
            //             result.profit, snapshot.currentServerThroughput, snapshot.currentTokens, result.tokensUsed, remainingTokens, result.transactionsUpgraded);
            try (FileWriter fw = new FileWriter("tps_" + serverId + ".csv", true); PrintWriter out = new PrintWriter(fw)) {

                // Write header only if file is empty
                File file = new File("tps_" + serverId + ".csv");
                if (file.length() == 0) {
                    out.println("Timestamp,Profit,CurrentTPS,CurrentTokens,TokensUsed,TransactionsUpgraded");
                }
                // Write data row
                out.printf("%d,%.2f,%.2f,%.2f,%.2f,%d%n",
                        System.currentTimeMillis(),
                        result.profit, snapshot.currentServerThroughput, snapshot.currentTokens, result.totalTokensUsed,
                        result.transactionsUpgraded);
            } catch (IOException e) {
                e.printStackTrace();
            }

         

            return result;
        } catch (Exception e) {
            // System.out.println("Error in handleTokenBucket: " + e.getMessage());
            e.printStackTrace();
            return null;
        } finally {
            redisLock.writeLock().unlock();
        }
    }

    // this gives me the current rolling throughput, at the time of ack I add the
    // transactions timestamp
    private double getSystemWideThroughput() {
        // this can be accessed while sending ack also so we want to ensure that only
        // thread enters
        synchronized (systemWideThroughput) {
            Long currentTimeStamp = System.currentTimeMillis();
            recordThroughput(ackTransactionsTimeStamps, currentTimeStamp, false);
            return ackTransactionsTimeStamps.size() / 5;
        }
    }

    private double getWriteConcernTPS(int writeConcern) {
        synchronized (writeConcernThroughputLocks.get(writeConcern)) {
            Long currentTimeStamp = System.currentTimeMillis();
            ConcurrentLinkedQueue<Long> writeConcernSpecificTimeStamps = ackTransactionTimeStampsForAllWriteConcerns
                    .get(writeConcern);
            if (writeConcernSpecificTimeStamps != null) {
                recordThroughput(writeConcernSpecificTimeStamps, currentTimeStamp, false);
                return writeConcernSpecificTimeStamps.size() / 5; // 5-second window
            }
            return 0.0;
        }
    }

    private void initializeReadLatencyKey(int key) {
        readConcernLatencies.putIfAbsent(key, new ArrayDeque<>());
        readConcernLatencySum.putIfAbsent(key, 0L);
        readConcernLatencyLocks.putIfAbsent(key, new Object());
        prevReadConcernLatencies.putIfAbsent(key, 0.0);
    }

    private int getReadLatencyKey(ReadConcern readConcern, ReadLevel readLevel) {
        if (readConcern == ReadConcern.CAUSAL) {
            return readLevel == ReadLevel.MAJORITY ? RC_KEY_CAUSAL_MAJORITY : RC_KEY_CAUSAL_LOCAL;
        }
        if (readConcern == ReadConcern.LINEARIZABLE) {
            return RC_KEY_LINEARIZABLE_ALL;
        }
        return RC_KEY_EVENTUAL_ALL;
    }

    boolean canServeLinearizableRead() {
        long LEASE_DURATION_MS = 1900;
        long now = System.currentTimeMillis();
        int freshCount = 0;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i == serverId)
                continue;
            if (now - lastHeartBeatReceived[i].get() < LEASE_DURATION_MS) {
                freshCount++;
            }
        }
        return freshCount >= (NUM_OF_SERVERS / 2);
    }

    /**
     * Record latency for a specific ReadConcern (mirrors writeConcern latency tracking).
     * Uses a 5-second sliding window.
     */
    private void recordReadConcernLatency(ReadConcern readConcern, ReadLevel readLevel, long arrivalTime) {
        int key = getReadLatencyKey(readConcern, readLevel);
        initializeReadLatencyKey(key);
        synchronized (readConcernLatencyLocks.get(key)) {
            long now = System.currentTimeMillis();
            long currentLatency = now - arrivalTime;
            Deque<Latency> latencies = readConcernLatencies.get(key);
            while (!latencies.isEmpty() && (now - latencies.peek().timestamp) >= 5000L) {
                Latency old = latencies.poll();
                readConcernLatencySum.put(key, readConcernLatencySum.get(key) - old.latency);
            }
            latencies.add(new Latency(now, currentLatency));
            readConcernLatencySum.put(key, readConcernLatencySum.get(key) + currentLatency);
        }
    }

    public long getAverageReadConcernLatency(ReadConcern readConcern, ReadLevel readLevel) {
        int key = getReadLatencyKey(readConcern, readLevel);
        initializeReadLatencyKey(key);
        synchronized (readConcernLatencyLocks.get(key)) {
            long val = readConcernLatencySum.get(key)
                    / Math.max(readConcernLatencies.get(key).size(), 1);
            if (val == 0) {
                return prevReadConcernLatencies.getOrDefault(key, 0.0).longValue();
            } else {
                prevReadConcernLatencies.put(key, (double) val);
            }
            return val;
        }
    }

    private Ack buildReadAck(ReadConcern readConcern, ReadLevel readLevel, String key, String id) {
        KvStore.Versioned versioned = (readLevel == ReadLevel.LOCAL)
                ? kv.readLocal(key)
                : kv.readCommitted(key);
        return Ack.newBuilder().addAckMessage(AckMessage.newBuilder()
                .setReadValue(versioned == null ? "" : versioned.value())
                .setId(id)
                .setExecutedLevelIncluded(true)
                .setExecutedReadConcern(readConcern)
                .setExecutedReadLevel(readLevel)
                .build()).build();
    }

    private Ack buildFailureAck(String id) {
        return Ack.newBuilder()
                .addAckMessage(AckMessage.newBuilder().setFailure(true).setId(id).build())
                .build();
    }

    /**
     * Process a read request from the batch.
     * Returns an Ack for immediate reads (EVENTUAL), or null when the result
     * is sent asynchronously (CAUSAL via callback) or goes through log replication (LINEARIZABLE).
     */
    public Ack processReadRequest(ClientMessage cm) {
        Transaction t = cm.getT();
        String key = t.getKey();
        String id = t.getId();
        ReadLevel readLevel = t.getReadLevel();
        String host = cm.getCallbackHost();
        int port = cm.getCallbackPort();
        ReadConcern readConcern = t.getReadConcern();
        long arrivalTime = t.getTransactionArrivalTimeOnLeader();

        if (shouldDropServerNetworkTraffic()) {
            return buildFailureAck(id);
        }

        int majority = (NUM_OF_SERVERS / 2) + 1;

        if (readConcern == ReadConcern.CAUSAL) {
            // Causal: async wait for log to catch up to client's timestamp, then ack via callback
            HybridClock.TimeStamp timeStampRequestedByClient = HybridClock.TimeStamp
                    .convertToTimeStamp(cm.getTimeStamp());

            long startTime = System.nanoTime();
            long maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(400);

            String clientId = getClientId(host, port);
            RaftStub clientStub = clientStubs.computeIfAbsent(clientId, k -> createClientStub(host, port));

            Runnable checkLogTask = new Runnable() {
                @Override
                public void run() {
                    if (shouldDropServerNetworkTraffic()) {
                        clientStub.sendAckToClient(buildFailureAck(id), new StreamObserver<Empty>() {
                            @Override public void onNext(Empty value) {}
                            @Override public void onError(Throwable t) {}
                            @Override public void onCompleted() {}
                        });
                        return;
                    }

                    if (System.nanoTime() - startTime > maxWaitNanos) {
                        // Timeout — record latency and send failure ack to client
                        long failNow = System.currentTimeMillis();
                        synchronized (systemWideLatency) {
                            long latencyOfThisTransaction = failNow - arrivalTime;
                            while (!systemWideLatencies.isEmpty()
                                    && (failNow - systemWideLatencies.peek().timestamp) >= 1000L) {
                                Latency latency = systemWideLatencies.poll();
                                Long systemLatency = totalSystemWideLatency.get();
                                totalSystemWideLatency.set(systemLatency - latency.latency);
                                countOfSystemWideLatencies.decrementAndGet();
                            }
                            systemWideLatencies.add(new Latency(failNow, latencyOfThisTransaction));
                            Long systemLatency = totalSystemWideLatency.get();
                            totalSystemWideLatency.set(systemLatency + latencyOfThisTransaction);
                            countOfSystemWideLatencies.incrementAndGet();
                                // System.out.printf("📥 [Incoming Transactions] Server %d | Transactions/sec: %d | Time: %s%n", serverId,
                                //         incomingTPS,
                                //         new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(currentTime)));
                        }
                        Ack failAck = buildFailureAck(id);
                        clientStub.sendAckToClient(failAck, new StreamObserver<Empty>() {
                            @Override public void onNext(Empty value) {}
                            @Override public void onError(Throwable t) {}
                            @Override public void onCompleted() {}
                        });
                        return;
                    }
                    LogEntry entry = null;
                    lock.readLock().lock();
                    try {
                        if (readLevel == ReadLevel.MAJORITY) {
                            entry = log.get(commitIndex.get());
                        } else {
                            entry = log.get(log.size() - 1);
                        }
                    } finally {
                        lock.readLock().unlock();
                    }
                    if (entry != null && entry.timeStamp.compareTo(timeStampRequestedByClient) >= 0) {
                        // Log has caught up — serve the read
                        long now = System.currentTimeMillis();
                        synchronized (systemWideThroughput) {
                            recordThroughput(ackTransactionsTimeStamps, now, true);
                        }
                        synchronized (systemWideLatency) {
                            long latencyOfThisTransaction = now - arrivalTime;
                            while (!systemWideLatencies.isEmpty()
                                    && (now - systemWideLatencies.peek().timestamp) >= 1000L) {
                                Latency latency = systemWideLatencies.poll();
                                Long systemLatency = totalSystemWideLatency.get();
                                totalSystemWideLatency.set(systemLatency - latency.latency);
                                countOfSystemWideLatencies.decrementAndGet();
                            }
                            systemWideLatencies.add(new Latency(now, latencyOfThisTransaction));
                            Long systemLatency = totalSystemWideLatency.get();
                            totalSystemWideLatency.set(systemLatency + latencyOfThisTransaction);
                            countOfSystemWideLatencies.incrementAndGet();
                        }
                        recordReadConcernLatency(readConcern, readLevel, arrivalTime);
                        Ack ack = buildReadAck(readConcern, readLevel, key, id);
                        clientStub.sendAckToClient(ack, new StreamObserver<Empty>() {
                            @Override public void onNext(Empty value) {}
                            @Override public void onError(Throwable t) {}
                            @Override public void onCompleted() {}
                        });
                    } else {
                        // Still behind, retry after 20ms
                        causalReadScheduler.schedule(this, 20, TimeUnit.MILLISECONDS);
                    }
                }
            };
            causalReadScheduler.execute(checkLogTask);
            return null; // ack sent asynchronously via callback

        } else if (readConcern == ReadConcern.LINEARIZABLE) {
            // Linearizable reads must go through the Raft log with writeConcern = majority.
            // Only the true leader (with quorum) can commit — a stale leader in a
            // network partition will never get majority acks, so the read won't be served.
            lock.readLock().lock();
            try {
                if (status != ServerCurrentStatus.LEADER) {
                    // not the leader, can't handle linearizable read
                    return null;
                }
            } finally {
                lock.readLock().unlock();
            }
            batchLock.lock();
            try {
                ClientMessage clientMessage = ClientMessage.newBuilder().setWriteConcern(majority)
                        .setT(Transaction.newBuilder().setId(id).setIsReadOnly(true).setWriteConcern(majority)
                                .setMinRequiredConsistency(majority).setKey(key)
                                .setReadConcern(readConcern).setReadLevel(readLevel)
                                .setTransactionArrivalTimeOnLeader(arrivalTime).build())
                        .setCallbackHost(host).setCallbackPort(port).build();
                batchOfTransactions.add(TransactionOption.fromClientMessage(clientMessage));
            } finally {
                batchLock.unlock();
            }
            // Latency recorded when the ack is actually sent after majority replication
            return null;

        } else {
            // EVENTUAL: serve immediately from latest local state
            long now = System.currentTimeMillis();
            synchronized (systemWideThroughput) {
                recordThroughput(ackTransactionsTimeStamps, now, true);
            }
            synchronized (systemWideLatency) {
                long latencyOfThisTransaction = now - arrivalTime;
                while (!systemWideLatencies.isEmpty()
                        && (now - systemWideLatencies.peek().timestamp) >= 1000L) {
                    Latency latency = systemWideLatencies.poll();
                    Long systemLatency = totalSystemWideLatency.get();
                    totalSystemWideLatency.set(systemLatency - latency.latency);
                    countOfSystemWideLatencies.decrementAndGet();
                }
                systemWideLatencies.add(new Latency(now, latencyOfThisTransaction));
                Long systemLatency = totalSystemWideLatency.get();
                totalSystemWideLatency.set(systemLatency + latencyOfThisTransaction);
                countOfSystemWideLatencies.incrementAndGet();
            }
            recordReadConcernLatency(readConcern, readLevel, arrivalTime);
            return buildReadAck(readConcern, readLevel, key, id);
        }
    }

    // this resets and starts the timer again
    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }

    // ===== ReadIndex leadership confirmation (Chameleon stage 1) =====

    /** Thrown when a leader-only operation is invoked on a non-leader. */
    public static final class NotLeaderException extends Exception {
        public NotLeaderException(String message) {
            super(message);
        }
    }

    private static final long CONFIRM_LEADERSHIP_TIMEOUT_MS = 1000;

    private static final class LeadershipConfirmation {
        final int term;
        final int readIndex;
        final long registeredAtNanos;
        final Set<Integer> ackedFollowers = ConcurrentHashMap.newKeySet();
        final CompletableFuture<Integer> future = new CompletableFuture<>();

        LeadershipConfirmation(int term, int readIndex, long registeredAtNanos) {
            this.term = term;
            this.readIndex = readIndex;
            this.registeredAtNanos = registeredAtNanos;
        }
    }

    private final ConcurrentLinkedQueue<LeadershipConfirmation> pendingLeadershipConfirmations = new ConcurrentLinkedQueue<>();

    /**
     * ReadIndex (linearizable reads without log entries): snapshot the commit
     * index, then confirm this node is still the leader by observing a majority
     * of followers acknowledge an AppendEntries round that started after this
     * call. The returned future completes with the snapshotted commit index;
     * the caller can then serve the read from local committed state. No log
     * entry is created, and the confirmation rides the regular replication
     * heartbeats already in flight.
     *
     * The committed KV view is applied in the same critical sections that
     * advance the commit index, so once the future completes the local
     * committed view already covers the returned index and no apply-wait is
     * needed.
     */
    public CompletableFuture<Integer> confirmLeadership() {
        lock.readLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER) {
                return CompletableFuture.failedFuture(
                        new NotLeaderException("server " + serverId + " is not the leader"));
            }
            int readIndex = commitIndex.get();
            if (NUM_OF_SERVERS == 1) {
                return CompletableFuture.completedFuture(readIndex);
            }
            LeadershipConfirmation confirmation = new LeadershipConfirmation(
                    currentTerm.get(), readIndex, System.nanoTime());
            pendingLeadershipConfirmations.add(confirmation);
            causalReadScheduler.schedule(() -> {
                if (pendingLeadershipConfirmations.remove(confirmation)) {
                    confirmation.future.completeExceptionally(new TimeoutException(
                            "no majority heartbeat round within " + CONFIRM_LEADERSHIP_TIMEOUT_MS + " ms"));
                }
            }, CONFIRM_LEADERSHIP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return confirmation.future;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recordLeadershipAck(int followerId, long sendStartNanos, int termAtSend) {
        if (pendingLeadershipConfirmations.isEmpty()) {
            return;
        }
        int majority = (NUM_OF_SERVERS / 2) + 1;
        for (LeadershipConfirmation confirmation : pendingLeadershipConfirmations) {
            // Only rounds started after registration prove leadership at (or
            // after) the moment the read index was snapshotted.
            if (confirmation.term != termAtSend || sendStartNanos < confirmation.registeredAtNanos) {
                continue;
            }
            confirmation.ackedFollowers.add(followerId);
            // +1 counts this node itself.
            if (confirmation.ackedFollowers.size() + 1 >= majority
                    && pendingLeadershipConfirmations.remove(confirmation)) {
                confirmation.future.complete(confirmation.readIndex);
            }
        }
    }

    private void failPendingLeadershipConfirmations() {
        LeadershipConfirmation confirmation;
        while ((confirmation = pendingLeadershipConfirmations.poll()) != null) {
            confirmation.future.completeExceptionally(new NotLeaderException("stepped down from leadership"));
        }
    }

    /**
     * Stop all background activity (election timer, replication and batch
     * schedulers, worker pool) and close every gRPC channel this node opened.
     * Used by tests and orderly teardown; the node is unusable afterwards.
     */
    public void shutdown() {
        dropAllServerNetworkTraffic = true;
        electionTimer.stop();
        electionTimer.shutdown();
        failPendingLeadershipConfirmations();
        if (batchProcessor != null) {
            batchProcessor.shutdownNow();
        }
        if (sendAppendEntriesScheduler != null) {
            sendAppendEntriesScheduler.shutdownNow();
        }
        if (causalReadScheduler != null) {
            causalReadScheduler.shutdownNow();
        }
        if (backLogScheuler != null) {
            backLogScheuler.shutdownNow();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
        synchronized (ownedChannels) {
            for (ManagedChannel channel : ownedChannels) {
                channel.shutdownNow();
            }
        }
    }
}
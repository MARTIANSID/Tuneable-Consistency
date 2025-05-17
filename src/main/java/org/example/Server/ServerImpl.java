package org.example.Server;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.*;
import org.example.Timer.CustomTimer;

import java.util.stream.Collectors;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.ds.paxos.RaftGrpc.*;

import org.example.TokenBucket.TokenBucketImpl;
import org.example.Utility.HybridClock;
import org.example.Utility.LogEntry;
import org.example.Utility.RaftLog;
import org.example.Utility.ServerStatus.*;
import org.example.TokenBucket.TokenBucketImpl.TokenBucketData;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class ServerImpl extends RaftGrpc.RaftImplBase {
    int NUM_OF_SERVERS = 5;
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

    ReadWriteLock ackLock;
    // private TokenBucketGrpc.TokenBucketBlockingStub tokenBucketStub;
    // private final double TOKEN_REFILL_RATE_PER_MS = 1; // 1 token/sec

    AtomicInteger totalTransactions;

    ConcurrentLinkedQueue<Long> ackTransactionsTimeStamps;

    private final Object metricCalculations;

    ReadWriteLock redisLock;

    TokenBucketImpl tokenBucket;

    private long serverStartTimeMs;
    private AtomicLong totalAckedTransactions = new AtomicLong(0);




    //Knapsack
    private static class QueuedTransaction {
        Transaction txn;
        int minRequiredConsistency;
        double baseProfit;
        double extraProfitOnMajority;
    
        public QueuedTransaction(Transaction txn, int minRequiredConsistency, double baseProfit, double extraProfitOnMajority) {
            this.txn = txn;
            this.minRequiredConsistency = minRequiredConsistency;
            this.baseProfit = baseProfit;
            this.extraProfitOnMajority = extraProfitOnMajority;
        }
    }
    
    private LinkedBlockingQueue<QueuedTransaction> incomingTransactions = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService batchProcessor = Executors.newScheduledThreadPool(1);
    private static final int BATCH_INTERVAL_MS = 200; // batch interval
    private static final double COST_W1 = 0.5;
    private static final double COST_MAJORITY = 1.0;
    private static final int MIN_REQUIRED_THROUGHPUT = 55; // Target TPS7


    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = new ConcurrentHashMap<>();
        this.commitIndex = new AtomicInteger(-1);
        this.lastApplied = new AtomicInteger(-1);
        this.serverId = serverId;
        this.electionTimer = new CustomTimer(() -> startElection(), (new Random().nextInt(200) + 300), TimeUnit.MILLISECONDS);
        this.peers = new ArrayList<>();
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);
        this.doesLeaderHasHighestTerm = false;
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
        this.ackTransactionsTimeStamps = new ConcurrentLinkedQueue<>();
        this.metricCalculations = new Object();
        this.redisLock = new ReentrantReadWriteLock();
        this.tokenBucket = new TokenBucketImpl("127.0.0.1", 6379);
        this.serverStartTimeMs = System.currentTimeMillis();

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
        }
        // setting up the client stub
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        clientStub = RaftGrpc.newStub(channel);

         // Initialize token bucket stub (assuming token server is on port 8500)
        //  ManagedChannel tokenChannel = ManagedChannelBuilder.forAddress("localhost", 8500).usePlaintext().build();
        //  tokenBucketStub = TokenBucketGrpc.newBlockingStub(tokenChannel);

        batchProcessor.scheduleAtFixedRate(this::processBatch, 0, BATCH_INTERVAL_MS, TimeUnit.MILLISECONDS);

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
            // update clock of follower using leaders clock, if the follower is behind it can catchup
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
                if (status == ServerCurrentStatus.LEADER) {
                    // if this node was leader we need to start the timer
                    startTheElectionTimer();
                }
                // changing the state to follower because higher term node is found, that is trying to become leader
                status = ServerCurrentStatus.FOLLOWER;
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
            // can use a blocking stub here
            blockingStubs[currentLeader].sendTransaction(clientMessage);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        Transaction t = clientMessage.getT();
        int writeConcern = clientMessage.getWriteConcern();
        String appType = t.getAppType();
    
        double baseProfit = 0, extraProfit = 0;
        int minConsistency = writeConcern;
    
        if (appType.equals("like")) {
            baseProfit = 1;
            extraProfit = 1; // if upgraded to majority
        } else if (appType.equals("comment")) {
            baseProfit = 1;
            extraProfit = 2;
        } else if (appType.equals("post")) {
            baseProfit = 2;
            extraProfit = 0; // post already needs majority
        }
    
        incomingTransactions.add(new QueuedTransaction(t, minConsistency, baseProfit, extraProfit));
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

//         String id = t.getId();

// //        if (tIdToLogIndex.containsKey(id)) {
// //            int logIndex = tIdToLogIndex.get(id);
// //            if (commitIndex.get() >= logIndex) {
// //                // already committed so just send the ack
// //            } else if (isWriteConcernStatisfied()) {
// //            }
// //            // now here if the writeConcernStatisfied does not have the data basically it is a replica then the replica will update the writeConcern done on its end using appendEntries
// //            responseObserver.onNext(Empty.newBuilder().build());
// //            responseObserver.onCompleted();
// //            return;
// //        }
//         int writeConcern = clientMessage.getWriteConcern();



//         //TOKEN BUCKET ALGORITHM:
//         // double requiredTokens = (writeConcern == 1) ? 30 : 80;
//         // TokenConsumeResponse tokenResp = tokenBucketStub.consumeTokens(
//         // TokenConsumeRequest.newBuilder()
//         //     .setRequiredTokens(requiredTokens)
//         //     .build()
//         //  );

//         // if (!tokenResp.getGranted()) {
//         //     System.out.println("Not enough tokens. Dropping or queuing txn: " + id);
//         //     responseObserver.onNext(Empty.newBuilder().build());
//         //     responseObserver.onCompleted();
//         //     return;
//         // }



//         int index = -1;
//         // we want it to be synchronized in order to get the correct index, and not allow multiple threads to get same index
//         // log.size() takes in only read lock hence we need synchronized
//         List<LogEntry> entry = new ArrayList<>();
//         lock.writeLock().lock();
//         try {
//             System.out.println("Got the transaction!");

//             // adding the logic of Token Bucket
//             redisLock.writeLock().lock();
//             try {
//                 // get the token data
//                 TokenBucketData tokenBucketData = tokenBucket.getCurrentTokenBucketData();

//                 double currentTokens = tokenBucketData.getTokenCount();
//                 long lastUpdateTime = tokenBucketData.getLastUpdateTime();


//                 System.out.println("The current tokens are--" + currentTokens + " The last update time is--" + lastUpdateTime);


//                 if (currentTokens <= 0) {
//                     System.out.println("Throttling the request");
//                     responseObserver.onNext(Empty.newBuilder().build());
//                     responseObserver.onCompleted();
//                     return;
//                 }
//                 // update the token count in redis along with lastUpdateTime
//                 tokenBucket.updateTokens(currentTokens - 1, lastUpdateTime);
//             } finally {
//                 redisLock.writeLock().unlock();
//             }

//             index = log.size();

//             if (clientMessage.hasTimeStamp()) {
//                 // updating the clock of leader using the time stamp sent by the client
//                 hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(clientMessage.getTimeStamp()));
//             }

//             // we do update the clock of leader using followers
//             HybridClock.TimeStamp currentTimeStamp = hybridClock.now();
//             // appending the entry in log
//             log.append(new LogEntry(index, currentTerm.get(), t, writeConcern, currentTimeStamp, NUM_OF_SERVERS));
//             updateBalances(t, clientBalancesLatest);
//             // we need this to implement causal consistency
//             timeStampsInLog.put(currentTimeStamp, index);
//             // since this is in right lock only updated entry will be sent to the followers
//             log.updateWriteConcern(index, serverId);

//             ackSent.put(id, false);
//             timeAtWhichTransactionWasReceived.put(id, System.currentTimeMillis());
//             // if write concern becomes 0 we will send ack to client
//             if (log.get(index).writeConcern == 0) {
//                 entry.add(log.get(index));
//             }
//             System.out.println(index);
//         } finally {
//             totalAcks.put(index, 1);
//             tIdToLogIndex.put(id, index);

//             lock.writeLock().unlock();

//             CountDownLatch latch = new CountDownLatch(1);
//             if (!entry.isEmpty()) {
//                 sendAckForEntries(entry, latch);
//             } else {
//                 latch.countDown();
//             }

//             try {
//                 latch.await(200, TimeUnit.MILLISECONDS);
//             } catch (InterruptedException e) {
//                 throw new RuntimeException(e);
//             }
//         }
//         responseObserver.onNext(Empty.newBuilder().build());
//         responseObserver.onCompleted();
//     }

private void processBatch() {
    List<QueuedTransaction> batch = new ArrayList<>();
    incomingTransactions.drainTo(batch);

    if (batch.isEmpty()) return;

    redisLock.writeLock().lock();
    try {
        System.out.println(System.currentTimeMillis());

        double currentTps = getCurrentRollingThroughput();
        TokenBucketData tb = tokenBucket.getCurrentTokenBucketData();
        double rawTokens = tb.getTokenCount();
        long lastUpdate = tb.getLastUpdateTime();
        int maxTokens = (int)Math.floor(rawTokens * 2);
        long now = System.currentTimeMillis();
        double avgThroughput = (double) totalAckedTransactions.get() * 1000 / (now - serverStartTimeMs);
        boolean throughputLow = avgThroughput < MIN_REQUIRED_THROUGHPUT;

    
        int n = batch.size();
        int R = (int)Math.ceil(MIN_REQUIRED_THROUGHPUT * BATCH_INTERVAL_MS / 1000.0);
        R = Math.min(R, n); 
        
        
        


        // 3) Build cost & profit arrays
         // build cost & profit arrays (same as you have)…
        int[] costMin = new int[n], costMaj = new int[n];
        double[] profMin = new double[n], profMaj = new double[n];
        // for (int i = 0; i < n; i++) {
        // QueuedTransaction qt = batch.get(i);
        // costMin[i] = qt.minRequiredConsistency == 1 ? 1 : 2;
        // costMaj[i] = 2;
        // profMin[i] = qt.baseProfit;
        // profMaj[i] = qt.baseProfit + qt.extraProfitOnMajority;
        // }

        for (int i = 0; i < n; i++) {
            QueuedTransaction qt = batch.get(i);
            costMin[i] = qt.minRequiredConsistency == 1 ? 1 : 2;
            costMaj[i] = 2;
            profMin[i] = qt.baseProfit;
            profMaj[i] = qt.baseProfit + qt.extraProfitOnMajority;

            if (throughputLow) {
                // Force no upgrades if throughput is low
                if (qt.minRequiredConsistency == 1) {
                    costMaj[i] = Integer.MAX_VALUE;
                    profMaj[i] = Double.NEGATIVE_INFINITY;
                }
            }
        }

        // 4) DP table: dp[i][j][w] = max profit using first i txns, picking exactly j txns, spending w units
        double[][][] dp = new double[n+1][n+1][maxTokens+1];
        int[][][] choice = new int[n+1][n+1][maxTokens+1]; 
        //   choice = 0→skip, 1→takeMin, 2→takeMaj

        // initialize to -∞, except dp[0][0][0] = 0
        for (int i = 0; i <= n; i++)
            for (int j = 0; j <= n; j++)
                Arrays.fill(dp[i][j], Double.NEGATIVE_INFINITY);
        dp[0][0][0] = 0;

        // 5) Fill DP
        for (int i = 1; i <= n; i++) {
            for (int j = 0; j <= i; j++) {
                for (int w = 0; w <= maxTokens; w++) {
                    // SKIP
                    dp[i][j][w] = dp[i-1][j][w];
                    choice[i][j][w] = 0;

                    // TAKE at min consistency
                    if (j > 0 && w >= costMin[i-1] 
                        && dp[i-1][j-1][w-costMin[i-1]] > Double.NEGATIVE_INFINITY) {
                        double cand = dp[i-1][j-1][w-costMin[i-1]] + profMin[i-1];
                        if (cand > dp[i][j][w]) {
                            dp[i][j][w] = cand;
                            choice[i][j][w] = 1;
                        }
                    }
                    // TAKE upgraded to majority (only if original min was 1)
                    if (batch.get(i-1).minRequiredConsistency == 1 
                        && j > 0 && w >= costMaj[i-1]
                        && dp[i-1][j-1][w-costMaj[i-1]] > Double.NEGATIVE_INFINITY) {
                        double cand = dp[i-1][j-1][w-costMaj[i-1]] + profMaj[i-1];
                        if (cand > dp[i][j][w]) {
                            dp[i][j][w] = cand;
                            choice[i][j][w] = 2;
                        }
                    }
                }
            }
        }

        // 6) Pick the best over j ≥ R (you can pick beyond R to maximize profit)
        double bestProfit = Double.NEGATIVE_INFINITY;
        int bestJ = R, bestW = 0;
        for (int j = R; j <= n; j++) {
            for (int w = 0; w <= maxTokens; w++) {
                if (dp[n][j][w] > bestProfit) {
                    bestProfit = dp[n][j][w];
                    bestJ = j;
                    bestW = w;
                }
            }
        }

        // 7) Backtrack to find which items & at what consistency
// new—use the JDK's Map.Entry as your tuple type
        List<Map.Entry<QueuedTransaction,Integer>> chosen = new ArrayList<>();
        int i = n, j = bestJ, w = bestW;
        while (i > 0) {
            int ch = choice[i][j][w];
            if (ch == 1 || ch == 2) {
                QueuedTransaction qt = batch.get(i-1);
                int execConsistency = (ch == 1 ? qt.minRequiredConsistency : 2);
                chosen.add(new AbstractMap.SimpleEntry<>(qt, execConsistency));
                w -= (ch==1 ? costMin[i-1] : costMaj[i-1]);
                j--;
            }
            i--;
        }
        Collections.reverse(chosen);

        // 8) Any not chosen get requeued
        Set<QueuedTransaction> selSet = chosen.stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    List<QueuedTransaction> leftovers = batch.stream()
        .filter(qt -> !selSet.contains(qt))
        .collect(Collectors.toList());


        // 9) Actually execute everything we chose
        long upgrades = chosen.stream()
        .filter(e -> e.getValue() == 2)
        .count();

        // actually execute
        double tokensUsed = bestW / 2.0;
        for (var e : chosen) {
        actuallyAppendTransaction(e.getKey().txn, e.getValue());
        }

        // requeue any you skipped
        incomingTransactions.addAll(leftovers);

        // write back bucket
        double newTokenCount = rawTokens - tokensUsed;
        tokenBucket.updateTokens(newTokenCount, lastUpdate);

        // **NEW**: a single “batch summary” line with only the fields you care about
        System.out.printf(
            "📦 BatchSummary: picked %d/%d (upgrades=%d), profit=%.1f, tokensUsed=%.1f→left=%.1f, avg=%.1f tps, rolling=%.1f tps%n",
            chosen.size(),      // # executed
            n,                  // batch size
            upgrades,           // how many got bumped to majority
            bestProfit,         // total profit
            tokensUsed,         // tokens consumed
            newTokenCount,      // tokens left
            avgThroughput,      // average tps
            currentTps          // rolling tps
        );


    } finally {
        redisLock.writeLock().unlock();
    }
} 





    private void actuallyAppendTransaction(Transaction txn, int writeConcern) {
        lock.writeLock().lock();
        try {
            int index = log.size();
            HybridClock.TimeStamp currentTimeStamp = hybridClock.now();
            log.append(new LogEntry(index, currentTerm.get(), txn, writeConcern, currentTimeStamp, NUM_OF_SERVERS));
            updateBalances(txn, clientBalancesLatest);
            timeStampsInLog.put(currentTimeStamp, index);
            log.updateWriteConcern(index, serverId);
            tIdToLogIndex.put(txn.getId(), index);
            ackSent.put(txn.getId(), false);
            timeAtWhichTransactionWasReceived.put(txn.getId(), System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }
    

    private double getCost(int consistency) {
        return (consistency == 1) ? COST_W1 : COST_MAJORITY;
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

        for (int i = 0; i < NUM_OF_SERVERS; i++) {

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
                    } else if (votes.get() >= (NUM_OF_SERVERS / 2)) {
                        // majority is reached here, no need to continue the election
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
            result.add(LogEntryProto.newBuilder().setLogIndex(entry.index).setT(entry.t).setTerm(entry.term).addAllServersThatReplicatedThisEntry(entry.serversThatReplicatedThisEntry).setWriteConcern(entry.writeConcern).setTimeStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).build());
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

        // 5 servers
        int index = NUM_OF_SERVERS - 1;


        while (index >= 0) {
            // traverse through all the indexes which are equal to this current index
            int currentIndex = sortedMatchIndex[index], cnt = 0, val = index;

            // currentIndex can be < 0, that means we don't have matchIndex for this follower
            if (currentIndex == -1) return -1;


            while (index > 0 && sortedMatchIndex[index] == currentIndex) {
                cnt++;
                index--;
            }

            // (4 - val) is there because the indexes on the right hand side of the current index support this index if not equal to -1
            if ((cnt + (4 - val)) >= 2 && log.get(currentIndex).term == currentTerm.get()) {
                // we return because we want the best index (array is sorted), that is the biggest index
                return currentIndex;
            }
        }
        // if no commitIndex is possible
        return -1;
    }

    // it is inside write lock
    private boolean checkIfIndexIsAValidCommitIndex(int index) {

        if (index <= commitIndex.get()) return false;

        // check if majority of the servers are at-least at this index
        int cnt = 0;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (matchIndex.get(i) >= index) {
                cnt++;
            }
        }
        // here I have used >= 2 because we don't actually update the match index of the leader
        return (cnt >= 2 && log.get(index).term == currentTerm.get());
    }

    // it is not in log
    private void sendAckForEntries(List<LogEntry> entriesToBeAck, CountDownLatch latch) {

        List<AckMessage> ackMessages = new ArrayList<>();

        // maybe we can acquire a read lock (but to optimise it we can keep this lock specific to the sendAck logic to avoid sending multiple ack
        // this is just about minimising repeated acks, not compulsory to add it, now for the calculation of the metrics this is strictly required
        for (LogEntry entry : entriesToBeAck) {
            String id = entry.t.getId();

            // need to add lock because lot of shared variables are being accessed here
            synchronized (metricCalculations) {
                if (ackSent.containsKey(id) && !ackSent.get(id)) {
                    // marking it as sent, if it fails the client can retry from its end
                    ackSent.put(id, true);
                    totalAckedTransactions.incrementAndGet();

                    // send ack for this entry
                    // System.out.println("sending ack");
//                    System.out.println("Replicated to ----" + entry.serversThatReplicatedThisEntry);
                    Long timeStampOfTransaction = System.currentTimeMillis();
                    // here I have implemented the logic of rolling throughput
                    // remove the old transactions from the queue, we maintain a window of 1 seconds
                    while (!ackTransactionsTimeStamps.isEmpty() &&
                            (timeStampOfTransaction - ackTransactionsTimeStamps.peek()) >= 1000L) {
                        ackTransactionsTimeStamps.poll();
                    }
                    // add the current transactions timestamp in the queue
                    ackTransactionsTimeStamps.add(timeStampOfTransaction);
                    // size of this queue should be the TPS / Rolling throughput
                    // System.out.println("Rolling throughput is--" + ackTransactionsTimeStamps.size());

                    // this latency is in ms
                    long latency = (timeStampOfTransaction - timeAtWhichTransactionWasReceived.get(id));
                    int currentTotalTransactions = totalTransactions.incrementAndGet();
                    long currentTotalLatency = totalLatency.addAndGet(latency);
                    // System.out.println("Current throughput of the system--" + (double) ((currentTotalTransactions * 1000)) / currentTotalLatency);
                    ackTransactionCount.incrementAndGet();
                    ackMessages.add(AckMessage.newBuilder().setT(entry.t).setTimStamp(HybridClock.TimeStamp.convertToProto(entry.timeStamp)).setCurrentLeader(serverId).build());

                }
            }
        }

        // this is the case when ack was sent earlier because of lesser writeConcern but now it is being sent again on comitting
        if (ackMessages.isEmpty()) {
            latch.countDown();
            return;
        }

        // refresh the context
        RaftGrpc.RaftStub refreshContextClientStub = clientStub.withDeadlineAfter(2, TimeUnit.SECONDS);
        refreshContextClientStub.sendAckToClient(Ack.newBuilder().addAllAckMessage(ackMessages).build(), new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
                // System.out.println("Error in sending ack--" + throwable);

                // implement retry logic
            }

            @Override
            public void onCompleted() {
                // this where we receive the message done
                System.out.println("Ack Sent successfully");
                latch.countDown();
            }
        });

    }

    private double getCurrentRollingThroughput() {
        synchronized (metricCalculations) {
            long currentTime = System.currentTimeMillis();
            while (!ackTransactionsTimeStamps.isEmpty() && (currentTime - ackTransactionsTimeStamps.peek()) > 1000) {
                ackTransactionsTimeStamps.poll();
            }
            return ackTransactionsTimeStamps.size();
        }
    }
    

    // we can optimize the write lock herek
    private boolean handleAppendEntriesResult(AppendEntriesResult appendEntriesResult, int matchIndexOfFollower, int prevNextIndex) {
        boolean result = appendEntriesResult.getIsSuccessFull();
        int termOfFollower = appendEntriesResult.getCurrentTerm(), idOfFollower = appendEntriesResult.getFollowerId();

        TimeStampProto followersTimeStamp = appendEntriesResult.getTimeStamp();

        List<LogEntry> committedEntriesAck = new ArrayList<>();
        List<LogEntry> eventualEntriesAck = new ArrayList<>();

        // Lock for reading and writing shared state
        lock.writeLock().lock();  // Lock to ensure exclusive write access for updating `nextIndex`, `matchIndex`, etc.
        try {

            // updating the clock of leader, if its behind it can catchup
            hybridClock.update(HybridClock.TimeStamp.convertToTimeStamp(followersTimeStamp));

            if (termOfFollower > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, termOfFollower));
                // Become follower
                status = ServerCurrentStatus.FOLLOWER;
                startTheElectionTimer();
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
                    int candidateCommitIndex = getCommitIndexIfPossible();

                    if (candidateCommitIndex > commitIndex.get()) {
                        commitIndex.updateAndGet(index -> Math.max(index, candidateCommitIndex)); // Update commitIndex

                        // update the majority committed map
                        for (int i = (prevCommitIndex + 1); i <= commitIndex.get(); i++) {
                            updateBalances(log.get(i).t, clientBalancesMajorityCommitted);
                        }
                        // System.out.println("The commit index of leader updated to -- " + commitIndex.get());
                        // System.out.println("Log size of leader is---" + log.size());

                        // Send acknowledgements from [(prevCommitIndex + 1), commitIndex] if needed
                        committedEntriesAck = log.getEntries(prevCommitIndex + 1, commitIndex.get());
                    }
                    // the leader see what all entries have been replicated by the replica, and decrements the appendEntries for those
                    eventualEntriesAck = checkIfWriteConcernsAreSatisfied(prevMatchIndex, matchIndex.get(idOfFollower), idOfFollower);
                }
            }
        } finally {
            lock.writeLock().unlock(); // Unlock after modifying shared state

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
                System.out.println("This follower --" + idOfFollower + "is updating the writeConcern of" + log.get(i).t);
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
            if (votes.get() >= (NUM_OF_SERVERS / 2) && status != ServerCurrentStatus.FOLLOWER) {
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
                // but here also there are two things that, if the readConcern is majority then we need to wait till the commit point passes or equals to the expected log entry
                if (readConcern == ReadConcern.MAJORITY) {
                    int logIndexOfRequestedEntry = timeStampsInLog.get(timeStampRequestedByClient);
                    // the requested logIndex should be safely committed
                    if (commitIndex.get() >= logIndexOfRequestedEntry) {
                        // return error, or we can wait for some more time, this is dependent on our design choice
                        responseObserver.onNext(Balance.newBuilder().setFailure(true).build());
                        responseObserver.onCompleted();
                    }
                } else {
                    // here the readConcern will be LOCAL
                    responseObserver.onNext(getBalanceBasedOnReadConcern(readConcern, accName));
                    responseObserver.onCompleted();
                }
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
                        responseObserver.onNext(Balance.newBuilder().setBalance(clientBalancesMajorityCommitted.getOrDefault(accName, 0.0)).setFailure(false).setAccName(accName).build());
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
        // System.out.println("The commit index of this node is -- " + commitIndex.get());
        // System.out.println("The size of log is ---" + log.size());
        // System.out.println("The majority committed map -- " + clientBalancesMajorityCommitted);
        // System.out.println("The latest map -- " + clientBalancesLatest);

//        System.out.println(totalLatency.get());
//        System.out.println("The latency of the system is in ms----" + (totalLatency.get() / (ackTransactionCount.get())));
//        System.out.println("The current clock time of this node is----" + hybridClock.now());
//        log.printLog();
    }

    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }
}
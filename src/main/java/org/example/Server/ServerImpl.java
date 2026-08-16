package org.example.Server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.example.Timer.CustomTimer;
import org.example.Utility.LogEntry;
import org.example.Utility.RaftLog;
import org.example.Utility.ServerStatus.ServerCurrentStatus;
import org.example.raft.AppendEntriesArgument;
import org.example.raft.AppendEntriesResult;
import org.example.raft.LogEntryProto;
import org.example.raft.RaftGrpc;
import org.example.raft.RaftGrpc.RaftStub;
import org.example.raft.ReadIndexRequest;
import org.example.raft.ReadIndexResult;
import org.example.raft.RequestVoteArguments;
import org.example.raft.RequestVoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * One Raft node: election, log replication, commit tracking, the KV state
 * machine (local and committed views), ReadIndex leadership confirmation, and
 * the wait registries the per-request path (KvClientService) blocks on. All
 * request-level decisions live in KvClientService; this class only offers the
 * mechanics: appendWrite, awaitLocalLogIndex, awaitCommitIndex,
 * awaitReplication, confirmLeadership.
 */
public class ServerImpl extends RaftGrpc.RaftImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ServerImpl.class);

    final int NUM_OF_SERVERS;
    final int serverId;

    public final AtomicInteger currentTerm;
    private int votedFor;
    final RaftLog log;
    final AtomicInteger commitIndex;
    private final AtomicIntegerArray nextIndex;
    private final AtomicIntegerArray matchIndex;

    private final CustomTimer electionTimer;
    public RaftStub[] stubs;
    private final AtomicInteger votes;
    public volatile ServerCurrentStatus status;
    private final AtomicBoolean isElectionOver;
    private final AtomicBoolean doesLeaderHasHighestTerm;
    volatile int currentLeader = -1;

    final ReadWriteLock lock;

    // Replicated KV state machine: local view applied at log append (may roll
    // back), committed view applied at commit-index advance (never rolls back).
    final KvStore kv;

    // Wait registries the per-request path blocks on. Index registries are
    // monotonic: after a truncation the old high-water mark stands, which is
    // correct because a wait anchored on a rolled-back entry has nothing left
    // to wait for.
    private final IndexWaitRegistry localLogWaiters = new IndexWaitRegistry();
    private final IndexWaitRegistry commitWaiters = new IndexWaitRegistry();
    private final ReplicationWaitRegistry replicationWaiters = new ReplicationWaitRegistry();

    private ScheduledExecutorService sendAppendEntriesScheduler;
    private final ScheduledExecutorService waitScheduler;
    final ExecutorService executorService;

    // When true, this node stops all Raft inter-server network communication.
    private volatile boolean dropAllServerNetworkTraffic;

    // Every gRPC channel this node opens, so shutdown() can close them all.
    private final List<ManagedChannel> ownedChannels = Collections.synchronizedList(new ArrayList<>());

    private static final int HEARTBEAT_INTERVAL_MS = 20;
    private static final int MAX_ENTRIES_PER_RPC = 10000;
    private static final int ELECTION_TIMEOUT_BASE_MS = 2000;
    private static final int ELECTION_TIMEOUT_JITTER_MS = 700;

    // Cluster wiring (from config; see ExperimentConfig.cluster)
    private static volatile List<String> SERVER_HOSTS = List.of("localhost", "localhost", "localhost");
    private static volatile int SERVER_BASE_PORT = 8000;

    /** Apply cluster configuration. Must run before any ServerImpl is constructed. */
    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        applyClusterSettings(config.cluster.serverHosts, config.cluster.serverBasePort);
    }

    /** Point the peer stubs at a cluster without a full config (tests use this directly). */
    public static void applyClusterSettings(List<String> serverHosts, int basePort) {
        SERVER_HOSTS = List.copyOf(serverHosts);
        SERVER_BASE_PORT = basePort;
    }

    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.serverId = serverId;
        this.NUM_OF_SERVERS = NUM_OF_SERVERS;
        this.dropAllServerNetworkTraffic = false;
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = -1;
        this.commitIndex = new AtomicInteger(-1);
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);
        this.doesLeaderHasHighestTerm = new AtomicBoolean(false);
        this.lock = new ReentrantReadWriteLock();
        this.kv = new KvStore();
        this.log = new RaftLog();
        this.stubs = new RaftStub[NUM_OF_SERVERS];
        this.nextIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.matchIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            nextIndex.set(i, 0);
            matchIndex.set(i, -1);
        }
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.sendAppendEntriesScheduler = Executors.newScheduledThreadPool(1);
        this.waitScheduler = Executors.newScheduledThreadPool(1);
        this.electionTimer = new CustomTimer(this::startElection,
                ELECTION_TIMEOUT_BASE_MS + new Random().nextInt(ELECTION_TIMEOUT_JITTER_MS), TimeUnit.MILLISECONDS);
        this.electionTimer.start();
    }

    public void setUpStubs() {
        // When this node's configured host is a literal IPv4 address (the geo
        // latency setup gives every server its own loopback IP), outgoing
        // connections bind to it as their source address, so the OS-level
        // delay rules can identify the (source, destination) server pair.
        // Hostnames like "localhost" are never bound: they may resolve to a
        // different family than the destination picks.
        String ownHost = SERVER_HOSTS.get(serverId);
        final java.net.InetSocketAddress ownSource = ownHost.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")
                ? new java.net.InetSocketAddress(ownHost, 0)
                : null;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i != serverId) {
                io.grpc.netty.NettyChannelBuilder builder = io.grpc.netty.NettyChannelBuilder
                        .forAddress(SERVER_HOSTS.get(i), SERVER_BASE_PORT + (i + 1)).enableRetry()
                        .usePlaintext();
                if (ownSource != null) {
                    builder.localSocketPicker(new io.grpc.netty.NettyChannelBuilder.LocalSocketPicker() {
                        @Override
                        public java.net.SocketAddress createSocketAddress(
                                java.net.SocketAddress remoteAddress, io.grpc.Attributes attrs) {
                            return ownSource;
                        }
                    });
                }
                ManagedChannel channel = builder.build();
                ownedChannels.add(channel);
                stubs[i] = RaftGrpc.newStub(channel);
            }
        }
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

    // ===== Public mechanics for the per-request path =====

    /**
     * Leader-only: append one KV write to the log, apply it to the local view,
     * and return its log index. Replication happens on the heartbeat schedule;
     * callers wait for their write concern via awaitCommitIndex (majority) or
     * awaitReplication (intermediate counts).
     */
    public int appendWrite(String key, String value, String opId) throws NotLeaderException {
        lock.writeLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER) {
                throw new NotLeaderException(currentLeader, "server " + serverId + " is not the leader");
            }
            int index = log.size();
            LogEntry entry = new LogEntry(index, currentTerm.get(), key, value, opId);
            // State before index: the lock-free fast path in KvClientService
            // serves as soon as lastLogIndex covers its anchor, so the KV view
            // must already contain everything the published index promises.
            kv.applyLocal(entry);
            log.append(entry);
            localLogWaiters.signal(index);
            return index;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Completes once this node's local log covers the given index. */
    public CompletableFuture<Void> awaitLocalLogIndex(int index) {
        return localLogWaiters.await(index);
    }

    /** Completes once this node's commit index covers the given index. */
    public CompletableFuture<Void> awaitCommitIndex(int index) {
        return commitWaiters.await(index);
    }

    /** Leader-only: completes once the entry is replicated on at least the given number of nodes. */
    public CompletableFuture<Void> awaitReplication(int entryIndex, int requiredReplicas) {
        return replicationWaiters.await(entryIndex, requiredReplicas, this::replicaCount);
    }

    /** Number of nodes (leader included) known to hold the entry at this index. */
    public int replicaCount(int entryIndex) {
        int count = 1;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i != serverId && matchIndex.get(i) >= entryIndex) {
                count++;
            }
        }
        return count;
    }

    public int lastLogIndex() {
        return log.size() - 1;
    }

    public int currentCommitIndex() {
        return commitIndex.get();
    }

    public boolean isLeader() {
        return status == ServerCurrentStatus.LEADER;
    }

    /** Best-known leader id, -1 if unknown. */
    public int leaderIdHint() {
        return currentLeader;
    }

    public int majority() {
        return (NUM_OF_SERVERS / 2) + 1;
    }

    public int nodeId() {
        return serverId;
    }

    // ===== Raft RPC handlers =====

    @Override
    public void appendEntries(AppendEntriesArgument args, StreamObserver<AppendEntriesResult> responseObserver) {
        if (shouldDropServerNetworkTraffic()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: appendEntries dropped")
                    .asRuntimeException());
            return;
        }
        int leadersTerm = args.getLeadersTerm(),
                prevLogIndex = args.getPrevLogIndex(),
                prevLogTerm = args.getPrevLogTerm(),
                leadersCommitIndex = args.getLeadersCommit(),
                leaderId = args.getLeadersId();

        lock.writeLock().lock();
        try {
            if (leadersTerm > currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                becomeFollower();
            }
            if (leadersTerm == currentTerm.get()) {
                currentLeader = leaderId;
                // If we're CANDIDATE or LEADER and receive AppendEntries from a
                // valid leader, step down.
                if (status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            }
            if (leadersTerm < currentTerm.get()) {
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get())
                        .setIsSuccessFull(false).setFollowerId(serverId).build());
                responseObserver.onCompleted();
                return;
            }
            startTheElectionTimer();

            if (!log.checkIfPrevLogIndexHasPrevLogTerm(prevLogIndex, prevLogTerm)) {
                responseObserver.onNext(AppendEntriesResult.newBuilder().setCurrentTerm(currentTerm.get())
                        .setIsSuccessFull(false).setFollowerId(serverId).build());
                responseObserver.onCompleted();
                return;
            }

            // Raft 5.3: truncate only at the first entry whose (index, term)
            // conflicts with ours, never on a matching prefix. The leader
            // sends rounds every 30 ms without waiting for responses, so
            // duplicate and reordered rounds are routine; an unconditional
            // truncation at prevLogIndex+1 would wipe and re-append the log on
            // every stale round, and a stale empty probe would wipe it
            // outright - after which the monotonic wait registries would let
            // causal reads serve the emptied view.
            List<LogEntryProto> entries = args.getEntriesList();
            int skip = 0;
            while (skip < entries.size()) {
                LogEntryProto proto = entries.get(skip);
                if (proto.getLogIndex() < log.size() && log.get(proto.getLogIndex()).term == proto.getTerm()) {
                    skip++;
                } else {
                    break;
                }
            }
            if (skip < entries.size()) {
                // Truncate any genuinely conflicting suffix (rebuilds the
                // local KV view), then append the new entries. State before
                // index (see appendWrite): apply each entry to the local KV
                // view before the log publishes its index.
                truncateSuffixAndRebuildLocal(entries.get(skip).getLogIndex());
                for (int i = skip; i < entries.size(); i++) {
                    LogEntryProto proto = entries.get(i);
                    LogEntry entry = new LogEntry(proto.getLogIndex(), proto.getTerm(), proto.getKey(),
                            proto.getValue(), proto.getOpId());
                    kv.applyLocal(entry);
                    log.append(entry);
                }
                localLogWaiters.signal(log.size() - 1);
            }

            if (leadersCommitIndex > commitIndex.get()) {
                int newCommitIndex = Math.min(leadersCommitIndex, log.size() - 1);
                // Committed view before commit index, same ordering rule.
                for (int i = commitIndex.get() + 1; i <= newCommitIndex; i++) {
                    kv.applyCommitted(log.get(i));
                }
                commitIndex.set(newCommitIndex);
                commitWaiters.signal(newCommitIndex);
            }

            responseObserver.onNext(AppendEntriesResult.newBuilder()
                    .setIsSuccessFull(true).setFollowerId(serverId).setCurrentTerm(currentTerm.get()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("Exception in appendEntries on server {}", serverId, e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // inside write lock; logIndex is the first log position being truncated
    private void truncateSuffixAndRebuildLocal(int logIndex) {
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
        // Shrink the published log index first, then rebuild the local KV view
        // (committed state plus the surviving uncommitted suffix, in log
        // order). Lock-free readers between the two steps see a low index and
        // wait, which is the conservative direction.
        List<LogEntry> survivingSuffix = log.entriesInRange(commitIndex.get() + 1, logIndex);
        log.truncateAfter(logIndex);
        kv.rebuildLocal(survivingSuffix);
    }

    // in lock
    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogIndex();
        // deny vote condition
        return !((lastLogTermOfCurrentNode > lastLogTermOfCandidate)
                || ((lastLogTermOfCurrentNode == lastLogTermOfCandidate)
                        && (lastLogIndexOfCurrentNode > lastLogIndexOfCandidate)));
    }

    @Override
    public void requestVote(RequestVoteArguments args, StreamObserver<RequestVoteResult> responseObserver) {
        if (shouldDropServerNetworkTraffic()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: requestVote dropped")
                    .asRuntimeException());
            return;
        }
        int currentTermOfTheCandidate = args.getCandidatesTerm(),
                lastLogIndexOfCandidate = args.getLastLogIndex(),
                lastLogTermOfCandidate = args.getLastLogTerm(),
                candidateId = args.getCandidateId();

        boolean isVoteGranted = true;

        lock.writeLock().lock();
        try {
            if (currentTermOfTheCandidate > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, currentTermOfTheCandidate));
                votedFor = -1;
                becomeFollower();
            }
            if (votedFor != -1 || (this.currentTerm.get() > currentTermOfTheCandidate)
                    || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
                isVoteGranted = false;
            }
            if (isVoteGranted) {
                votedFor = candidateId;
                startTheElectionTimer();
            }
            responseObserver.onNext(RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted)
                    .setCurrentTerm(currentTerm.get()).build());
            responseObserver.onCompleted();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ===== Election flow =====

    public int getLastLogIndex() {
        LogEntry last = log.getLastLogEntry();
        return last == null ? -1 : last.index;
    }

    public int getLastLogTerm() {
        LogEntry last = log.getLastLogEntry();
        return last == null ? -1 : last.term;
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

    // all atomic variables, no lock needed
    public boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if (requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            currentTerm.updateAndGet(term -> Math.max(term, requestVoteResult.getCurrentTerm()));
            return false;
        }
        if (requestVoteResult.getIsVoteGranted()) {
            votes.incrementAndGet();
        }
        return true;
    }

    private void requestForVotes(RequestVoteArguments requestVoteArguments) {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(NUM_OF_SERVERS - 1);

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i == serverId) {
                continue;
            }
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
                        if (!isSuccessful) {
                            if (isElectionOver.compareAndSet(false, true)) {
                                shouldBecomeFollower = true;
                            }
                        } else if (votes.get() > (NUM_OF_SERVERS / 2)) {
                            if (isElectionOver.compareAndSet(false, true)) {
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
                        while (latch.getCount() > 0) {
                            latch.countDown();
                        }
                    } else if (shouldBecomeFollower) {
                        lock.writeLock().lock();
                        try {
                            becomeFollower();
                        } finally {
                            lock.writeLock().unlock();
                        }
                        while (latch.getCount() > 0) {
                            latch.countDown();
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                }
            });
        }
        try {
            boolean success = latch.await(800, TimeUnit.MILLISECONDS);
            lock.writeLock().lock();
            try {
                if (!success && isElectionOver.compareAndSet(false, true)
                        && status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            } finally {
                lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void startElection() {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }
        lock.writeLock().lock();
        RequestVoteArguments requestVoteArguments;
        try {
            this.status = ServerCurrentStatus.CANDIDATE;
            currentTerm.incrementAndGet();
            startTheElectionTimer();
            votes.set(1);
            votedFor = serverId;
            isElectionOver.set(false);
            requestVoteArguments = getRequestVoteArgumentsObject();
        } finally {
            lock.writeLock().unlock();
        }
        requestForVotes(requestVoteArguments);
    }

    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            // A candidate that became follower via requestVote in between must
            // not become leader.
            if (status != ServerCurrentStatus.CANDIDATE) {
                return;
            }
            doesLeaderHasHighestTerm.set(true);
            electionTimer.stop();
            reinitialiseIndexes();
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

    // should be inside write lock
    private void becomeFollower() {
        if (status == ServerCurrentStatus.LEADER || status == ServerCurrentStatus.CANDIDATE) {
            startTheElectionTimer();
        }
        status = ServerCurrentStatus.FOLLOWER;
        votedFor = -1;
        // A node that is no longer leader can never confirm leadership or
        // satisfy leader-side replication waits.
        failPendingLeadershipConfirmations();
        replicationWaiters.failAll(new NotLeaderException(currentLeader, "stepped down from leadership"));
        if (sendAppendEntriesScheduler != null && !sendAppendEntriesScheduler.isShutdown()) {
            sendAppendEntriesScheduler.shutdownNow();
            sendAppendEntriesScheduler = null;
        }
    }

    // already in writeLock
    private void reinitialiseIndexes() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
        }
    }

    // ===== Replication =====

    private void sendAppendEntries() {
        sendAppendEntriesScheduler.scheduleAtFixedRate(() -> {
            if (shouldDropServerNetworkTraffic() || status != ServerCurrentStatus.LEADER) {
                return;
            }
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (i == serverId) {
                    continue;
                }
                final int followerId = i;

                executorService.submit(() -> {
                    int indexToSendFrom;
                    int endIndex;
                    int matchIndexForFollower;
                    int prevLogIndex;
                    int prevLogTerm;
                    // Taken before the RPC is sent: any response to this round
                    // proves leadership only for confirmations registered
                    // before this instant (ReadIndex).
                    final long sendStartNanos = System.nanoTime();

                    lock.readLock().lock();
                    try {
                        if (status != ServerCurrentStatus.LEADER) {
                            return;
                        }
                        indexToSendFrom = nextIndex.get(followerId);
                        prevLogIndex = indexToSendFrom - 1;
                        prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term : -1;
                        endIndex = Math.min(log.size(), indexToSendFrom + MAX_ENTRIES_PER_RPC);
                        matchIndexForFollower = endIndex - 1;
                    } finally {
                        lock.readLock().unlock();
                    }

                    List<LogEntryProto> protoEntries = new ArrayList<>();
                    for (LogEntry entry : log.entriesInRange(indexToSendFrom, endIndex)) {
                        protoEntries.add(LogEntryProto.newBuilder()
                                .setLogIndex(entry.index).setTerm(entry.term)
                                .setKey(entry.key).setValue(entry.value).setOpId(entry.opId)
                                .build());
                    }

                    AppendEntriesArgument request = AppendEntriesArgument.newBuilder()
                            .setLeadersTerm(currentTerm.get())
                            .setLeadersId(serverId)
                            .setLeadersCommit(commitIndex.get())
                            .setPrevLogIndex(prevLogIndex)
                            .setPrevLogTerm(prevLogTerm)
                            .addAllEntries(protoEntries)
                            .build();

                    if (shouldDropServerNetworkTraffic()) {
                        return;
                    }

                    stubs[followerId].appendEntries(request, new StreamObserver<AppendEntriesResult>() {
                        @Override
                        public void onNext(AppendEntriesResult result) {
                            doesLeaderHasHighestTerm.compareAndSet(true,
                                    handleAppendEntriesResult(result, matchIndexForFollower, indexToSendFrom));
                            // ReadIndex: a response whose term does not exceed
                            // ours means this follower accepted our authority
                            // for this round.
                            if (result.getCurrentTerm() <= request.getLeadersTerm()) {
                                recordLeadershipAck(followerId, sendStartNanos, request.getLeadersTerm());
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            LOG.debug("AppendEntries RPC failed for follower {}", followerId, throwable);
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
                });
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private boolean handleAppendEntriesResult(AppendEntriesResult result, int matchIndexOfFollower,
            int prevNextIndex) {
        boolean success = result.getIsSuccessFull();
        int termOfFollower = result.getCurrentTerm(), idOfFollower = result.getFollowerId();

        lock.writeLock().lock();
        try {
            // idempotence: stale responses after stepping down are ignored
            if (status != ServerCurrentStatus.LEADER) {
                return false;
            }
            if (termOfFollower > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, termOfFollower));
                becomeFollower();
                return false;
            }
            if (!success) {
                // Back off; guard against double decrements from overlapping rounds.
                if (nextIndex.get(idOfFollower) >= prevNextIndex) {
                    nextIndex.decrementAndGet(idOfFollower);
                }
            } else if (matchIndex.get(idOfFollower) < matchIndexOfFollower) {
                matchIndex.set(idOfFollower, matchIndexOfFollower);
                nextIndex.set(idOfFollower, matchIndexOfFollower + 1);

                int prevCommitIndex = commitIndex.get();
                int candidateCommitIndex = getCommitIndexIfPossibleEarlyExitMethod();
                if (candidateCommitIndex > prevCommitIndex) {
                    // Committed view before commit index (see appendWrite).
                    for (int i = prevCommitIndex + 1; i <= candidateCommitIndex; i++) {
                        kv.applyCommitted(log.get(i));
                    }
                    commitIndex.set(candidateCommitIndex);
                    commitWaiters.signal(candidateCommitIndex);
                }
                replicationWaiters.evaluate(this::replicaCount);
            }
        } catch (Exception e) {
            LOG.error("Exception while handling AppendEntriesResult on server {}", serverId, e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
        return true;
    }

    // should be inside a lock; walks down from the max matchIndex, so it is
    // O(n) in practice because followers trail the leader by a few entries
    private int getCommitIndexIfPossibleEarlyExitMethod() {
        int maxMatchIndex = -1;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            maxMatchIndex = Math.max(maxMatchIndex, matchIndex.get(i));
        }
        for (int idx = maxMatchIndex; idx > commitIndex.get(); idx--) {
            int count = 0;
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (matchIndex.get(i) >= idx) {
                    count++;
                }
            }
            // count is followers only (the leader's own matchIndex stays -1),
            // so followers >= N/2 plus the leader itself is a majority. Only
            // current-term entries may be committed by counting.
            if (count >= (NUM_OF_SERVERS / 2) && log.get(idx).term == currentTerm.get()) {
                return idx;
            }
        }
        return -1;
    }

    // this resets and starts the timer again
    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }

    // ===== ReadIndex leadership confirmation =====

    /** Thrown when a leader-only operation is invoked on a non-leader. */
    public static final class NotLeaderException extends Exception {
        /** Best-known leader id at the time of rejection, -1 if unknown. */
        public final int leaderId;

        public NotLeaderException(int leaderId, String message) {
            super(message);
            this.leaderId = leaderId;
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
                        new NotLeaderException(currentLeader, "server " + serverId + " is not the leader"));
            }
            int readIndex = commitIndex.get();
            if (NUM_OF_SERVERS == 1) {
                return CompletableFuture.completedFuture(readIndex);
            }
            LeadershipConfirmation confirmation = new LeadershipConfirmation(
                    currentTerm.get(), readIndex, System.nanoTime());
            pendingLeadershipConfirmations.add(confirmation);
            waitScheduler.schedule(() -> {
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
        for (LeadershipConfirmation confirmation : pendingLeadershipConfirmations) {
            // Only rounds started after registration prove leadership at (or
            // after) the moment the read index was snapshotted.
            if (confirmation.term != termAtSend || sendStartNanos < confirmation.registeredAtNanos) {
                continue;
            }
            confirmation.ackedFollowers.add(followerId);
            // +1 counts this node itself.
            if (confirmation.ackedFollowers.size() + 1 >= majority()
                    && pendingLeadershipConfirmations.remove(confirmation)) {
                confirmation.future.complete(confirmation.readIndex);
            }
        }
    }

    private void failPendingLeadershipConfirmations() {
        LeadershipConfirmation confirmation;
        while ((confirmation = pendingLeadershipConfirmations.poll()) != null) {
            confirmation.future.completeExceptionally(
                    new NotLeaderException(currentLeader, "stepped down from leadership"));
        }
    }

    /**
     * RPC handler: a follower asks this node (believing it the leader) for a
     * confirmed read index. Runs the same ReadIndex confirmation as a local
     * linearizable read; a non-leader answers success=false with its best
     * leader hint instead of an error, so the caller can redirect.
     */
    @Override
    public void confirmReadIndex(ReadIndexRequest request, StreamObserver<ReadIndexResult> responseObserver) {
        if (shouldDropServerNetworkTraffic()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: confirmReadIndex dropped")
                    .asRuntimeException());
            return;
        }
        confirmLeadership().whenComplete((readIndex, error) -> {
            ReadIndexResult result = (error == null)
                    ? ReadIndexResult.newBuilder().setSuccess(true).setReadIndex(readIndex)
                            .setLeaderId(serverId).build()
                    : ReadIndexResult.newBuilder().setSuccess(false).setReadIndex(-1)
                            .setLeaderId(currentLeader == serverId ? -1 : currentLeader).build();
            synchronized (responseObserver) {
                try {
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    LOG.debug("Dropping confirmReadIndex reply on server {}: {}", serverId, e.toString());
                }
            }
        });
    }

    // ===== Batched follower read index =====

    // At most one read-index round is in flight per follower; requests that
    // arrive before a round's send share its returned index (their
    // linearization point, the confirmation, happens after their arrival).
    // Requests arriving while a round is in flight join the next round.
    // Callers must never orTimeout the shared future directly; derive first.
    private CompletableFuture<Integer> inFlightReadIndexRound;   // guarded by readIndexRoundLock
    private CompletableFuture<Integer> queuedReadIndexRound;     // guarded by readIndexRoundLock
    private final Object readIndexRoundLock = new Object();
    final AtomicInteger readIndexRoundsSent = new AtomicInteger(); // test observability

    /**
     * Follower-side half of follower linearizable reads: obtain a confirmed
     * read index from the current leader, batching concurrent callers into
     * shared rounds. The caller must then wait for this node's own commit
     * index to reach the returned index before serving from the committed
     * view. Fails with NotLeaderException when no leader is known or the
     * presumed leader has stepped down.
     */
    public CompletableFuture<Integer> readIndexFromLeader() {
        if (status == ServerCurrentStatus.LEADER) {
            return confirmLeadership();
        }
        CompletableFuture<Integer> round;
        boolean sendNow = false;
        synchronized (readIndexRoundLock) {
            if (inFlightReadIndexRound != null) {
                if (queuedReadIndexRound == null) {
                    queuedReadIndexRound = new CompletableFuture<>();
                }
                round = queuedReadIndexRound;
            } else {
                inFlightReadIndexRound = new CompletableFuture<>();
                round = inFlightReadIndexRound;
                sendNow = true;
            }
        }
        if (sendNow) {
            sendReadIndexRound(round);
        }
        return round;
    }

    private void sendReadIndexRound(CompletableFuture<Integer> round) {
        int leaderId = currentLeader;
        if (status == ServerCurrentStatus.LEADER) {
            confirmLeadership().whenComplete((readIndex, error) -> finishReadIndexRound(round, readIndex, error));
            return;
        }
        if (leaderId < 0 || leaderId == serverId || dropAllServerNetworkTraffic) {
            finishReadIndexRound(round, null,
                    new NotLeaderException(-1, "server " + serverId + " knows no leader to ask for a read index"));
            return;
        }
        readIndexRoundsSent.incrementAndGet();
        stubs[leaderId].confirmReadIndex(ReadIndexRequest.newBuilder().setRequesterId(serverId).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReadIndexResult result) {
                        if (result.getSuccess()) {
                            finishReadIndexRound(round, result.getReadIndex(), null);
                        } else {
                            finishReadIndexRound(round, null, new NotLeaderException(result.getLeaderId(),
                                    "server " + leaderId + " is no longer the leader"));
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        finishReadIndexRound(round, null, t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    private void finishReadIndexRound(CompletableFuture<Integer> round, Integer readIndex, Throwable error) {
        // Promote the queued round before completing this one, so late
        // callers can never join a round that has already resolved.
        CompletableFuture<Integer> next;
        synchronized (readIndexRoundLock) {
            inFlightReadIndexRound = queuedReadIndexRound;
            queuedReadIndexRound = null;
            next = inFlightReadIndexRound;
        }
        if (error != null) {
            round.completeExceptionally(error);
        } else {
            round.complete(readIndex);
        }
        if (next != null) {
            sendReadIndexRound(next);
        }
    }

    /**
     * Stop all background activity (election timer, replication scheduler,
     * worker pool) and close every gRPC channel this node opened. Used by
     * tests and orderly teardown; the node is unusable afterwards.
     */
    public void shutdown() {
        dropAllServerNetworkTraffic = true;
        electionTimer.stop();
        electionTimer.shutdown();
        failPendingLeadershipConfirmations();
        replicationWaiters.failAll(new NotLeaderException(-1, "node shut down"));
        if (sendAppendEntriesScheduler != null) {
            sendAppendEntriesScheduler.shutdownNow();
        }
        waitScheduler.shutdownNow();
        executorService.shutdownNow();
        synchronized (ownedChannels) {
            for (ManagedChannel channel : ownedChannels) {
                channel.shutdownNow();
            }
        }
    }
}

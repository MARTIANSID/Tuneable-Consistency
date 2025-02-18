package org.example.Server;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.ds.paxos.Client;
import org.ds.paxos.*;
import org.example.Keys.KeyGeneration;
import org.example.Log.Log;
import org.example.Log.TransactionStatus;
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


public class ServerImpl extends PbftGrpc.PbftImplBase {
    String serverId;
    AtomicInteger viewNo;
    AtomicInteger sequenceNumber;

    PrivateKey privateKey;

    ConcurrentHashMap<Integer, PublicKey> publicKeys;

    List<ManagedChannel> peers;

    CustomTimer requestTimer;
    CustomTimer viewChangeTimer;

    ConcurrentHashMap<Integer, Log> logBasedOnSequenceNumber;
    ConcurrentHashMap<Integer, Integer> unExecutedRequests;

    ConcurrentHashMap<Integer, Double> clientBalances;

    ConcurrentHashMap<String, ConcurrentLinkedDeque<Prepare>> checkPrepares;
    ConcurrentHashMap<String, ConcurrentLinkedDeque<Commit>> checkCommits;

    ConcurrentHashMap<String, Integer> clientRequests;

    ConcurrentHashMap<String, Reply> lastSentReply;

    ConcurrentHashMap<String, Integer> inProgressClientRequests;

    ConcurrentHashMap<Integer, List<ViewChange>> viewChangeMessages;

    ConcurrentHashMap<Integer, Boolean> gotNwViewMessage;

    AtomicBoolean viewChangeStarted;

    ViewChange lastViewChangeMessageSent;

    ConcurrentHashMap<Integer, Boolean> newViewSentCheck;

    ConcurrentHashMap<Integer, List<ViewChange>> successfulViewChangeMessages;
    ConcurrentHashMap<Integer, NewView> successfulNewViewMessages;

    AtomicBoolean isAlive;

    AtomicBoolean isByzantine;

    ConcurrentHashMap<Integer, Duration> timeTakenForExecutingSequenceNumber;
    ConcurrentHashMap<Integer, Boolean> decisionMadeForSequenceNumber;

    int clusterId;

    Connection connection;

    ConcurrentHashMap<Integer, String> locks;

    ConcurrentLinkedDeque<TransactionStatus> dataStore;

    int serverKeyId;

    HashMap<String, Integer> serverIdToServerKeyId;

    PbftGrpc.PbftStub[] stubs;

    HashMap<String, Integer> serverPorts;

    ConcurrentHashMap<String, CurrentStatus> resultOfTheSequenceNumber;

    ConcurrentHashMap<String, CustomTimer> coordinatorTimer;


    public ServerImpl(String serverId, int clusterId, Connection connection, int serverKeyId) {
        this.viewNo = new AtomicInteger(0);
        this.serverId = serverId;
        this.sequenceNumber = new AtomicInteger(0);
        this.publicKeys = new ConcurrentHashMap<>(KeyGeneration.publicKeys);
        this.logBasedOnSequenceNumber = new ConcurrentHashMap<>();
        this.peers = new ArrayList<>();
        this.unExecutedRequests = new ConcurrentHashMap<>();
        this.clientBalances = new ConcurrentHashMap<>();
        this.requestTimer = new CustomTimer(this::startViewChange, (int) 1e9, TimeUnit.SECONDS);
        this.checkCommits = new ConcurrentHashMap<>();
        this.checkPrepares = new ConcurrentHashMap<>();
        this.clientRequests = new ConcurrentHashMap<>();
        this.lastSentReply = new ConcurrentHashMap<>();
        this.inProgressClientRequests = new ConcurrentHashMap<>();
        this.viewChangeStarted = new AtomicBoolean(false);
        this.lastViewChangeMessageSent = ViewChange.newBuilder().setViewNo(-1).build();
        this.viewChangeMessages = new ConcurrentHashMap<>();
        this.viewChangeTimer = new CustomTimer(this::startConsecutiveViewChange, 10, TimeUnit.SECONDS);
        this.gotNwViewMessage = new ConcurrentHashMap<>();
        this.newViewSentCheck = new ConcurrentHashMap<>();
        this.isAlive = new AtomicBoolean(true);
        this.isByzantine = new AtomicBoolean(false);
        this.successfulViewChangeMessages = new ConcurrentHashMap<>();
        this.successfulNewViewMessages = new ConcurrentHashMap<>();
        this.timeTakenForExecutingSequenceNumber = new ConcurrentHashMap<>();
        this.connection = connection;
        this.clusterId = clusterId;
        this.locks = new ConcurrentHashMap<>();
        this.dataStore = new ConcurrentLinkedDeque<>();
        this.decisionMadeForSequenceNumber = new ConcurrentHashMap<>();
        this.serverKeyId = serverKeyId;
        this.serverIdToServerKeyId = new HashMap<>();
        this.privateKey = KeyGeneration.privateKeys.get(this.serverKeyId);
        this.resultOfTheSequenceNumber = new ConcurrentHashMap<>();
        this.stubs = new PbftGrpc.PbftStub[13];
        this.serverPorts = new HashMap<>();
        this.coordinatorTimer = new ConcurrentHashMap<>();


        // assigning serverids to their key IDS
        serverIdToServerKeyId.put("S1", 1);
        serverIdToServerKeyId.put("S2", 2);
        serverIdToServerKeyId.put("S3", 3);
        serverIdToServerKeyId.put("S4", 4);
        serverIdToServerKeyId.put("S5", 5);
        serverIdToServerKeyId.put("S6", 6);
        serverIdToServerKeyId.put("S7", 7);
        serverIdToServerKeyId.put("S8", 8);
        serverIdToServerKeyId.put("S9", 9);
        serverIdToServerKeyId.put("S10", 10);
        serverIdToServerKeyId.put("S11", 11);
        serverIdToServerKeyId.put("S12", 12);

        serverPorts.put("S1", 1);
        serverPorts.put("S2", 2);
        serverPorts.put("S3", 3);
        serverPorts.put("S4", 4);
        serverPorts.put("S5", 5);
        serverPorts.put("S6", 6);
        serverPorts.put("S7", 7);
        serverPorts.put("S8", 8);
        serverPorts.put("S9", 9);
        serverPorts.put("S10", 10);
        serverPorts.put("S11", 11);
        serverPorts.put("S12", 12);

        for (int i = 0; i <= 12; i++) {
            if (i == 0) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
                stubs[i] = PbftGrpc.newStub(channel);
            } else {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i).usePlaintext().build();
                stubs[i] = PbftGrpc.newStub(channel);
            }
        }

        if (clusterId == 1) {
            for (int i = 1; i <= 4; i++) {
                if (("S" + i).equals(serverId)) continue;
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i)
                        .usePlaintext()
                        .build();
                peers.add(channel);
            }

            for (int i = 1; i <= 1000; i++) {
                this.clientBalances.put(i, 10.0);
            }
        }
        if (clusterId == 2) {
            for (int i = 5; i <= 8; i++) {
                if (("S" + i).equals(serverId)) continue;
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i)
                        .usePlaintext()
                        .build();
                peers.add(channel);
            }

            for (int i = 1001; i <= 2000; i++) {
                this.clientBalances.put(i, 10.0);
            }
        }
        if (clusterId == 3) {
            for (int i = 9; i <= 12; i++) {
                if (("S" + i).equals(serverId)) continue;
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i)
                        .usePlaintext()
                        .build();
                peers.add(channel);
            }
            for (int i = 2001; i <= 3000; i++) {
                this.clientBalances.put(i, 10.0);
            }

        }
    }


    @Override
    public void clientRequest(Client c, StreamObserver<Empty> streamObserver) {

        System.out.println("Got Request");
        if (!isAlive.get()) return;
        if (viewChangeStarted.get()) return;

        // introducing a random dealy in case threads from different backups come at the same time
        try {
            Thread.sleep(20 + new Random().nextInt(5));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (lastSentReply.containsKey(getClientKey(c))) {
            Reply reply = lastSentReply.get(getClientKey(c));
            sendReplyAgain(reply);
            return;
        }

        if (!getCurrentLeader().equals(serverId)) {
            // send the request to the primary node
            System.out.println("Forwarding request to leader" + getCurrentLeader());
            sendClientMessageToLeader(c);
            if (!requestTimer.isRunning() && !isByzantine.get()) {
                requestTimer.start();
            }
            return;
        }


        // if the request is already in progress ignore it
        if (inProgressClientRequests.containsKey(getClientKey(c)) && (logBasedOnSequenceNumber.get(inProgressClientRequests.get(getClientKey(c))).inProgress || (logBasedOnSequenceNumber.get(inProgressClientRequests.get(getClientKey(c))).state == Log.State.EXECUTE)))
            return;
        if (clientRequests.containsKey(getClientKey(c)) && logBasedOnSequenceNumber.containsKey(getClientKey(c)) && logBasedOnSequenceNumber.get(getClientKey(c)).inProgress)
            return;

        try {

            Transaction t = c.getT();

            int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

            double amount = t.getAmount();

            String clientKey = getClientKey(c);

            // balance condition

            // locks condition
            if (senderCluster == clusterId) {
                if (locks.containsKey(sender) && !locks.get(sender).equals(clientKey)) {
                    Thread.sleep(5000);
                }
            }
            if (receiverCluster == clusterId) {
                if (locks.containsKey(receiver) && !locks.get(receiver).equals(clientKey)) {
                    Thread.sleep(5000);
                }
            }

            if (clientBalances.get(sender) < amount) {
                return;
            }
            // acquire locks
            locks.put(sender, clientKey);
            if (senderCluster == receiverCluster) {
                locks.put(receiver, clientKey);
            }

            if (receiver == 2994) {
                System.out.println("Goot iiittt ----------------------");
            }

            int seqNoOfPrePrepare;

            if (clientRequests.containsKey(getClientKey(c))) {
                seqNoOfPrePrepare = clientRequests.get(getClientKey(c));
            } else {
                incrementSequenceNumber();
                seqNoOfPrePrepare = this.sequenceNumber.get();
            }


            if (!logBasedOnSequenceNumber.containsKey(seqNoOfPrePrepare) || (logBasedOnSequenceNumber.containsKey(seqNoOfPrePrepare) && logBasedOnSequenceNumber.get(seqNoOfPrePrepare).state != Log.State.EXECUTE)) {
                unExecutedRequests.put(seqNoOfPrePrepare, 1);
            }

            clientRequests.put(getClientKey(c), seqNoOfPrePrepare);

            String digestOfClientMessage = createDigestOfClientMessage(c);
            String key = UUID.randomUUID().toString();

            int viewNo = this.viewNo.get();

            boolean gotPrepareQuorum = sendPrePrepareToAll(viewNo, digestOfClientMessage, seqNoOfPrePrepare, c, key);

            int currentViewNoOfPrimary = this.viewNo.get(); // i am initialising the view no variable here because a transaction has to complete within a view, otherwise it will be part of the view change, and also while verifying the prepares we check if the prepares have the current view no of the server or not
            if (gotPrepareQuorum) {
                System.out.println("Got Prepare Qurum");
                if (isByzantine.get()) {
                    // it will still log the prepare
                    this.logBasedOnSequenceNumber.get(seqNoOfPrePrepare).prepareDeQueue.addAll(checkPrepares.get(key));
                    logBasedOnSequenceNumber.get(seqNoOfPrePrepare).inProgress = false;
                    return;
                }
                boolean gotCommitQorum = sendPreparesToAll(currentViewNoOfPrimary, seqNoOfPrePrepare, digestOfClientMessage, key);
                this.checkPrepares.remove(key);

                if (gotCommitQorum) {

                    System.out.println("Got Commit Qurom");
                    sendCommitsToAll(currentViewNoOfPrimary, seqNoOfPrePrepare, key);
                    this.checkCommits.remove(key);

                    if (logBasedOnSequenceNumber.get(seqNoOfPrePrepare).state != Log.State.EXECUTE) {
                        execute(seqNoOfPrePrepare);
                        t = logBasedOnSequenceNumber.get(seqNoOfPrePrepare).prePrepare.getMessage().getT();
                        sender = t.getSenderId();
                        receiver = t.getReceiverId();
                        senderCluster = getClusterIndex(sender);
                        receiverCluster = getClusterIndex(receiver);
                        Thread.sleep(100);
                        if (senderCluster != receiverCluster) {
                            System.out.println("Sending result!!");
                            coordinatorTimer.put(clientKey, new CustomTimer(() -> coordinatorTimerExpired(c, seqNoOfPrePrepare), 30, TimeUnit.SECONDS));
                            sendResultToAll(seqNoOfPrePrepare, CurrentStatus.PREPARED);
                            coordinatorTimer.get(clientKey).start();
                        }
                    }
                } else {
                    this.checkCommits.remove(key);
                    logBasedOnSequenceNumber.get(seqNoOfPrePrepare).inProgress = false;
                }

            } else {
                this.checkPrepares.remove(key);
                logBasedOnSequenceNumber.get(seqNoOfPrePrepare).inProgress = false;
            }

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException | SignatureException | InterruptedException e) {

        }

    }


    @Override
    public void sendPrePrepare(PrePrepare prePrepare, StreamObserver<Prepare> streamObserver) {

        if (prePrepare.getMessage().getT().getSenderId() == 2770) {
            System.out.println("Got the preparrrrerere!!");
        }

        if (!isAlive.get()) return;

        int viewNoOfBackup = this.viewNo.get(), sequenceNumberInPrePrepare = prePrepare.getSequenceNo();

        // verify signature of prePrepare message
        PublicKey publicKeyOfPrimary = publicKeys.get(serverIdToServerKeyId.get(prePrepare.getServerId()));


        System.out.println(serverIdToServerKeyId.get(prePrepare.getServerId()));

        if (!checkPrePrepareSignature(prePrepare, publicKeyOfPrimary)) return;


        Client c = prePrepare.getMessage();

        // verify signature of client message
        if (!checkClientSignature(c)) return;


        // verify the digest of m
        String digestInPrePrepare = prePrepare.getDigest();
        try {
            if (!digestInPrePrepare.equals(createDigestOfClientMessage(c))) return;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        // verify view no
        if (prePrepare.getViewNo() != viewNoOfBackup) return;

        //it has not accepted a pre-prepare message for this sequence number

        if ((this.logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) && logBasedOnSequenceNumber.get(sequenceNumberInPrePrepare).viewNo == viewNoOfBackup) && (this.logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) && !logBasedOnSequenceNumber.get(sequenceNumberInPrePrepare).prePrepare.getDigest().equals(digestInPrePrepare)))
            return;

        Transaction t = c.getT();

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

        double amount = t.getAmount();

        String clientKey = getClientKey(c);

        // balance condition

        if (prePrepare.getStatus() != CurrentStatus.COMMITTED && prePrepare.getStatus() != CurrentStatus.ABORT) {

            // balance condition

            // locks condition
            if (senderCluster == clusterId) {
                if (locks.containsKey(sender) && !locks.get(sender).equals(clientKey)) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                    }
                }
            }
            if (receiverCluster == clusterId) {
                if (locks.containsKey(receiver) && !locks.get(receiver).equals(clientKey)) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                    }
                }
            }
            if (senderCluster == clusterId) {
                if (clientBalances.get(sender) < amount) {
                    return;
                }
            }
        }

        //between a low watermark, and a high watermark.

        if (sequenceNumberInPrePrepare < 0 || sequenceNumberInPrePrepare > 2000) return;


        // if we don't have all previous sequence numbers ignore the request
        if (sequenceNumberInPrePrepare > 1 && !logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare - 1)) {
            if (sender == 2770) {
                System.out.println(logBasedOnSequenceNumber);
                System.out.println("Log not up to date!! " + "this is the seqNo " + sequenceNumberInPrePrepare);
            }
            return;
        }

        if (prePrepare.getMessage().getT().getSenderId() == 2770) {
            System.out.println("Got the valid preparrrrerere!!");
        }

        // we start the timer once we know that the prePrepare is valid

        if (!logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) || (logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) && logBasedOnSequenceNumber.get(sequenceNumberInPrePrepare).state != Log.State.EXECUTE))
            unExecutedRequests.put(sequenceNumberInPrePrepare, 1);
        // append in prePrepare, since all the conditions have passed

        if (!logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) || (logBasedOnSequenceNumber.containsKey(sequenceNumberInPrePrepare) && logBasedOnSequenceNumber.get(sequenceNumberInPrePrepare).state == Log.State.PREPREPARE))
            addPrePrepare(prePrepare);

        if (prePrepare.getStatus() != CurrentStatus.NOSTATUS) {
            resultOfTheSequenceNumber.put(getClientKey(c), prePrepare.getStatus());
            logBasedOnSequenceNumber.get(sequenceNumberInPrePrepare).crossShardStatus = prePrepare.getStatus();
        }

        if (isByzantine.get()) {
            return;
        }

        if (!this.requestTimer.isRunning()) {
            this.requestTimer.start();
        }

        unExecutedRequests.put(sequenceNumberInPrePrepare, 1);

        if (!clientRequests.containsKey(c)) {
            if (sender == 45) {
                System.out.println("This isss this ----" + sequenceNumberInPrePrepare);
            }
            clientRequests.put(getClientKey(c), sequenceNumberInPrePrepare);
        } else {
        }
        //send prepare
        try {
            Prepare signedPrepare = createSignedPrepare(prePrepare);
            streamObserver.onNext(signedPrepare);
            streamObserver.onCompleted();

        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendClientMessageToLeader(Client c) {

        Context newContext = Context.current().fork();
        Context origContext = newContext.attach();

//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + getCurrentLeaderPort()).usePlaintext().build();
//        PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);
        stubs[getCurrentLeaderPort()].clientRequest(c, new StreamObserver<Empty>() {
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
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            newContext.detach(origContext);
        }
    }

    private String getCurrentLeader() {
        if (clusterId == 1) {
            return "S1";
        } else if (clusterId == 2) {
            return "S5";
        } else {
            return "S9";
        }
    }

    private int getCurrentLeaderPort() {
        return serverPorts.get(getCurrentLeader());
    }

    private String getClientKey(Client c) {
        return c.getClientId() + "." + c.getTimestamp();
    }

    private boolean sendPrePrepareToAll(int viewNo, String digest, int sequenceNumberOfPrePrepare, Client c, String key) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException {

        Context newContext = Context.current().fork();
        Context origContext = newContext.attach();
        // First create the signed PrePrepare
        PrePrepare signedPrePreparePrepare = createSignedPrePrepare(viewNo, digest, sequenceNumberOfPrePrepare, c);

//        if(resultOfTheSequenceNumber.containsKey(sequenceNumberOfPrePrepare)) {
//            signedPrePreparePrepare = signedPrePreparePrepare.toBuilder().setStatus(resultOfTheSequenceNumber.get(sequenceNumberOfPrePrepare)).build();
//        }

        if (!logBasedOnSequenceNumber.containsKey(sequenceNumberOfPrePrepare) || (logBasedOnSequenceNumber.containsKey(sequenceNumberOfPrePrepare) && logBasedOnSequenceNumber.get(sequenceNumberOfPrePrepare).state == Log.State.PREPREPARE)) {
            // Add the signed PrePrepare to the log
            addPrePrepare(signedPrePreparePrepare);
        }

        logBasedOnSequenceNumber.get(sequenceNumberOfPrePrepare).inProgress = true;
        Prepare signedPrepareOfLeader = createSignedPrepare(signedPrePreparePrepare);

        checkPrepares.putIfAbsent(key, new ConcurrentLinkedDeque<>());
        checkPrepares.get(key).add(signedPrepareOfLeader);

        // Set the number of expected prepares (n - f)
        CountDownLatch latch = new CountDownLatch(peers.size());
        for (ManagedChannel peer : peers) {
            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(peer);

            asyncStub.sendPrePrepare(signedPrePreparePrepare, new StreamObserver<Prepare>() {

                @Override
                public void onNext(Prepare prepare) {
                    if (verifyPrepare(prepare)) {
                        checkPrepares.putIfAbsent(key, new ConcurrentLinkedDeque<>());
                        checkPrepares.get(key).add(prepare);
                    }
                    // Count down the latch
                    latch.countDown();
                }

                @Override
                public void onError(Throwable throwable) {
                    // Log the error if necessary
                }

                @Override
                public void onCompleted() {
                    // Optionally handle completion if needed
                }
            });
        }

        try {
            Thread.sleep(200);
            // Wait for quorum or timeout after 200 ms
            if (this.checkPrepares.containsKey(key) && this.checkPrepares.get(key).size() >= 3) {
                return true;
            } else {
                return false;
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            newContext.detach(origContext);
        }
        return false;
    }


    // need to test this logic
    @Override
    public void sendAllPrepares(Prepares prepares, StreamObserver<Commit> streamObserver) {

        if (!isAlive.get()) return;

        if (isByzantine.get()) {
            logBasedOnSequenceNumber.get(prepares.getPrepare().getSequenceNo()).prepareDeQueue.addAll(prepares.getPreparesList());
            setPrepare(prepares.getPrepare().getSequenceNo());
            return;
        }

        if (viewChangeStarted.get()) return;

        List<Prepare> preparesList = prepares.getPreparesList();
        Prepare prepareOfLeader = prepares.getPrepare();
        if (!verifyPrepare(prepareOfLeader)) return;

        List<Prepare> verifiedPrepares = new ArrayList<>();

        for (Prepare prepare : preparesList) {
            if (verifyPrepare(prepare)) {
                verifiedPrepares.add(prepare);
            }
        }

        // using the validPrepare we can send the commit message
        if (verifiedPrepares.size() >= 3) {
            try {
                Log.State state = logBasedOnSequenceNumber.get(prepareOfLeader.getSequenceNo()).state;
                if (state != Log.State.PREPARE && state != Log.State.COMMIT && state != Log.State.EXECUTE) {
                    logBasedOnSequenceNumber.get(prepareOfLeader.getSequenceNo()).prepareDeQueue.addAll(verifiedPrepares);
                    logBasedOnSequenceNumber.get(prepareOfLeader.getSequenceNo()).state = Log.State.PREPARE;
                    setPrepare(prepareOfLeader.getSequenceNo());
                }
                Commit signedCommit = createSignedCommit(logBasedOnSequenceNumber.get(prepareOfLeader.getSequenceNo()).prepare);

                if (isByzantine.get()) {
                    return;
                } else {
                    streamObserver.onNext(signedCommit);
                    streamObserver.onCompleted();
                }
            } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
                throw new RuntimeException(e);
            }

        }

    }

    @Override
    public void sendAllCommits(Commits commits, StreamObserver<Empty> streamObserver) {

        if (!isAlive.get()) return;
        if (viewChangeStarted.get()) return;
        if (isByzantine.get()) {
            this.logBasedOnSequenceNumber.get(commits.getCommit().getSequenceNo()).commitDeQueue.addAll(commits.getCommitsList());
            setCommit(commits.getCommit().getSequenceNo());
            return;
        }

        List<Commit> commitList = commits.getCommitsList();
        Commit commitOfLeader = commits.getCommit();

        if (!verifyCommit(commitOfLeader)) return;

        List<Commit> verifiedCommits = new ArrayList<>();

        for (Commit commit : commitList) {
            if (verifyCommit(commit)) {
                verifiedCommits.add(commit);
            }
        }
        if (verifiedCommits.size() >= 3) {
            // execution phase
            Log.State state = logBasedOnSequenceNumber.get(commitOfLeader.getSequenceNo()).state;
            if (state != Log.State.COMMIT && state != Log.State.EXECUTE) {
                logBasedOnSequenceNumber.get(commitOfLeader.getSequenceNo()).commitDeQueue.addAll(verifiedCommits);
                logBasedOnSequenceNumber.get(commitOfLeader.getSequenceNo()).state = Log.State.COMMIT;
                setCommit(commitOfLeader.getSequenceNo());
            }
            try {
                execute(commitOfLeader.getSequenceNo());
            } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();
    }

    private boolean sendPreparesToAll(int currentViewNo, int prePrepareSequeunceNumber, String digestOfClientMessage, String key) {

        Prepares prepares = null;

        Log.State state = logBasedOnSequenceNumber.get(prePrepareSequeunceNumber).state;
        if (state == Log.State.PREPARE || state == Log.State.EXECUTE || state == Log.State.COMMIT) {
            Prepare leaderPrepare = null;
            ConcurrentLinkedDeque<Prepare> prepareList = this.checkPrepares.get(key);

            for (Prepare prepare : prepareList) {
                if (prepare.getServerId().equals(this.serverId)) {
                    leaderPrepare = prepare;
                    break;
                }
            }

            prepares = Prepares.newBuilder().addAllPrepares(prepareList).setPrepare(leaderPrepare).build();
        } else {
            this.logBasedOnSequenceNumber.get(prePrepareSequeunceNumber).prepareDeQueue.addAll(checkPrepares.get(key));
            setPrepare(prePrepareSequeunceNumber);
            this.logBasedOnSequenceNumber.get(prePrepareSequeunceNumber).state = Log.State.PREPARE;
            prepares = Prepares.newBuilder().addAllPrepares(this.logBasedOnSequenceNumber.get(prePrepareSequeunceNumber).prepareDeQueue).setPrepare(this.logBasedOnSequenceNumber.get(prePrepareSequeunceNumber).prepare).build();
        }

        CountDownLatch latch = new CountDownLatch(peers.size());

        try {
            Commit signedCommitOfLeader = createSignedCommitOfLeader(prePrepareSequeunceNumber, currentViewNo, digestOfClientMessage);

            checkCommits.putIfAbsent(key, new ConcurrentLinkedDeque<>());
            checkCommits.get(key).add(signedCommitOfLeader);

        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }

        for (ManagedChannel peer : peers) {
            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(peer);

            asyncStub.sendAllPrepares(prepares, new StreamObserver<Commit>() {
                @Override
                public void onNext(Commit commit) {
//                        System.out.println("Got this commit from: " + commit.getServerId() + commit);
                    if (verifyCommit(commit)) {
                        checkCommits.putIfAbsent(key, new ConcurrentLinkedDeque<>());
                        checkCommits.get(key).add(commit);
                    }
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
            Thread.sleep(200);
            // Wait for quorum or timeout after 200 ms
            if (checkCommits.containsKey(key) && checkCommits.get(key).size() >= 3) {
//                    System.out.println("Got Commit Quorum");
                return true;
            } else {
                return false;
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
//            System.err.println("Interrupted while waiting for quorum: " + e.getMessage());
        }
        return false;
    }

    public void sendCommitsToAll(int currentViewNo, int prePrepareSequenceNumber, String key) {

        Commits commits;
        Log.State state = logBasedOnSequenceNumber.get(prePrepareSequenceNumber).state;
        if (state == Log.State.COMMIT || state == Log.State.EXECUTE) {
            ConcurrentLinkedDeque<Commit> commitsList = checkCommits.get(key);

            Commit leaderCommit = null;

            for (Commit commit : commitsList) {
                if (commit.getServerId().equals(commit.getServerId())) {
                    leaderCommit = commit;
                }
            }
            commits = Commits.newBuilder().addAllCommits(commitsList).setCommit(leaderCommit).build();

        } else {
            this.logBasedOnSequenceNumber.get(prePrepareSequenceNumber).commitDeQueue.addAll(checkCommits.get(key));
            setCommit(prePrepareSequenceNumber);
            this.logBasedOnSequenceNumber.get(prePrepareSequenceNumber).state = Log.State.COMMIT;
            commits = Commits.newBuilder().addAllCommits(this.logBasedOnSequenceNumber.get(prePrepareSequenceNumber).commitDeQueue).setCommit(this.logBasedOnSequenceNumber.get(prePrepareSequenceNumber).commit).build();
        }


        CountDownLatch latch = new CountDownLatch(peers.size());

        for (ManagedChannel peer : peers) {
            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(peer);
            asyncStub.sendAllCommits(commits, new StreamObserver<Empty>() {
                @Override
                public void onNext(Empty empty) {
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
            if (!latch.await(2000, TimeUnit.MILLISECONDS)) {
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void setPrepare(int sequenceNumberOfPrepare) {
        for (Prepare prepare : this.logBasedOnSequenceNumber.get(sequenceNumberOfPrepare).prepareDeQueue) {
            if (this.serverId.equals(prepare.getServerId())) {
                this.logBasedOnSequenceNumber.get(sequenceNumberOfPrepare).prepare = prepare;
                break;
            }
        }
    }

    private void setCommit(int sequenceNumberOfPrepare) {
        for (Commit commit : this.logBasedOnSequenceNumber.get(sequenceNumberOfPrepare).commitDeQueue) {
            if (this.serverId.equals(commit.getServerId())) {
                this.logBasedOnSequenceNumber.get(sequenceNumberOfPrepare).commit = commit;
                break;
            }
        }
    }


    private boolean checkPrePrepareSignature(PrePrepare prePrepare, PublicKey publicKeyOfPrimary) {
        if (!verifySignature(prePrepare.toBuilder().clearSignature().clearMessage().build().toByteArray(), prePrepare.getSignature().toByteArray(), publicKeyOfPrimary))
            return false;
        return true;
    }

    private boolean checkPrepareSignature(Prepare prepare, PublicKey publicKey) {
        if (!verifySignature(prepare.toBuilder().clearSignature().build().toByteArray(), prepare.getSignature().toByteArray(), publicKey))
            return false;
        return true;
    }

    private boolean checkCommitSignature(Commit commit, PublicKey publicKey) {
        if (!verifySignature(commit.toBuilder().clearSignature().build().toByteArray(), commit.getSignature().toByteArray(), publicKey))
            return false;

        return true;
    }

    private boolean checkClientSignature(Client c) {
        int clientId = c.getClientId();
        ByteString clientSignature = c.getSignature();
        byte[] clientSignatureBytes = clientSignature.toByteArray();
        PublicKey clientPublicKey = this.publicKeys.get(clientId);
        byte[] clientData = c.toBuilder().clearSignature().build().toByteArray();
        return verifySignature(clientData, clientSignatureBytes, clientPublicKey);
    }

    private void incrementSequenceNumber() {
        int currentSequenceNumber = this.sequenceNumber.get();
        sequenceNumber.set(currentSequenceNumber + 1);
    }

    private String createDigestOfClientMessage(Client c) throws NoSuchAlgorithmException {
        byte[] clientData = c.toBuilder().clearSignature().build().toByteArray();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(clientData);
        byte[] hashBytes = digest.digest();
        return Base64.getEncoder().encodeToString(hashBytes);
    }

    private boolean verifySignature(byte[] data, byte[] signature, PublicKey publicKey) {
        try {
            Signature sig = Signature.getInstance("SHA256withRSA");
            sig.initVerify(publicKey);
            sig.update(data);
            return sig.verify(signature);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private PrePrepare createSignedPrePrepare(int viewNo, String digest, int sequenceNumberOfPrepare, Client c) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException {
        PrePrepare prePrepare;

        if (resultOfTheSequenceNumber.containsKey(getClientKey(c))) {
            prePrepare = PrePrepare.newBuilder().setViewNo(viewNo).setSequenceNo(sequenceNumberOfPrepare).setDigest(digest).setStatus(resultOfTheSequenceNumber.get(getClientKey(c))).setServerId(this.serverId).build();
        } else {
            prePrepare = PrePrepare.newBuilder().setViewNo(viewNo).setSequenceNo(sequenceNumberOfPrepare).setDigest(digest).setServerId(this.serverId).build();
        }
        byte[] signedPrePrepare = signMessage(prePrepare.toByteArray(), this.privateKey);
        PrePrepare signedPrePrepareProto;
        if (c == null) {
            signedPrePrepareProto = prePrepare.toBuilder().setSignature(ByteString.copyFrom(signedPrePrepare)).build();
        } else {

            signedPrePrepareProto = prePrepare.toBuilder().setSignature(ByteString.copyFrom(signedPrePrepare)).setMessage(c).build();
        }
        return signedPrePrepareProto;
    }

    private Commit createSignedCommit(Prepare prepare) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException {
        Commit commit = Commit.newBuilder().setDigest(prepare.getDigest()).setServerId(this.serverId).setViewNo(prepare.getViewNo()).setSequenceNo(prepare.getSequenceNo()).build();
        byte[] signedCommit = signMessage(commit.toByteArray(), this.privateKey);
        Commit signedCommitProto = commit.toBuilder().setSignature(ByteString.copyFrom(signedCommit)).build();
        return signedCommitProto;
    }

    private boolean verifyPrepare(Prepare prepare) {
        int viewNoOfPrepare = prepare.getViewNo(), sequenceNumberOfPrepare = prepare.getSequenceNo();

        PublicKey publicKeyOfPrepareSender = this.publicKeys.get(serverIdToServerKeyId.get(prepare.getServerId()));

        // First I check the signature of prepare
        if (!checkPrepareSignature(prepare, publicKeyOfPrepareSender)) return false;


        // check if the current view of the server and that of prepare is same or not
        if (this.viewNo.get() != viewNoOfPrepare) return false;

        // second I check if the corresponding PrePrepare exist in the log
        if (this.logBasedOnSequenceNumber.containsKey(sequenceNumberOfPrepare)) {
            PrePrepare correspondingPrePrepare = this.logBasedOnSequenceNumber.get(sequenceNumberOfPrepare).prePrepare;

            // then I check the sequenceNo, viewNo and the digest
            if (prepare.getSequenceNo() == correspondingPrePrepare.getSequenceNo() && prepare.getDigest().equals(correspondingPrePrepare.getDigest())) {
                return true;
            }
        }
        return false;
    }

    private boolean verifyCommit(Commit commit) {
        int viewNoOfCommit = commit.getViewNo(), sequenceNumberOfCommit = commit.getSequenceNo();

        PublicKey publicKeyOfPrepareSender = this.publicKeys.get(serverIdToServerKeyId.get(commit.getServerId()));

        // First I check the signature of prepare
        if (!checkCommitSignature(commit, publicKeyOfPrepareSender)) {
            return false;
        }

        // check if the current view of the server is same as that of commit or not
        if (this.viewNo.get() != viewNoOfCommit) return false;

        // second I check if the corresponding PrePrepare exist in the log
        if (this.logBasedOnSequenceNumber.containsKey(sequenceNumberOfCommit)) {
            PrePrepare correspondingPrePrepare = this.logBasedOnSequenceNumber.get(sequenceNumberOfCommit).prePrepare;

            // then I check the sequenceNo, viewNo and the digest
            if (commit.getSequenceNo() == correspondingPrePrepare.getSequenceNo() && commit.getDigest().equals(correspondingPrePrepare.getDigest())) {
                return true;
            }
        }
        return false;
    }

    private Prepare createSignedPrepare(PrePrepare prePrepare) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException {
        Prepare prepare = Prepare.newBuilder().setDigest(prePrepare.getDigest()).setServerId(this.serverId).setSequenceNo(prePrepare.getSequenceNo()).setViewNo(prePrepare.getViewNo()).build();
        byte[] signedPrepare = signMessage(prepare.toByteArray(), this.privateKey);
        Prepare signedPrepareProto = prepare.toBuilder().setSignature(ByteString.copyFrom(signedPrepare)).build();
        return signedPrepareProto;
    }

    private void addPrePrepare(PrePrepare prePrepare) {
        int viewNoForPrePrepare = prePrepare.getViewNo(), sequenceNumberForPrePrepare = prePrepare.getSequenceNo();
        Log log = new Log();
        log.prePrepare = prePrepare;
        log.sequenceNumber = sequenceNumberForPrePrepare;
        log.viewNo = viewNoForPrePrepare;
        log.state = Log.State.PREPREPARE;
        if (prePrepare.getStatus() != null && prePrepare.getStatus() != CurrentStatus.NOSTATUS) {
            log.crossShardStatus = prePrepare.getStatus();
        }

        this.logBasedOnSequenceNumber.put(sequenceNumberForPrePrepare, log);

    }

    private Commit createSignedCommitOfLeader(int prePrepareSequeunceNumber, int currentViewNo, String digestOfClientMessage) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException {

        Commit commitOfLeader = Commit.newBuilder().setDigest(digestOfClientMessage).setServerId(this.serverId).setSequenceNo(prePrepareSequeunceNumber).setViewNo(currentViewNo).build();
        byte[] signedCommit = signMessage(commitOfLeader.toByteArray(), this.privateKey);
        Commit signedCommitOfLeader = commitOfLeader.toBuilder().setSignature(ByteString.copyFrom(signedCommit)).build();
        return signedCommitOfLeader;
    }

    private void addPrepare(Prepare prepare) {
        int viewNoForPrepare = prepare.getViewNo(), sequenceNumberForPrepare = prepare.getSequenceNo();
        this.logBasedOnSequenceNumber.get(sequenceNumberForPrepare).prepareDeQueue.add(prepare);
    }

    private void addCommit(Commit commit) {
        int viewNoForCommit = commit.getViewNo(), sequenceNumberForCommit = commit.getSequenceNo();
        this.logBasedOnSequenceNumber.get(sequenceNumberForCommit).commitDeQueue.add(commit);
    }

    private byte[] signMessage(byte[] messageData, PrivateKey privateKey) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException {
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(messageData);
        return signature.sign();
    }


    private boolean checkViewChangeSignature(ViewChange viewChangeMessage) {
        return verifySignature(viewChangeMessage.toBuilder().clearSignature().build().toByteArray(), viewChangeMessage.getSignature().toByteArray(), publicKeys.get(serverIdToServerKeyId.get(viewChangeMessage.getServerId())));
    }

    @Override
    public void sendViewChange(ViewChange viewChangeMessage, StreamObserver<Empty> streamObserver) {

        if (!isAlive.get()) return;

        try {
            Thread.sleep(20 + new Random().nextInt(1));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();

        if (!checkViewChangeSignature(viewChangeMessage)) return;

        int viewNo = viewChangeMessage.getViewNo();

        viewChangeMessages.putIfAbsent(viewNo, new ArrayList<>());
        List<ViewChange> messages = new ArrayList<>();
        for (ViewChange message : messages) {
            if (message.getSignature().equals(viewChangeMessage.getSignature())) {
                return;
            }
        }
        viewChangeMessages.get(viewNo).add(viewChangeMessage);

        if (viewChangeMessages.get(viewNo).size() >= 3) {
            String newLeader = ("S" + (((viewNo) % 7) + 1));

            if (newLeader.equals(this.serverId) && !newViewSentCheck.containsKey(viewNo)) {
                if (isByzantine.get()) {
                    return;
                }
                newViewSentCheck.put(viewNo, true);
                this.viewNo.set(viewChangeMessage.getViewNo());
                NewView newViewMessage = createNewViewMessage();
                broadCastNewViewMessage(newViewMessage);
                successfulViewChangeMessages.put(newViewMessage.getViewNo(), viewChangeMessages.get(newViewMessage.getViewNo()));
                successfulNewViewMessages.put(newViewMessage.getViewNo(), newViewMessage);
                this.viewChangeMessages = new ConcurrentHashMap<>();
                // now I need to broadcast view change messages, using already made functions
                sendPrePreparesToCatchUp(newViewMessage);
            } else if (!newLeader.equals(this.serverId)) {
                // this should only be started when I am sure I have not received the new view change message already!!!
                if (!gotNwViewMessage.containsKey(viewNo) && !viewChangeTimer.isRunning()) {
                    System.out.println("Got Quorum of view changes!!");
                    viewChangeTimer.start();
                }
            }


        } else if (viewChangeMessages.get(viewNo).size() >= 3) {

            // now this node will broadcast view change for this view
            if (!viewChangeStarted.get()) {
                if (requestTimer.isRunning()) {
                    requestTimer.stop();
                }
                ViewChange thisNodeViewChangeMessage = createViewChange(viewNo);
                viewChangeMessages.get(viewNo).add(thisNodeViewChangeMessage);
                viewChangeStarted.set(true);
                lastViewChangeMessageSent = thisNodeViewChangeMessage;
                broadCastViewChangeMessage(thisNodeViewChangeMessage);
            }
        }


    }

    @Override
    public void sendNewView(NewView newView, StreamObserver<Empty> streamObserver) {

        if (!isAlive.get()) return;

        System.out.println("Got New View !!");

        if (viewChangeTimer.isRunning()) {
            this.viewChangeTimer.stop();
        }
        this.gotNwViewMessage.put(newView.getViewNo(), true);
        this.viewNo.set(newView.getViewNo());
        System.out.println(this.serverId + "is in view: " + this.viewNo);
        this.viewChangeStarted.set(false);
        List<PrePrepare> prePrepares = newView.getPrePreparesList();

        for (PrePrepare prePrepare : prePrepares) {
            if (logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo()) && prePrepare.getDigest().equals("null")) {
                clientRequests.remove(getClientKey(logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).prePrepare.getMessage()));
            }
        }

        for (PrePrepare prePrepare : prePrepares) {
            if (logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo())) {
                if (!logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).prePrepare.getDigest().equals(prePrepare.getDigest())) {
                    addPrePrepare(prePrepare);
                }
            }
        }


        successfulViewChangeMessages.put(newView.getViewNo(), viewChangeMessages.get(newView.getViewNo()));
        successfulNewViewMessages.put(newView.getViewNo(), newView);

        // clear maps
        this.viewChangeMessages = new ConcurrentHashMap<>();
    }

    private void sendPrePreparesToCatchUp(NewView newViewMessage) {

        for (PrePrepare prePrepare : newViewMessage.getPrePreparesList()) {
            if (logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo()) && prePrepare.getDigest().equals("null")) {
                clientRequests.remove(getClientKey(logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).prePrepare.getMessage()));
            }
        }
        for (PrePrepare prePrepare : newViewMessage.getPrePreparesList()) {
            if (logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo())) {
                if (!logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).prepare.getDigest().equals(prePrepare.getDigest())) {
                    addPrePrepare(prePrepare);
                }
            }
        }
        this.viewChangeStarted.set(false);

        int viewNo = this.viewNo.get();

        List<PrePrepare> prePrepares = newViewMessage.getPrePreparesList();


        // sorting prePrepares based on sequence numbers
//        Collections.sort(prePrepares, (a,b) ->{
//            return a.getSequenceNo() - b.getSequenceNo();
//        });
        if (prePrepares.size() > 0) {
            this.sequenceNumber.set(Math.max(this.sequenceNumber.get(), prePrepares.get(prePrepares.size() - 1).getSequenceNo()));
        }
        for (PrePrepare prePrepare : prePrepares) {
            String key = UUID.randomUUID().toString();


            try {


                if (!logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo()) || (logBasedOnSequenceNumber.containsKey(prePrepare.getSequenceNo()) && logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).state != Log.State.EXECUTE)) {
                    unExecutedRequests.put(prePrepare.getSequenceNo(), 1);
                }

                clientRequests.put(getClientKey(prePrepare.getMessage()), prePrepare.getSequenceNo());

                boolean prepareQuorum = sendPrePrepareToAll(viewNo, prePrepare.getDigest(), prePrepare.getSequenceNo(), prePrepare.getMessage(), key);

                if (prepareQuorum) {
                    boolean gotCommitQorum = sendPreparesToAll(viewNo, prePrepare.getSequenceNo(), prePrepare.getDigest(), key);
                    this.checkPrepares.remove(key);

                    if (gotCommitQorum) {
                        sendCommitsToAll(viewNo, prePrepare.getSequenceNo(), key);
                        this.checkCommits.remove(key);
                        execute(prePrepare.getSequenceNo());
                    } else {
                        this.checkCommits.remove(key);
                        logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).inProgress = false;
                    }
                } else {
                    this.checkPrepares.remove(key);
                    logBasedOnSequenceNumber.get(prePrepare.getSequenceNo()).inProgress = false;
                }

            } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void broadCastNewViewMessage(NewView newView) {
        for (ManagedChannel peer : peers) {
            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(peer);
            asyncStub.sendNewView(newView, new StreamObserver<Empty>() {
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

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private NewView createNewViewMessage() {
        // I have already updated the view no of the new leader
        int viewNo = this.viewNo.get();
        NewView.Builder newViewBuilder = NewView.newBuilder().setClientId(this.serverId).setViewNo(viewNo);

        List<ViewChange> viewChangeMessages = this.viewChangeMessages.get(viewNo);
        // remove duplicates here maybe

        HashMap<Integer, PrePrepare> prePrepareForSequenceNumber = new HashMap<>();

        int minS = 1, maxS = 0;

        for (ViewChange viewChangeMessage : viewChangeMessages) {
            List<setP> preparedRequests = viewChangeMessage.getPreparedRequestsList();

            for (setP p : preparedRequests) {
                PrePrepare prePrepare = p.getPrePrepare();

                // logic to send prePrepare with highest sequence Number
                if (!prePrepareForSequenceNumber.containsKey(prePrepare.getSequenceNo()) || (prePrepareForSequenceNumber.containsKey(prePrepare.getSequenceNo()) && prePrepareForSequenceNumber.get(prePrepare.getSequenceNo()).getViewNo() < prePrepare.getViewNo())) {
                    prePrepareForSequenceNumber.put(prePrepare.getSequenceNo(), prePrepare);
                    maxS = Math.max(maxS, prePrepare.getSequenceNo());
                }
            }
        }
        for (int i = minS; i <= maxS; i++) {
            if (prePrepareForSequenceNumber.containsKey(i)) {
                PrePrepare prePrepare = prePrepareForSequenceNumber.get(i);
                try {
                    PrePrepare prePrepareSignedByNewLeader = createSignedPrePrepare(prePrepare.getViewNo(), prePrepare.getDigest(), i, prePrepare.getMessage());
                    newViewBuilder.addPrePrepares(prePrepareSignedByNewLeader);
                } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
                    throw new RuntimeException(e);
                }

            } else {
                // create null operation prepepare
                try {
                    PrePrepare prePrepareSignedByNewLeader = createSignedPrePrepare(viewNo, "null", i, null);
                    newViewBuilder.addPrePrepares(prePrepareSignedByNewLeader);
                } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        newViewBuilder.addAllViewChangeMessages(viewChangeMessages);
        NewView newViewWithoutSignature = newViewBuilder.build();
        try {
            byte[] signature = signMessage(newViewWithoutSignature.toByteArray(), this.privateKey);
            NewView signedNiewView = newViewWithoutSignature.toBuilder().setSignature(ByteString.copyFrom(signature)).build();
            return signedNiewView;
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException(e);
        }
    }


    private void startConsecutiveViewChange() {
        int viewToBeSent;

        System.out.println(this.serverId + " is in consecutive view change !!");

        this.viewChangeStarted.set(true);

        if (lastViewChangeMessageSent == null) {
            viewToBeSent = this.viewNo.get() + 1;
        } else {
            viewToBeSent = Math.max(this.viewNo.get(), lastViewChangeMessageSent.getViewNo()) + 1;
        }
        ViewChange viewChange = createViewChange(viewToBeSent);
        lastViewChangeMessageSent = viewChange;
        broadCastViewChangeMessage(viewChange);
    }

    private void startViewChange() {
        // now it will only listen to view change related messages
        this.viewChangeStarted.set(true);
        int viewToBeSent;
        if (lastViewChangeMessageSent == null) {
            viewToBeSent = this.viewNo.get() + 1;
        } else {
            viewToBeSent = Math.max(this.viewNo.get(), lastViewChangeMessageSent.getViewNo()) + 1;
        }
        ViewChange viewChangeMessage = createViewChange(viewToBeSent);
        lastViewChangeMessageSent = viewChangeMessage;
        this.viewChangeMessages.putIfAbsent(viewToBeSent, new ArrayList<>());
        this.viewChangeMessages.get(viewToBeSent).add(viewChangeMessage);
        broadCastViewChangeMessage(viewChangeMessage);


    }

    private ViewChange createViewChange(int viewNo) {
        Set<Integer> sequenceNumbers = logBasedOnSequenceNumber.keySet();

        List<setP> listOfSetP = new ArrayList<>();


        for (int sequenceNumberInLog : sequenceNumbers) {
            Log.State stateOfLog = logBasedOnSequenceNumber.get(sequenceNumberInLog).state;
            if (stateOfLog == Log.State.PREPARE || stateOfLog == Log.State.COMMIT || stateOfLog == Log.State.EXECUTE) {
                setP p = setP.newBuilder().addAllMatchingPrepares(logBasedOnSequenceNumber.get(sequenceNumberInLog).prepareDeQueue).setPrePrepare(logBasedOnSequenceNumber.get(sequenceNumberInLog).prePrepare).build();
                listOfSetP.add(p);
            }
        }
        ViewChange viewChangeMessage = ViewChange.newBuilder().setServerId(this.serverId).setViewNo(viewNo).addAllPreparedRequests(listOfSetP).build();

        try {
            byte[] signedCommit = signMessage(viewChangeMessage.toByteArray(), this.privateKey);
            ViewChange signedViewChange = viewChangeMessage.toBuilder().setSignature(ByteString.copyFrom(signedCommit)).build();

            return signedViewChange;

        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException(e);
        }

    }

    private void broadCastViewChangeMessage(ViewChange viewChangeMessage) {
        for (ManagedChannel peer : peers) {
            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(peer);
            asyncStub.sendViewChange(viewChangeMessage, new StreamObserver<Empty>() {
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

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    // here I need to add the reply logic
    private synchronized void execute(int sequenceNumber) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, InterruptedException {


        Transaction tt = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage().getT();


        if (logBasedOnSequenceNumber.get(sequenceNumber).state == Log.State.EXECUTE) {

//            Transaction t = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage().getT();
//
//            Client c = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage();
//
//
//            int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);
//            double amount = t.getAmount();
//
//            if(sender != receiver && !decisionMadeForSequenceNumber.containsKey(sequenceNumber)) {
//               CurrentStatus state = logBasedOnSequenceNumber.get(sequenceNumber).crossShardStatus;
//
//               if(state == CurrentStatus.COMMITTED) {
//                   dataStore.add(new TransactionStatus(sequenceNumber,c, TransactionStatus.State.COMMITTED));
//               } else {
//                   // we need to abort the transaction first
//
//                   if(senderCluster == clusterId) {
//                       clientBalances.put(sender, clientBalances.get(sender) + amount);
//                   } else {
//                       clientBalances.put(receiver, clientBalances.get(receiver) - amount);
//                   }
//                   dataStore.add(new TransactionStatus(sequenceNumber, c, TransactionStatus.State.ABORT));
//               }
//                   // here we also have to stop the timer of the coordinator
//                logBasedOnSequenceNumber.get(sequenceNumber).inProgress = false;
//               decisionMadeForSequenceNumber.put(sequenceNumber, true);
//            }

            if (requestTimer.isRunning()) {
                requestTimer.stop();
            }

            if (unExecutedRequests.size() > 0) {
                requestTimer.reset();
            }
            return;
        }


        if (sequenceNumber > 1 && (!logBasedOnSequenceNumber.containsKey(sequenceNumber - 1) || logBasedOnSequenceNumber.get(sequenceNumber - 1).state != Log.State.EXECUTE)) {
            // cannot execute current request
        } else {
            this.requestTimer.stop();
            processTransaction(sequenceNumber);

            System.out.println("These are the unexecuted requests before removal ----- ------------" + "server id is--" + serverId + " " + unExecutedRequests);
            unExecutedRequests.remove(sequenceNumber);
            System.out.println("These are the unexecuted requests ----- ------------" + "server id is--" + serverId + " " + unExecutedRequests);
            logBasedOnSequenceNumber.get(sequenceNumber).state = Log.State.EXECUTE;

            Transaction t = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage().getT();

            int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

            CurrentStatus currentStatus = logBasedOnSequenceNumber.get(sequenceNumber).crossShardStatus;


            int next = sequenceNumber + 1;
            while (logBasedOnSequenceNumber.containsKey(next) && logBasedOnSequenceNumber.get(next).state == Log.State.COMMIT) {
                processTransaction(next);
                unExecutedRequests.remove(next);
                logBasedOnSequenceNumber.get(next).state = Log.State.EXECUTE;
                next = next + 1;
            }
            if (unExecutedRequests.size() > 0) {
                this.requestTimer.reset();
            }
        }
//        System.out.println(logBasedOnSequenceNumber);
    }

    public Reply createSignedReply(int sequenceNumber, double result) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException {
        Client message = this.logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage();
        Reply reply = Reply.newBuilder().setServerId(this.serverId).setClientId(message.getClientId()).setTimestamp(message.getTimestamp()).setResult(result).build();
        byte[] signature = signMessage(reply.toByteArray(), this.privateKey);
        Reply signedReply = reply.toBuilder().setSignature(ByteString.copyFrom(signature)).build();
        return signedReply;

    }

    public void sendReplyAgain(Reply reply) {

        Context newContext = Context.current().fork();
        Context origContext = newContext.attach();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);
        asyncStub.sendReplyToClient(reply, new StreamObserver<Empty>() {
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
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            newContext.detach(origContext);
        }
    }

    public void sendReplyToClientFromServer(int sequenceNumber, double result) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, InterruptedException {

        Reply signedReply = createSignedReply(sequenceNumber, result);
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
//        PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);

        stubs[0].sendReplyToClient(signedReply, new StreamObserver<Empty>() {
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

        logBasedOnSequenceNumber.get(sequenceNumber).inProgress = false;
        lastSentReply.put(getClientKey(logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage()), signedReply);
        Thread.sleep(10);
    }


    private void processTransaction(int sequenceNumber) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, InterruptedException {
        if (logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getDigest().equals("null")) {
            return;
        }

        Transaction t = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage().getT();

        Client c = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage();

        String clientKey = getClientKey(c);

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

        if (senderCluster != receiverCluster) {
            CurrentStatus crossShardStatus = logBasedOnSequenceNumber.get(sequenceNumber).crossShardStatus;
            if (crossShardStatus != null && (crossShardStatus == CurrentStatus.COMMITTED || crossShardStatus == CurrentStatus.ABORT)) {

                int prevSeqNo = clientRequests.get(clientKey);

                if (prevSeqNo == sequenceNumber) {
                    Set<Integer> logs = logBasedOnSequenceNumber.keySet();
                    for (int seq : logs) {
                        if (getClientKey(logBasedOnSequenceNumber.get(seq).prePrepare.getMessage()).equals(clientKey)) {
                            if (seq != sequenceNumber)
                                prevSeqNo = seq;
                        }
                    }
                }
                // checking if the prev seq no request was executed or not
                if (logBasedOnSequenceNumber.get(prevSeqNo).state == Log.State.EXECUTE) {
                    if (crossShardStatus == CurrentStatus.COMMITTED) {
                        rollbackWal(clientKey);
                        dataStore.add(new TransactionStatus(sequenceNumber, c, TransactionStatus.State.COMMITTED));
                    } else {
                        // reverse transaction here
                        System.out.println("Here-----------------------" + senderCluster + " " + clusterId);
                        if (senderCluster == clusterId) {
                            double balance = getBalance(sender);
                            updateBalanceSender(sender, balance + t.getAmount());
                            clientBalances.put(sender, clientBalances.get(sender) + t.getAmount());
                        } else {
                            double balance = getBalance(receiver);
                            updateBalanceReceiver(receiver, balance - t.getAmount());
                            clientBalances.put(receiver, clientBalances.get(receiver) - t.getAmount());
                        }
                        rollbackWal(clientKey);
                        dataStore.add(new TransactionStatus(sequenceNumber, c, TransactionStatus.State.ABORT));
                    }
                }

                System.out.println("this is the server " + serverId + "  " + dataStore);
                // release locks
                if (senderCluster == clusterId) {
                    locks.remove(sender);
                }
                if (receiverCluster == clusterId) {
                    locks.remove(receiver);
                }

                if (senderCluster == clusterId) {
                    logBasedOnSequenceNumber.get(prevSeqNo).inProgress = false;
                }
                logBasedOnSequenceNumber.get(sequenceNumber).inProgress = false;
            } else {
                // preform the transaction here
                if (senderCluster == clusterId) {

                    double balance = getBalance(sender);
                    System.out.println("balance of " + sender + " is " + balance);
                    updateBalanceSender(sender, (balance - t.getAmount()));
                    clientBalances.put(sender, clientBalances.get(sender) - t.getAmount());
                } else {
                    double balance = getBalance(receiver);
                    updateBalanceReceiver(receiver, (balance + t.getAmount()));
                    System.out.println(receiver);
                    System.out.println(serverId);
                    clientBalances.put(receiver, clientBalances.get(receiver) + t.getAmount());
                }
                updateWal(clientKey,t,CurrentStatus.PREPARED);
                dataStore.add(new TransactionStatus(sequenceNumber, c, TransactionStatus.State.PREPARE));

                if (receiverCluster == clusterId) {
                    logBasedOnSequenceNumber.get(sequenceNumber).inProgress = false;
                }
            }
        } else {
            // release locks
            if (senderCluster == clusterId) {
                locks.remove(sender);
            }
            if (receiverCluster == clusterId) {
                locks.remove(receiver);
            }

            double balance = getBalance(sender);
            updateBalanceSender(sender, (balance - t.getAmount()));
            clientBalances.put(sender, clientBalances.get(sender) - t.getAmount());
            balance = getBalance(receiver);
            updateBalanceReceiver(receiver, (balance + t.getAmount()));
            clientBalances.put(receiver, clientBalances.get(receiver) + t.getAmount());
            dataStore.add(new TransactionStatus(sequenceNumber, c, TransactionStatus.State.COMMITTED));
            decisionMadeForSequenceNumber.put(sequenceNumber, true);
        }

        timeTakenForExecutingSequenceNumber.put(sequenceNumber, Duration.between(logBasedOnSequenceNumber.get(sequenceNumber).startTime, LocalTime.now()));

        CurrentStatus crossShardStatus = logBasedOnSequenceNumber.get(sequenceNumber).crossShardStatus;

        if (senderCluster == receiverCluster) {
            sendReplyToClientFromServer(sequenceNumber, clientBalances.get(t.getSenderId()));
        } else if (senderCluster == clusterId && crossShardStatus != null && (crossShardStatus == CurrentStatus.COMMITTED || crossShardStatus == CurrentStatus.ABORT)) {
            System.out.println("Sent Reply to Client!!");
            sendReplyToClientFromServer(sequenceNumber, clientBalances.get(t.getSenderId()));
        }
    }

    private void sendResultToAll(int sequenceNumber, CurrentStatus currentStatus) {

        Transaction t = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare.getMessage().getT();

        PrePrepare prePrepare = logBasedOnSequenceNumber.get(sequenceNumber).prePrepare;

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

        Result result = Result.newBuilder().setStatus(currentStatus).setPrePrepare(prePrepare).addAllCommits(logBasedOnSequenceNumber.get(sequenceNumber).commitDeQueue).build();

        if (senderCluster == clusterId) {

            // send message to receiver cluster
            sendResultToEveryNode(result, receiverCluster);
        } else {
            // send message to sender cluster
            sendResultToEveryNode(result, senderCluster);
        }
    }

    private void sendResultToEveryNode(Result result, int clusterIndex) {

        int start = 1;

        if (clusterIndex == 1) {
            start = 1;
        } else if (clusterIndex == 2) {
            start = 5;
        } else {
            start = 9;
        }

        for (int i = start; i <= (start + 3); i++) {
            System.out.println("sending result -- " + start);
//            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000 + i).usePlaintext().build();
//            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);
            stubs[i].sendResult(result, new StreamObserver<Empty>() {
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
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int getStartIndex(int clusterId) {
        if (clusterId == 1) {
            return 1;
        } else if (clusterId == 2) {
            return 5;
        } else {
            return 9;
        }
    }


    @Override
    public void setAlive(Alive aliveStatus, StreamObserver<Empty> streamObserver) {
        isAlive.set(aliveStatus.getIsAlive());
        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();
    }

    @Override
    public void setByzantine(Byzantine byzantineStatus, StreamObserver<Empty> streamObserver) {
        isByzantine.set(byzantineStatus.getIsByzantine());
        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();
    }

    @Override
    public void printDB(accId id, StreamObserver<Balance> streamObserver) {

//        for (Integer client : clients) {
//            double amount = clientBalances.get(client);
//            Balance balance = Balance.newBuilder().setAmount(amount).setClientId(client).build();
//            balancesBuilder.addBalances(balance);
//        }

        System.out.println(serverId);
        streamObserver.onNext(Balance.newBuilder().setAmount(clientBalances.get(id.getId())).build());
        streamObserver.onCompleted();
    }

    @Override
    public void printPerformance(Empty empty, StreamObserver<Performance> streamObserver) {
        int totalTransactionsProcessed = timeTakenForExecutingSequenceNumber.size();
        long latency = 0;
        for (Duration d : timeTakenForExecutingSequenceNumber.values()) {
            latency += d.toMillis();
        }
        streamObserver.onNext(Performance.newBuilder().setTotalTransactionsProcessed(totalTransactionsProcessed).setLatency(latency).setServerId(this.serverId).build());
        streamObserver.onCompleted();
    }

    @Override
    public void printStatus(SequenceNumber sequenceNumber, StreamObserver<StateAtSequenceNumber> streamObserver) {
        StateAtSequenceNumber stateAtSequenceNumber;

        if (logBasedOnSequenceNumber.containsKey(sequenceNumber.getSequenceNumber())) {
            Log.State state = logBasedOnSequenceNumber.get(sequenceNumber.getSequenceNumber()).state;
            stateAtSequenceNumber = StateAtSequenceNumber.newBuilder().setState(state + "").setServerId(this.serverId).build();

        } else {
            stateAtSequenceNumber = StateAtSequenceNumber.newBuilder().setState("No Status").setServerId(this.serverId).build();
        }
        streamObserver.onNext(stateAtSequenceNumber);
        streamObserver.onCompleted();
    }

    @Override
    public void printLog(Empty empty, StreamObserver<Logs> streamObserver) {
        Set<Integer> sequenceNumbers = logBasedOnSequenceNumber.keySet();
        Logs.Builder logs = Logs.newBuilder();

        for (int sequenceNumber : sequenceNumbers) {
            Log logFromMap = logBasedOnSequenceNumber.get(sequenceNumber);
            LogProto.Builder log = LogProto.newBuilder().setPrePrepare(logFromMap.prePrepare);
            if (logFromMap.prepare != null) {
                log.setPrepare(logFromMap.prepare);
            }
            if (logFromMap.commit != null) {
                log.setCommit(logFromMap.commit);
            }
            if (logFromMap.state != null) {
                log.setState(logFromMap.state + "");
            }
            if (logFromMap.crossShardStatus != null) {
                log.setCrossShardStatus(logFromMap.crossShardStatus + "");
            }

            log.addAllPrepareCertificate(logFromMap.prepareDeQueue);
            log.addAllCommitCertificate(logFromMap.commitDeQueue);
            log.setInProgress(logFromMap.inProgress);
            logs.addLogs(log);
        }
        streamObserver.onNext(logs.build());
        streamObserver.onCompleted();
    }

    @Override
    public void printView(Empty empty, StreamObserver<NewViewMessages> streamObserver) {
        Set<Integer> views = successfulNewViewMessages.keySet();

        NewViewMessages.Builder newViewMessagesBuilder = NewViewMessages.newBuilder();

        for (Integer view : views) {
            NewView nv = successfulNewViewMessages.get(view);
            int size = nv.getPrePreparesList().size();
            for (int i = 0; i < size; i++) {
                PrePrepare modifiedPrePrepare = nv.getPrePrepares(i).toBuilder().clearMessage().build();
                nv = nv.toBuilder().setPrePrepares(i, modifiedPrePrepare).build();
                int s = nv.getViewChangeMessagesList().size();
                for (int k = 0; k < s; i++) {
                    int l = nv.getViewChangeMessages(k).getPreparedRequestsList().size();
                    for (int j = 0; j < l; j++) {
                        PrePrepare p = nv.getViewChangeMessages(k).getPreparedRequests(j).getPrePrepare().toBuilder().clearMessage().build();
                        setP modifiedSetP = nv.getViewChangeMessages(k)
                                .getPreparedRequests(j)
                                .toBuilder()
                                .setPrePrepare(modifiedPrePrepare)  // Set modified PrePrepare
                                .build();

                        ViewChange vwMessage = nv.getViewChangeMessages(k).toBuilder().setPreparedRequests(j, modifiedSetP).build();
                        nv = nv.toBuilder().setViewChangeMessages(k, vwMessage).build();
                        successfulNewViewMessages.put(nv.getViewNo(), nv);
                    }
                }
            }

            newViewMessagesBuilder.addNewViewMessages(successfulNewViewMessages.get(view));
        }

        streamObserver.onNext(newViewMessagesBuilder.setServerId(this.serverId).build());
        streamObserver.onCompleted();

    }

    public void sendResult(Result result, StreamObserver<Empty> streamObserver) {

        streamObserver.onNext(Empty.newBuilder().build());
        streamObserver.onCompleted();
        PrePrepare prePrepare = result.getPrePrepare();

        Client c = prePrepare.getMessage();

        String clientKey = getClientKey(c);

        Transaction t = c.getT();

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

        CurrentStatus status = result.getStatus();

        System.out.println("Got---" + status);

        int seqNo = -1;

        boolean runningConsensusForTheFirstTimeParticipant = false;
        if (senderCluster == clusterId) {
            // coordinator will already have sequence number
            System.out.println(clientKey);
            resultOfTheSequenceNumber.put(clientKey, status);

            if (getCurrentLeader().equals(this.serverId)) {
                incrementSequenceNumber();
                seqNo = this.sequenceNumber.get();
                unExecutedRequests.put(seqNo, 1);
            }
        } else {
            // participant
            if (!clientRequests.containsKey(clientKey)) {
                runningConsensusForTheFirstTimeParticipant = true;
                // I need to run consensus for the first time, for this cross shard
                if (getCurrentLeader().equals(this.serverId)) {
                    incrementSequenceNumber();
                    seqNo = this.sequenceNumber.get();
                    unExecutedRequests.put(seqNo, 1);
                }
                clientRequests.put(clientKey, seqNo);
                resultOfTheSequenceNumber.put(clientKey, status);
            } else {
                if (getCurrentLeader().equals(this.serverId)) {
                    incrementSequenceNumber();
                    seqNo = this.sequenceNumber.get();
                    unExecutedRequests.put(seqNo, 1);
                }
                resultOfTheSequenceNumber.put(clientKey, status);
            }
        }

        // now we can run consensus, if leader

        if (getCurrentLeader().equals(this.serverId)) {

            String digestOfClientMessage = null;
            try {
                digestOfClientMessage = createDigestOfClientMessage(c);

                String key = UUID.randomUUID().toString();

                int viewNo = this.viewNo.get();

                boolean gotPrepareQuorum = sendPrePrepareToAll(viewNo, digestOfClientMessage, seqNo, c, key);

//                if(senderCluster != clusterId && runningConsensusForTheFirstTimeParticipant) {
//                    gotPrepareQuorum = false;
//                }

                int currentViewNoOfPrimary = this.viewNo.get(); // i am initialising the view no variable here because a transaction has to complete within a view, otherwise it will be part of the view change, and also while verifying the prepares we check if the prepares have the current view no of the server or not
                if (gotPrepareQuorum) {
                    if (isByzantine.get()) {
                        // it will still log the prepare
                        this.logBasedOnSequenceNumber.get(seqNo).prepareDeQueue.addAll(checkPrepares.get(key));
                        logBasedOnSequenceNumber.get(seqNo).inProgress = false;
                        return;
                    }
                    boolean gotCommitQorum = sendPreparesToAll(currentViewNoOfPrimary, seqNo, digestOfClientMessage, key);
                    this.checkPrepares.remove(key);

                    if (gotCommitQorum) {
                        sendCommitsToAll(currentViewNoOfPrimary, seqNo, key);
                        this.checkCommits.remove(key);
                        execute(seqNo);
                        if (senderCluster == clusterId) {
                            System.out.println("This is the balance of 1 " + clientBalances.get(1));
                            // coordinator
                            System.out.println("Final Datastore of coordinator cluster");
                            System.out.println(dataStore);
                            sendResultToAll(seqNo, status);
                        } else {
                            // participant
                            if (runningConsensusForTheFirstTimeParticipant) {
                                sendResultToAll(seqNo, CurrentStatus.COMMITTED);
                            } else {
                                System.out.println("This is the balance of 2001 " + clientBalances.get(2001));
                                System.out.println("Final Datastore of participant cluster");
                                System.out.println(dataStore);
                                // send ACK
                                sendAckToCoordinator(seqNo);
                            }
                        }
                    } else {
                        this.checkCommits.remove(key);
                        logBasedOnSequenceNumber.get(seqNo).inProgress = false;

//                        sendResultToAll(seqNo, CurrentStatus.ABORT);
                    }

                } else {
                    this.checkPrepares.remove(key);
                    logBasedOnSequenceNumber.get(seqNo).inProgress = false;
//                    sendResultToAll(seqNo, CurrentStatus.ABORT);
                }

            } catch (Exception e) {

            }
        }


    }

    @Override
    public void sendAck(Ack ack, StreamObserver<Empty> responseObserver) {
        Client c = ack.getC();
        String clientKey = getClientKey(c);

        if (coordinatorTimer.get(clientKey).isRunning()) {
            coordinatorTimer.get(clientKey).stop();
        }
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void sendAckToCoordinator(int seqNo) {
        Log log = logBasedOnSequenceNumber.get(seqNo);
        Transaction t = log.prePrepare.getMessage().getT();

        Ack ack = Ack.newBuilder().setC(log.prePrepare.getMessage()).setStatus(log.crossShardStatus).build();

        int sender = t.getSenderId(), senderCluster = getClusterIndex(sender);

        int start = 1;

        if (senderCluster == 1) {
            start = 1;
        } else if (senderCluster == 2) {
            start = 5;
        } else {
            start = 9;
        }

        stubs[start].sendAck(ack, new StreamObserver<Empty>() {
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


        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void coordinatorTimerExpired(Client c, int seqNo) {
        String clientKey = getClientKey(c);
        if (resultOfTheSequenceNumber.containsKey(clientKey)) {
            CurrentStatus status = resultOfTheSequenceNumber.get(clientKey);
            sendResultToAll(seqNo, status);
        } else {
            Result result = Result.newBuilder().setPrePrepare(logBasedOnSequenceNumber.get(seqNo).prePrepare).setStatus(CurrentStatus.ABORT).build();
            // it needs to run the consensus of the abort, it will also send abort to the participatting cluster
            runAbortConsensusForLeader(result);
        }
        // resetting the timer again
        coordinatorTimer.get(clientKey).reset();
    }

    public void runAbortConsensusForLeader(Result result) {

        PrePrepare prePrepare = result.getPrePrepare();

        Client c = prePrepare.getMessage();

        String clientKey = getClientKey(c);

        Transaction t = c.getT();

        int sender = t.getSenderId(), receiver = t.getReceiverId(), senderCluster = getClusterIndex(sender), receiverCluster = getClusterIndex(receiver);

        CurrentStatus status = result.getStatus();

        System.out.println("Got---" + status);

        int seqNo = -1;

        boolean runningConsensusForTheFirstTimeParticipant = false;
        if (senderCluster == clusterId) {
            // coordinator will already have sequence number
            System.out.println(clientKey);
            resultOfTheSequenceNumber.put(clientKey, status);

            if (getCurrentLeader().equals(this.serverId)) {
                incrementSequenceNumber();
                seqNo = this.sequenceNumber.get();
                unExecutedRequests.put(seqNo, 1);
            }
        } else {
            // participant
            if (!clientRequests.containsKey(clientKey)) {
                runningConsensusForTheFirstTimeParticipant = true;
                // I need to run consensus for the first time, for this cross shard
                if (getCurrentLeader().equals(this.serverId)) {
                    incrementSequenceNumber();
                    seqNo = this.sequenceNumber.get();
                    unExecutedRequests.put(seqNo, 1);
                }
                clientRequests.put(clientKey, seqNo);
                resultOfTheSequenceNumber.put(clientKey, status);
            } else {
                if (getCurrentLeader().equals(this.serverId)) {
                    incrementSequenceNumber();
                    seqNo = this.sequenceNumber.get();
                    unExecutedRequests.put(seqNo, 1);
                }
                resultOfTheSequenceNumber.put(clientKey, status);
            }
        }

        // now we can run consensus, if leader

        if (getCurrentLeader().equals(this.serverId)) {

            String digestOfClientMessage = null;
            try {
                digestOfClientMessage = createDigestOfClientMessage(c);

                String key = UUID.randomUUID().toString();

                int viewNo = this.viewNo.get();

                boolean gotPrepareQuorum = sendPrePrepareToAll(viewNo, digestOfClientMessage, seqNo, c, key);

//                if(senderCluster != clusterId && runningConsensusForTheFirstTimeParticipant) {
//                    gotPrepareQuorum = false;
//                }

                int currentViewNoOfPrimary = this.viewNo.get(); // i am initialising the view no variable here because a transaction has to complete within a view, otherwise it will be part of the view change, and also while verifying the prepares we check if the prepares have the current view no of the server or not
                if (gotPrepareQuorum) {
                    if (isByzantine.get()) {
                        // it will still log the prepare
                        this.logBasedOnSequenceNumber.get(seqNo).prepareDeQueue.addAll(checkPrepares.get(key));
                        logBasedOnSequenceNumber.get(seqNo).inProgress = false;
                        return;
                    }
                    boolean gotCommitQorum = sendPreparesToAll(currentViewNoOfPrimary, seqNo, digestOfClientMessage, key);
                    this.checkPrepares.remove(key);

                    if (gotCommitQorum) {
                        sendCommitsToAll(currentViewNoOfPrimary, seqNo, key);
                        this.checkCommits.remove(key);
                        execute(seqNo);
                        if (senderCluster == clusterId) {
                            System.out.println("This is the balance of 1 " + clientBalances.get(1));
                            // coordinator
                            System.out.println("Final Datastore of coordinator cluster");
                            System.out.println(dataStore);
                            sendResultToAll(seqNo, status);
                        } else {
                            // participant
                            if (runningConsensusForTheFirstTimeParticipant) {
                                sendResultToAll(seqNo, CurrentStatus.COMMITTED);
                            } else {
                                System.out.println("This is the balance of 2001 " + clientBalances.get(2001));
                                System.out.println("Final Datastore of participant cluster");
                                System.out.println(dataStore);
                                // send ACK
                                sendAckToCoordinator(seqNo);
                            }
                        }
                    } else {
                        this.checkCommits.remove(key);
                        logBasedOnSequenceNumber.get(seqNo).inProgress = false;

//                        sendResultToAll(seqNo, CurrentStatus.ABORT);
                    }

                } else {
                    this.checkPrepares.remove(key);
                    logBasedOnSequenceNumber.get(seqNo).inProgress = false;
//                    sendResultToAll(seqNo, CurrentStatus.ABORT);
                }

            } catch (Exception e) {

            }
        }
    }

    private void updateBalanceSender(int clientId, double amount) {
        String updateQuery = "UPDATE " + serverId + " SET Balance = ? WHERE ClientID = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {

            preparedStatement.setDouble(1, amount);
            preparedStatement.setInt(2, clientId);

            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected == 0) {
                throw new SQLException("No rows updated. Client ID " + clientId + " might not exist in table: " + serverId);
            } else {
//                System.out.println("Balance updated for Client ID: " + clientId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void rollbackWal(String clientKey) {
        String walTableName = serverId + "_WAL";
        String deleteQuery = "DELETE FROM " + walTableName + " WHERE clientKey = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)) {
            preparedStatement.setString(1, clientKey);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Rollback completed. Rows affected: " + rowsAffected);
        } catch (SQLException e) {
            System.err.println("Error during rollback: " + e.getMessage());
        }
    }

    private void updateWal(String clientKey, Transaction t,CurrentStatus status) {
        int sender = t.getSenderId(), receiver = t.getReceiverId();
        double amount = t.getAmount();

        String walTableName = serverId + "_WAL";
        String sql = "INSERT INTO " + walTableName +
                " (clientKey, sender, receiver, status, amount) " +
                "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, clientKey);
            stmt.setInt(2, sender);
            stmt.setInt(3, receiver);
            stmt.setString(4, status + "");
            stmt.setDouble(5, amount);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateBalanceReceiver(int clientId, double amount) {
        String updateQuery = "UPDATE " + serverId + " SET Balance = ? WHERE ClientID = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
            preparedStatement.setDouble(1, amount);
            preparedStatement.setInt(2, clientId);

            int rowsAffected = preparedStatement.executeUpdate();

            if (rowsAffected == 0) {
                throw new SQLException("No rows updated. Client ID " + clientId + " might not exist in table: " + serverId);
            } else {
//                System.out.println("Balance updated for Client ID: " + clientId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private double getBalance(int clientId) {

        String query = "SELECT Balance FROM " + serverId + " WHERE ClientID = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, clientId);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getDouble("Balance");
                } else {
                    throw new SQLException("Client ID not found in table: " + serverId);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void printBalance(accId id, StreamObserver<Balance> streamObserver) {
        streamObserver.onNext(Balance.newBuilder().setAmount(clientBalances.get(id.getId())).build());
        streamObserver.onCompleted();
    }
    @Override
    public void printDataStore(Empty empty, StreamObserver<DataStore> streamObserver) {
        DataStore.Builder dataStoreBuilder = DataStore.newBuilder();

        for(TransactionStatus t : dataStore) {
            dataStoreBuilder.addDataItems(DataItem.newBuilder().setT(t.c.getT()).setSequenceNumber(t.sequenceNumber).setStatus(t.state+"").build());
        }
        streamObserver.onNext(dataStoreBuilder.build());
        streamObserver.onCompleted();
    }

    private int getClusterIndex(int clientId) {
        if (clientId >= 1 && clientId <= 1000) return 1;

        if (clientId >= 1001 && clientId <= 2000) return 2;

        if (clientId >= 2001 && clientId <= 3000) return 3;

        return -1;
    }

}

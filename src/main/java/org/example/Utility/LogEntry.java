package org.example.Utility;

import org.ds.paxos.Transaction;

import java.util.HashSet;
import java.util.*;

public class LogEntry {
    public int index;
    public int term;
    public Transaction t;
    public int writeConcern = 2;
    boolean ackSent;
    public HybridClock.TimeStamp timeStamp;

    public List<Boolean> serversThatReplicatedThisEntry;

    public int copyOfWriteConcern;


    // now if this timestamp is used at the follower end, there will some miscalculations for follower (this can only happen when leader fails and is not able to replicate to majority of servers, and on of the servers which have this entry becomes the leader). These little bit of miscalculations are fine as we only use them to adjust the writeConcern costs (this will ideally have the same effect for all)
    public Long timeOfArrivalAtLeader;

    // this constructor is for the leader
    public LogEntry(int index, int term, Transaction t, int writeConcern, HybridClock.TimeStamp timeStamp, int NUM_OF_SERVERS, long timeOfArrivalAtLeader) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
        this.ackSent = false;
        this.timeStamp = timeStamp;
        this.serversThatReplicatedThisEntry = new ArrayList<>();
        // it is expected that the server does not deduct the write concern before adding it in log
        // this copy is used at the time of ack to compute the throughput for individual writeConcerns
        this.copyOfWriteConcern = writeConcern;
        this.timeOfArrivalAtLeader = timeOfArrivalAtLeader;

        for(int i = 0 ; i < NUM_OF_SERVERS; i ++) {
            // false means that this entry was not replicated on this server
            serversThatReplicatedThisEntry.add(false);
        }
    }
    // this constructor is for the follower to use
    public LogEntry(int index, int term, Transaction t, int writeConcern, HybridClock.TimeStamp timeStamp, List<Boolean> serversThatReplicatedThisEntry, int copyOfWriteConcern) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
        this.ackSent = false;
        this.timeStamp = timeStamp;
        this.serversThatReplicatedThisEntry = serversThatReplicatedThisEntry;
        this.copyOfWriteConcern = copyOfWriteConcern;


//        for(int i = 0 ; i < 5; i ++) {
//            serversThatReplicatedThisEntry.set(i, serversThatReplicatedThisEntry.get(i));
//            // false means that this entry was not replicated on this server
//        }
    }


    public LogEntry(int index, int term, Transaction t) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = 2;
        this.ackSent = false;
    }
    @Override
    public String toString() {
        return "The Transaction is: " + t + " The time stamp is" + timeStamp + "The servers that replicated the entry are --" +serversThatReplicatedThisEntry.toString();
    }
}

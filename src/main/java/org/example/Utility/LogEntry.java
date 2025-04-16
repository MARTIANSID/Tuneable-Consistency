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


    public LogEntry(int index, int term, Transaction t, int writeConcern, HybridClock.TimeStamp timeStamp, int NUM_OF_SERVERS) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
        this.ackSent = false;
        this.timeStamp = timeStamp;
        this.serversThatReplicatedThisEntry = new ArrayList<>();

        for(int i = 0 ; i < NUM_OF_SERVERS; i ++) {
            // false means that this entry was not replicated on this server
            serversThatReplicatedThisEntry.add(false);
        }
    }

    public LogEntry(int index, int term, Transaction t, int writeConcern, HybridClock.TimeStamp timeStamp, List<Boolean> serversThatReplicatedThisEntry) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
        this.ackSent = false;
        this.timeStamp = timeStamp;
        this.serversThatReplicatedThisEntry = serversThatReplicatedThisEntry;

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

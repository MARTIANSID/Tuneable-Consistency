package org.example.Utility;

import org.ds.paxos.Transaction;

public class LogEntry {
    public int index;
    public int term;
    public Transaction t;
    public int writeConcern = 2;
    boolean ackSent;
   public HybridClock.TimeStamp timeStamp;

    public LogEntry(int index, int term, Transaction t, int writeConcern, HybridClock.TimeStamp timeStamp) {
        this.index = index;
        this.term = term;
        this.t = t;
        this.writeConcern = writeConcern;
        this.ackSent = false;
        this.timeStamp = timeStamp;
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
        return "The Transaction is: " + t;
    }
}

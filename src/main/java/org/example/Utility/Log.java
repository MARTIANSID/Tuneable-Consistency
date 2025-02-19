package org.example.Utility;

import org.ds.paxos.Transaction;

public class Log {
    public int index;
    public int term;
    public Transaction t;

    public Log(int index, int term, Transaction t) {
        this.index = index;
        this.term = term;
        this.t = t;
    }
}

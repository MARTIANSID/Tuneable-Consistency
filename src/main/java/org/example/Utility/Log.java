package org.example.Utility;

import org.ds.paxos.Transaction;

public class Log {
    int index;
    int term;
    Transaction t;

    Log(int index, int term, Transaction t) {
        this.index = index;
        this.term = term;
        this.t = t;
    }
}

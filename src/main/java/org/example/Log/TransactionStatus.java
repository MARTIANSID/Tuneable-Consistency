package org.example.Log;

import org.ds.paxos.Client;
//import org.ds.paxos.State;
import org.ds.paxos.Transaction;

public class TransactionStatus {
    public enum State {
        PREPARE, COMMITTED, ABORT
    }
    public State state;
   public int sequenceNumber;

    public Client c;

    public TransactionStatus(int sequenceNumber, Client c, State state) {
       this.sequenceNumber = sequenceNumber;
       this.c = c;
       this.state = state;
    }
    @Override
    public String toString(){
        return "This is the sequence number " + sequenceNumber + " This is the client message " + c + " This is the State " + state;
    }
}

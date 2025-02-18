package org.example.Log;

import org.ds.paxos.*;

import java.time.LocalTime;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Log {
    public enum State {
        PREPREPARE, PREPARE, COMMIT, EXECUTE
    }

    public State state;
    public int viewNo;
    public int sequenceNumber;
    public PrePrepare prePrepare;

    public ConcurrentLinkedDeque<Prepare> prepareDeQueue;

   public Prepare prepare;

    public ConcurrentLinkedDeque<Commit> commitDeQueue;

    public Commit commit;

    public boolean nullRequest = false;

    public boolean inProgress = false;
    public LocalTime startTime;


    public CurrentStatus crossShardStatus;

    public Log() {
        this.prepareDeQueue = new ConcurrentLinkedDeque<>();
        this.commitDeQueue = new ConcurrentLinkedDeque<>();
        this.nullRequest = false;
        this.startTime = LocalTime.now();
    }

    @Override
    public String toString(){
        return  "This is the PrePrepare: " + prePrepare + "\nThis is the Prepare" + prepare +"\n This is the commit" + commit + "\nThese is the prepareCertificates: " + prepareDeQueue+ "\nThese are the commit certificates" + commitDeQueue + "The state is" + state;
    }

}

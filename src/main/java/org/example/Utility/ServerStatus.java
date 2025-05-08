package org.example.Utility;

import org.ds.paxos.ClientMessage;

public class ServerStatus {
    public enum ServerCurrentStatus {
        FOLLOWER,
        LEADER,
        CANDIDATE
    }
}



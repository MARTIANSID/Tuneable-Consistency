package org.example.Utility;

import org.example.raft.ClientMessage;

public class ServerStatus {
    public enum ServerCurrentStatus {
        FOLLOWER,
        LEADER,
        CANDIDATE
    }
}



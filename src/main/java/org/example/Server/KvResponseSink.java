package org.example.Server;

import org.example.raft.KvResponse;

/** One admitted framed request and its exactly-once terminal response. */
interface KvResponseSink {

    boolean isFinished();

    double remainingDeadlineMs();

    double ingressElapsedMs();

    void respond(KvResponse.Builder response);
}

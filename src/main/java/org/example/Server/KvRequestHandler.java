package org.example.Server;

import org.example.raft.KvRequest;

/** Execution boundary used by the framed ingress after transport admission. */
@FunctionalInterface
interface KvRequestHandler {
    void execute(KvRequest request, long receivedNanos, KvResponseSink response);
}

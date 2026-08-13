package org.example.Server;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntUnaryOperator;

/**
 * Leader-side waiters for intermediate write concerns: a write acknowledged at
 * wc:k completes once its entry is replicated on at least k nodes (leader
 * included). Evaluated whenever a follower's match index advances; the leader
 * supplies the replica count function since it owns the matchIndex array.
 * wc:1 never registers here (acknowledged at append) and wc:majority uses the
 * commit-index registry instead.
 */
final class ReplicationWaitRegistry {

    private static final class Waiter {
        final int entryIndex;
        final int requiredReplicas;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        Waiter(int entryIndex, int requiredReplicas) {
            this.entryIndex = entryIndex;
            this.requiredReplicas = requiredReplicas;
        }
    }

    private final ConcurrentLinkedQueue<Waiter> waiters = new ConcurrentLinkedQueue<>();

    CompletableFuture<Void> await(int entryIndex, int requiredReplicas, IntUnaryOperator replicaCountOf) {
        if (replicaCountOf.applyAsInt(entryIndex) >= requiredReplicas) {
            return CompletableFuture.completedFuture(null);
        }
        Waiter waiter = new Waiter(entryIndex, requiredReplicas);
        waiters.add(waiter);
        // Re-check after registration to close the race with a concurrent
        // evaluate(); completing twice is harmless.
        if (replicaCountOf.applyAsInt(entryIndex) >= requiredReplicas) {
            waiters.remove(waiter);
            waiter.future.complete(null);
        }
        return waiter.future;
    }

    /** Complete every waiter whose replication requirement is now met. */
    void evaluate(IntUnaryOperator replicaCountOf) {
        if (waiters.isEmpty()) {
            return;
        }
        Iterator<Waiter> it = waiters.iterator();
        while (it.hasNext()) {
            Waiter waiter = it.next();
            if (replicaCountOf.applyAsInt(waiter.entryIndex) >= waiter.requiredReplicas) {
                it.remove();
                waiter.future.complete(null);
            }
        }
    }

    /** Fail everything (leadership lost); waiters can never be satisfied here. */
    void failAll(Exception reason) {
        Waiter waiter;
        while ((waiter = waiters.poll()) != null) {
            waiter.future.completeExceptionally(reason);
        }
    }
}

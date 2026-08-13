package org.example.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Waiters keyed by a monotonically advancing log position (local log index or
 * commit index). await(i) completes once signal(j) has been called with
 * j >= i. Completion happens on the signaling thread, which may hold the
 * consensus lock; callers must chain their reply work with *Async variants.
 */
final class IndexWaitRegistry {

    private final ConcurrentSkipListMap<Integer, List<CompletableFuture<Void>>> waiters = new ConcurrentSkipListMap<>();
    private final AtomicInteger reached = new AtomicInteger(-1);

    CompletableFuture<Void> await(int index) {
        if (reached.get() >= index) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        waiters.computeIfAbsent(index, k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(future);
        // Re-check: signal may have swept past between the first check and
        // registration; completing twice is harmless.
        if (reached.get() >= index) {
            future.complete(null);
        }
        return future;
    }

    void signal(int reachedIndex) {
        int previous;
        do {
            previous = reached.get();
            if (reachedIndex <= previous) {
                return;
            }
        } while (!reached.compareAndSet(previous, reachedIndex));

        Map<Integer, List<CompletableFuture<Void>>> ready = waiters.headMap(reachedIndex, true);
        for (Integer key : ready.keySet()) {
            List<CompletableFuture<Void>> futures = waiters.remove(key);
            if (futures != null) {
                synchronized (futures) {
                    for (CompletableFuture<Void> future : futures) {
                        future.complete(null);
                    }
                }
            }
        }
    }

    int reachedIndex() {
        return reached.get();
    }
}

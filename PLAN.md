# Chameleon redesign

## What to build

A replicated key-value store on Raft with single-key reads and writes. Clients do not choose a consistency level per request. Each application registers SLAs, and the server decides what level to actually deliver, upgrading above the application's floor when the value of doing so exceeds the capacity it consumes. All decisions are server side.

There are no operating modes. Nothing switches between normal and pressure behavior. There is no global latency threshold and no batch-average latency check. Degradation under load is produced by a price that rises, not by a mode that flips.

## Consistency levels

MongoDB style.

Reads: eventual-local, eventual-majority, causal-local, causal-majority, linearizable.

Local means the replica's log, which may contain uncommitted entries that a leader change can roll back. Majority means committed state. That distinction is real and must be stated in any writeup, because a reader assuming stock Raft will assume followers only apply committed entries.

Causal-local waits until the local log index reaches the client's uncommitted session index. Causal-majority waits until the commit index reaches the client's committed session index. Linearizable is leader-only.

Implement linearizable with ReadIndex, not with a no-op append and not as an entry. The leader records its commit index, confirms leadership via a heartbeat round with a majority, waits for its applied index to reach the recorded value, then serves locally. No log entry is created, and reads batch onto whatever AppendEntries round is already in flight. Consider follower linearizable reads (follower obtains the read index from the leader, waits for its own apply to catch up) as a way to relieve leader concentration.

Writes: wc:1 through wc:majority.

## SLA format

An application registers one or more SLAs. An SLA can be either for read or write and is a set of rungs, each a triple (consistency requirement, end-to-end latency threshold in milliseconds, profit). Rungs are not ordered by anything; the structure below handles any shape. A request will have an application ID and which SLA applies. This is what lets one application have a cheap path and an expensive path.

## Protocol

Request carries: application id, SLA id, operation (read or write, key, value), the client's committed session index, the client's uncommitted session index, and the client's current RTT estimate to this server. It does not carry a consistency level, a deadline, or any profit values.

Send the RTT rather than a pre-subtracted budget, because the SLA has several thresholds and the server subtracts from each.

Send both committed and uncommitted session indices, not one. Causal-local checks against the uncommitted index; causal-majority checks against the committed index. Folding them together means a wc:1 acknowledgment at an uncommitted index makes every later causal-majority read wait for an index that may never commit.

Response carries: the value or acknowledgment, the level actually delivered, which rung was satisfied, the node's current log index, the node's current commit index, and the server-side service time.

The service time is required. Clients estimate RTT by subtracting it from observed end-to-end latency, using replies that involved no waiting. Without it the RTT estimate absorbs server-side work and inflates under load, which is exactly backwards.

## Server state

Per node: Raft log index and commit index; a histogram of service times per (level, gap bucket); an occupancy accumulator; the shadow price lambda; the registered SLA tables.

Leader only: the replication rate bucket.

There is no pool and no leader-side control state beyond that bucket. Leaders and followers run the identical algorithm and differ only in which levels are legal and which resources they hold.

## Resource model

Two resources, priced separately.

Replication is a rate. Every write costs one log entry. Reads cost zero, including linearizable reads, since ReadIndex appends nothing. Budget it at the measured maximum commit rate in entries per second, refill each second, charge at admission, never return. This lives at the leader only, since all entries originate there, and in practice it gates writes only.

Occupancy is slot-time. Every request holds a slot from admission to reply, including time spent waiting on an index, a confirmation round, or replication acknowledgments. The cost of a level is its expected service time in milliseconds. Budget S_max at the concurrency where latency starts bending upward on a load sweep, not at a hard resource ceiling, so that utilization can exceed 1 and give the controller signal. Keep a genuine hard cap at roughly 1.5 times S_max as an immediate backstop. Occupancy is per node; followers have this and not the replication bucket.

## Request path

### Step 1: timestamp

Take `t_recv` the moment the request comes off the socket, before parsing and before it enters any queue. Use a monotonic clock. Everything downstream measures from here, and taking it later attributes queueing delay to the network, which makes clients conclude the network slowed down every time the server got busy.

### Step 2: determine the legal level set

Filter the ladder by this node's role and the operation type. In other words, followers can't take write requests. If the request is illegal here, redirect rather than reject.

Look up the SLA by (application id, SLA id). Reads use a read SLA, writes use a write SLA.

### Step 3: compute the index gap for every legal level

For each legal level `c`, compute the gap it must close before it can be served:

- eventual-local, eventual-majority: gap is zero, both values are already local
- causal-local: `gap = client_uncommitted_index - local_log_index`
- causal-majority: `gap = client_committed_index - commit_index`
- linearizable and write concerns: no gap dimension, they wait on rounds rather than indices

Bucket the gap coarsely based on the maximum batch size, for example into buckets for `gap <= 0`, `1..2000`, `2001..20000`, `> 20000`. Everything at or below zero lands in the same bucket, whose observed waits are near zero, so the case where an upgrade is free emerges from measured data rather than from a special rule.

The histogram cell `H[c][gap_bucket]` is now the price of level `c` for this request. Its CDF gives expected profit in step 4, its running mean gives occupancy cost.

### Step 4: score every legal level

Do this per level, independently. Symbols: the SLA is a set of rungs `r_j = (kappa_j, delta_j, pi_j)`, meaning consistency requirement, end-to-end latency threshold in milliseconds, and profit. `rho` is the client's RTT estimate from the request. `F_c` is the CDF of `H[c][gap_bucket]`, so `F_c(x)` is the fraction of recent executions of level `c` at this gap that finished within `x` milliseconds. `omega_c` is that cell's running mean, the occupancy cost in milliseconds. `lambda` is the current shadow price in profit per millisecond, initialized to zero. When a cell has no samples, treat the level as free and certain: omega_c = 0 and F_c(x) = 1 everywhere. Cap the number of requests concurrently riding an uncalibrated cell at a small number, since samples arrive only on completion.

**4a. Restrict to satisfiable rungs.** `R(c) = { j : c is at least as strong as kappa_j }`. A level satisfies its own rungs and every weaker one, which is why stronger levels can earn more.

**4b. Convert thresholds to the server's clock.** For each `j` in `R(c)`, set `d_j = delta_j - rho`. Discard any rung with `d_j <= 0`; the network alone already consumed that budget.

**4c. Sort and deduplicate.** Sort survivors by threshold ascending to get `d_(1) <= d_(2) <= ... <= d_(k)` with profits `pi_(1) ... pi_(k)`. Set `d_(0) = 0`.

**4d. Suffix maximum.** `M_i = max{ pi_(i), ..., pi_(k) }`, with `M_(k+1) = 0`.

Use the maximum rather than `pi_(i)` because beating a tight threshold means beating every looser one, so the request claims the best-paying rung among all it satisfied. This also keeps the formula correct for SLAs where a looser rung happens to pay more. Collapse identical thresholds to one entry holding the suffix max; duplicates otherwise produce empty intervals that contribute nothing.

**4e. Expected profit.**

```
E_c = sum over i of  M_i * ( F_c(d_(i)) - F_c(d_(i-1)) )
```

Implement the equivalent by-parts form, which needs only `k` CDF lookups and no differencing:

```
E_c = sum over i of  ( M_i - M_(i+1) ) * F_c(d_(i))
```

Read `F_c` from the cached prefix-sum array, interpolating linearly within the bucket that contains `d_(i)`.

**4f. Score.**

```
V_c = E_c - lambda * omega_c
```

`E_c` is expected profit, `omega_c` is milliseconds of slot time, `lambda` converts one into the other, so `V_c` is net profit after paying for the capacity the level consumes.

**4g. Choose.** `c* = argmax over legal c of V_c`.

Worked example, for testing. SLA rungs (linearizable, 150, 10), (causal-majority, 150, 6), (causal-majority, 400, 4), (eventual, 400, 1), (eventual, 150, 5), with `rho = 20`, so server-side thresholds are 130 and 380. After 4a through 4d: eventual collapses to 130 to 5 and 380 to 1; causal-majority to 130 to 6 and 380 to 4; linearizable to 130 to 10 and 380 to 4. With `F_eventual(130) = 0.99, F(380) = 1.00, omega = 2`; `F_cm(130) = 0.90, F(380) = 0.99, omega = 40`; `F_lin(130) = 0.70, F(380) = 0.97, omega = 80`, the expected profits are 4.96, 5.76, and 8.08. At `lambda = 0.02` the scores are 4.92, 4.96, and 6.48, so linearizable wins. At `lambda = 0.08` they are 4.80, 2.56, and 1.68, so eventual wins. Nothing switched modes; the price moved.

### Step 5: admit or reject, and charge

Reject if `max over c of V_c <= 0`. Nothing is worth what it would consume at the current price. This is the admission control: under light load `lambda` is near zero and nothing is refused, under overload `lambda` rises and the least valuable requests are shed first, in value order rather than arrival order.

Reject also on the two hard backstops, which exist to cover model error and react within a single request rather than within a control interval: no free occupancy slot against the hard cap, or an empty replication bucket for a write.

On admission, decrement the replication bucket if the request is a write, and open an occupancy slot by updating the accumulator:

```c
// per worker thread; sum A across workers at the interval boundary
long A     = 0;        // accumulated request-microseconds
int  n     = 0;        // requests currently in flight
long tLast = now();

void on_event(int delta) {      // +1 on admit, -1 on reply
    long t = now();
    A += (long)n * (t - tLast); // close out the stretch that just ended
    n += delta;
    tLast = t;
}
```

Call `on_event(+1)` here. No sampling is involved: the code that changes `n` is the code that records how long the previous value lasted.

### Step 6: execute

Wait for whatever the chosen level requires, then read or apply. Eventual levels wait for nothing. Causal levels wait for the relevant index to advance. Linearizable waits for a leadership confirmation round, and on a follower additionally for its own applied index to reach the read index. Write concerns wait for the required number of acknowledgments.

These are execution mechanics, not decision points. The decision was made in step 4 and is not revisited.

Bound the wait by the loosest server-side threshold computed in step 4b. If it expires, abandon the wait, fall back to the strongest level that needs no waiting, and serve immediately. A request that waits past every threshold will deliver zero profit whatever it does, so continuing to hold a slot for it is pure loss.

### Step 7: grade after the fact

Determine what was actually delivered, not what was targeted. Reads frequently satisfy a stronger level than the one chosen, and that profit is free.

For a read, take the returned value's index. If it is at or below the commit index, the majority rungs are satisfied as well as the local ones. If the replica turned out to be at or ahead of the client's session index, causal is satisfied even if eventual was executed.

Realized profit is the highest-profit rung whose consistency requirement the delivered result meets and whose threshold the total, service time plus `rho`, came in under.

Writes cannot be graded this way. Once acknowledged at wc:1 the decision is spent.

### Step 8: reply, release, record

Reply with the fields listed in the protocol section, including the measured service time `T = now() - t_recv`.

Call `on_event(-1)` to close the occupancy slot.

File the sample: in cell `H[c*][gap_bucket]`, where `c*` is the level actually executed and `gap_bucket` is the one it ran under, increment the bucket containing `T`, and add `T` to that cell's running sum and 1 to its running count. Three operations, no rebuild. Compute `omega` from the running sum over the running count, never from bucket midpoints.

Do not pre-aggregate samples per interval before filing them. Profit depends on which latency band the request lands in, so the distribution is the thing being used, and averaging first destroys it.

If the request abandoned its wait in step 6, file the sample into the cell of the level originally chosen, at the timeout value d_max, and file nothing under the fallback level, whose measured time is dominated by the abandoned wait and says nothing about what it costs.

## Background tasks

### Histogram refresh

Histogram cells use fixed geometric bucket edges, for example 64 buckets from 0.5 ms with ratio 1.15, so the bucket index for `x` is `floor(log(x/0.5)/log(1.15))` and no search is needed.

On each tick, multiply every bucket count, running sum, and running count by a decay factor around 0.95, then rebuild the prefix-sum array and publish it. Old observations fade geometrically, giving an effective memory of roughly `interval / (1 - decay)`, so 100 ms and 0.95 gives about 2000 ms; tune the decay for the memory you want.

Requests read the published snapshot and never block on the refresh. The snapshot is up to one tick stale, which is far below the estimate's own variance. With around 24 cells at 64 buckets a full sweep is about 1500 operations.

### Price controller, once per control interval

At the boundary, close out the final stretch of each worker's accumulator, sum them, and compute utilization:

```c
// tEnd is the interval boundary, T its length
A += (long)n * (tEnd - tLast);
tLast = tEnd;
double u = (double)A_summed_across_workers / ((double)S_max * T);
A = 0;
```

`u` is the time-weighted average number of requests in flight divided by `S_max`. Time weighting matters: a count of 10 held for 90 ms and 50 held for 10 ms averages to 14, not 30, and a boundary sample would have read 50.

Then update the price:

```
lambda <- max( lambda_min, lambda * exp( eta * ( u - u_target ) ) )
```

with `u_target` around 0.85, `eta` around 1, and `lambda_min` a small positive constant; 0.0001 works for rung profits on the order of 1 to 100. The floor does two jobs: it lets `lambda` start at zero, since the multiplicative update on its own would leave zero unchanged forever, and it stops `lambda` decaying so far during idle periods that recovery takes many intervals.

Multiplicative rather than additive because the right price spans orders of magnitude and because it can never go negative. A controller rather than a closed form because profit units are whatever the applications say they are, so a closed form would need a scale constant that is actually accurate. `lambda_min` is the one constant that remains, and it only has to be the right order of magnitude, since the controller finds the operating price from there.

Requests never read `u`. They read `lambda`, which is up to one interval stale by design, since a price recomputed per request would jitter and turn the ranking into noise. The hard occupancy cap in step 5 covers the fast timescale.

Compute `u` and `lambda` per node. Occupancy is local.

As a cross-check during development, the sum of service times of requests completing in the interval, divided by `S_max * T`, approximates the same quantity, with boundary bias when service times are comparable to the interval length.

## Write semantics

In Raft every accepted write takes the same replication path, so wc:1 and wc:majority are the same write imposing the same load. Only the moment of acknowledgment differs. A write upgrade therefore costs zero replication and pure occupancy, which is why a flat per-write cost would make write upgrades unconditional.

Holding the acknowledgment until majority commit closes the loss window, costs latency, and is gated by the SLA thresholds exactly like a read upgrade. That is the write upgrade.

An optional lower rung is acknowledging at wc:1 and letting the client learn later that the entry committed, from the commit index piggybacked on subsequent replies. This does not close the loss window, it only reports the outcome afterwards, so it is worth less and is semantically different. Note that a commit index alone confirms success but cannot detect failure, since a leader change can replace the entry at that index: detecting loss requires the acknowledgment to carry term and index together, and a check that the committed entry at that index still has the expected term. If that turns out to need per-session server state, drop this rung.

## Instrumentation to build in

Predicted versus realized profit per request, since profit now depends on a latency prediction and a misprediction is an accounting error, not just a missed deadline.

Fraction of upgrades that were free (gap at or below zero) versus waiting, swept against write load and key skew.

Per-level tail latency, not averages.

Which rung was satisfied, per request. This is the metric that makes a head-to-head against Pileus direct, since both systems consume the same declarative input.

## Implementation plan

### Ground rules

Transform the existing implementation in place rather than writing a parallel system. The Raft substrate in `ServerImpl` (election, AppendEntries replication, commit tracking, log rollback), the `Servers` startup and in-process cluster, `Raft.proto`, the strict `ExperimentConfig` loader, and the `run_all.sh` tmux harness all survive and are modified. The old decision machinery (batch upgrade auction, token bucket, pressure mode, callback-batched acks) is deleted, but only at the stage whose change actually orphans it, never preemptively. The pre-redesign system remains available on `main` for comparison runs.

Every stage must leave the tree in a working state: `mvn clean install -Dbuild.dir=target-script` green including tests, and `./run_all.sh <label>` completing a full run. Every stage lands unit or integration tests for what it adds; this codebase currently has none and the redesign is the moment that changes.

Topology stays as it is today: N server nodes in one JVM talking real gRPC to each other on localhost, clients in the same JVM talking to servers over real gRPC (streaming, per the protocol decision). SLAs come from the config file. The config loader keeps rejecting unknown keys; the schema evolves with each stage.

### What survives, what transforms, what dies

Survives with modification: `ServerImpl` (consensus core), `Servers` (startup, lifecycle, phase runner), `RaftLog`/`LogEntry`, `Raft.proto` (extended in place, `org.ds.paxos` package stays), `ExperimentConfig`, `run_all.sh`, `WorkloadSimulator` (becomes the SLA workload), `ClientMetricsTracker` (becomes the client instrumentation ledger), `LatencyAwareRouter` (its sliding-window base-latency machinery becomes the client RTT estimator; the profit-scoring half dies since the server now decides levels).

Dies, at the stage that orphans it: `BatchProcessor` and the upgrade auction, `TokenBucketImpl` and the Redis dependency (jedis, run_all.sh Redis handling), pressure mode, the independent admission-control flag and its fairness counters, the `SendTransaction` + callback-ack request path, `TransactionInjector`, `HybridClock` timestamp-based causality (replaced by session indices), per-transaction profit fields in the proto (replaced by SLA tables), the bank-account data model (`sender`/`receiver`/`amount`/balances, replaced by key-value).

### Stage 1: KV state machine and ReadIndex on the existing Raft

Purely additive to the server; the old request path keeps running so this stage is verifiable against today's behavior.

- Extend `Transaction` in `Raft.proto` with `key`/`value` fields; switch `WorkloadSimulator` to generate single-key KV writes. The bank fields stay until stage 2 removes them.
- Add a `KvStore` with two views: local (applied at log append, may roll back) and committed (applied when the commit index advances), each entry carrying the index that last modified it (step 7 grading needs this). On log truncation (`rollbackTillIndex`), rebuild the local view from the committed view plus the surviving uncommitted suffix. `updateBalances` and the balance maps are replaced by this.
- Implement `confirmLeadership()` on the leader: ReadIndex as specified, snapshot term and commit index, confirm via a majority acknowledgment round riding the existing `sendAppendEntries`, complete when applied index reaches the snapshot. No log entry.
- Tests: KvStore local-vs-committed semantics and truncation rebuild; a real 3-node cluster test (replicate, verify identical committed state); leader-crash failover preserving committed state; ReadIndex returning a covering index.

Exit: tests green; `run_all.sh` completes with the KV payload flowing through the unchanged batch path.

**Status: done (2026-08-12).** The generated proto package was also renamed from `org.ds.paxos` to `org.example.raft` (proto package `raft`). `KvStore` lives in `org.example.Server` and replaced the balance maps at all four apply points (leader append, leader commit, follower append, follower commit); the truncation rebuild throws if a truncation would ever reach committed entries instead of silently rolling them back. `confirmLeadership()` rides the existing 30 ms replication rounds: each pending confirmation counts follower responses to rounds whose send snapshot (nanoTime) is at or after registration, completes with the snapshotted commit index at majority, fails on step-down, and times out at 1 s; no apply-wait is needed because the committed KV view is applied in the same critical sections that advance the commit index. The workload generator in `Servers` now emits single-key KV ops over a 100-key space (the unused standalone `Client/WorkloadSimulator` main still builds bank ops and dies in stage 2). `ServerImpl.shutdown()` was added for orderly teardown, and surefire's working directory moved to the build directory so test-run CSVs stay out of the repo root. Verification: 7 tests green (KvStore semantics and truncation rebuild; 3-node replication with identical committed state; leader-crash failover preserving pre-crash state; ReadIndex covering index on the leader and NotLeaderException on a follower, all without Redis since admission is off in tests); full `run_all.sh` run exited 0 with mean TPS 28,862, matching the pre-change baseline.

### Stage 2: per-request protocol and level mechanics

This is the structural break: the request path becomes per-request and synchronous-response, and everything that only served the old path is deleted here.

- Add a client-facing streaming RPC to `Raft.proto` carrying exactly the protocol section's request fields (app id, SLA id, op, committed and uncommitted session indices, RTT estimate) and response fields (value/ack, delivered level, satisfied rung, log index, commit index, service time). Responses return on the stream; the callback-ack path (`sendAckToClient`, `ClientServerImpl` as ack receiver, batched `AckMessage`) dies.
- Take `t_recv` on a monotonic clock at message receipt, before any queueing.
- Implement all levels as forced mechanics with the level chosen by a temporary config knob (forced level or SLA floor), no scoring yet: eventual-local and eventual-majority (read the respective view), causal-local (wait for local log index to reach the uncommitted session index), causal-majority (wait for commit index to reach the committed session index), linearizable via stage 1's ReadIndex (leader only, redirect otherwise), writes acknowledged at wc:1 through wc:majority. Waits are bounded with a fallback to the strongest no-wait level, per step 6.
- Clients track both session indices from response log/commit indices and assert session guarantees (read-your-writes at the causal levels) under load.
- Deletion sweep of what this orphans: `BatchProcessor`, the upgrade auction, `TokenBucketImpl` plus Redis (jedis from pom, Redis block from run_all.sh), pressure mode, the admission-control flag and fairness counters, `TransactionInjector`, `HybridClock` causality, the bank-transaction fields and profit fields from the proto, the mode/pressure/tokenBucket config sections.
- Tests: each level's wait mechanics against a controlled cluster (e.g. causal-majority blocks until commit catches up); session assertions; wait-timeout fallback.

Exit: forced-level end-to-end run with direct responses at the target moderate TPS; session assertions clean; Redis nowhere in the tree.

**Status: done (2026-08-13).** The client protocol is a bidirectional gRPC stream (KvClient.Session) with the forced level carried per request rather than a server-side config knob, so one run exercises the full phase mix; the field dies in stage 4 with the scorer. All five read levels and wc:1/wc:majority run as mechanics in `KvClientService`, with waits served by two index registries (local log, commit) plus a leader-side replication-count registry on `ServerImpl`, bounded by `chameleon.maxWaitMs` with fallback to the strongest no-wait level of the same view. Deviations from the plan text: `LatencyAwareRouter` could not survive as a file (its config section and callers died), so its sliding-window estimator was extracted to `Client/SlidingWindow` and put to work immediately - the client already estimates per-node RTT from no-wait replies and sends it on every request, pulling that piece of stage 5 forward; slf4j became an explicit dependency (it had been riding in through jedis). The first full run caught two real races via the session assertions (2 violations in 9.6M requests): the server published log/commit indices before applying entries to the KV views, letting the lock-free fast path read a stale view, and the client folded the session anchor and per-key assertion floor from unordered ack updates; both fixed by ordering state-before-index on the server and snapshot-floor-into-anchor on the client. Verification: 11 tests green (Raft core, KvStore, per-level mechanics including wait-timeout fallback and follower redirect, session guarantees under load through the real client); full run of 9.62M requests with every request answered, zero lost, zero session violations, Heavy phase holding ~70k TPS with 70% linearizable reads (linearizable avg 54.6 ms, wc:2 28.5 ms, wc:1 6.5 ms).

### Stage 3: measurement plane

- Service-time histograms per (level, gap bucket): fixed geometric bucket edges (64 buckets from 0.5 ms, ratio 1.15, index by formula), per-cell running sum and count, decay tick (multiply by ~0.95, rebuild prefix sums, publish an immutable snapshot; requests only read snapshots).
- Gap computation per legal level (step 3) and coarse gap bucketing tied to the maximum batch size.
- Occupancy accumulator exactly as the step 5 pseudocode: `on_event(+1)` at admission, `on_event(-1)` at reply, per worker, summed at interval boundaries; utilization computed but not yet acted on.
- Sample filing discipline of step 8, including the abandoned-wait rule (file at the timeout under the chosen level, nothing under the fallback).
- Tests: bucket-index formula edges; decay and prefix-sum correctness; the accumulator against a scripted event sequence with known time-weighted average; cross-check `u` against the sum-of-completed-service-times approximation in an integration run.

Exit: a run produces populated cells and a per-interval utilization series that matches the cross-check within boundary bias.

**Status: done (2026-08-13).** `ServiceTimeHistograms` (geometric buckets per the spec, DoubleAdder hot path folded into decayed state by a single-threaded 100 ms tick, immutable snapshots with interpolated CDF and quantile queries, empty cell = free and certain), `OccupancyMeter` (the exact on_event accumulator with an injectable clock; the unit test reproduces the 10x90 + 50x10 = 14 worked example), and `MeasurementPlane` (per-node wiring, control-interval close, occupancy_N.csv and histograms_N.csv outputs) landed in `org.example.Server`; `KvClientService` opens the slot at t_recv, closes it at reply, computes the per-request gap, and files by the step 8 rules - the abandoned-wait rule (file at maxWaitMs under the chosen level, nothing under the fallback) is integration-tested. One refinement: linearizable's confirmation wait is now bounded by the same maxWaitMs budget as every other level instead of its internal 1 s timeout. S_max ships as a config placeholder (1000) until the stage 6 sweep. Verification: 21 tests green; full run of 9.63M requests (0 lost, 0 violations) with utilization tracking the completed-service cross-check at 12-16% mean deviation (the predicted boundary bias with ~50 ms LIN times against a 100 ms interval), leader utilization ~0.7-0.9 under Heavy, and the cell structure coming out right: leader shows LIN mean 16.9 ms / W:2 14.6 ms vs sub-ms immediate levels, followers populate causal cells at gap bucket 1 as anchors chase replication.

### Stage 4: economics

- SLA registration: read/write SLA tables (sets of (kappa, delta, pi) rungs) loaded from the config file per application; requests carry only (app id, SLA id).
- `RungScorer` implementing steps 4a through 4g: satisfiable-rung restriction, threshold conversion by subtracting rho, sort/dedup with identical thresholds collapsed to the suffix max, by-parts expected profit with linear interpolation into the histogram CDF, score `V_c = E_c - lambda * omega_c`, argmax. Uncalibrated cells are free-and-certain with a small cap on concurrent riders.
- Unit tests must reproduce the worked example: expected profits 4.96, 5.76, 8.08; linearizable wins at lambda 0.02, eventual wins at lambda 0.08.
- Admission (step 5): reject when max V_c <= 0; hard occupancy cap at ~1.5x S_max; leader-only replication rate bucket, in memory, budgeted at measured max commit rate, charged one entry per admitted write, refilled per second, never returned.
- Price controller per control interval: close accumulators, compute u, `lambda <- max(lambda_min, lambda * exp(eta * (u - u_target)))` with u_target 0.85, eta 1, lambda_min 0.0001. Requests read only the published lambda.
- The stage 2 forced-level knob is removed; the scorer is now the only decision point.
- Tests: scorer vectors above; controller unit test (lambda rises under u > target, floors at lambda_min); integration: light load rejects nothing with lambda near the floor, overload sheds lowest-value requests first.

Exit: under a load ramp, degradation is continuous in lambda with no mode boundary; worked-example tests pin the arithmetic.

**Status: done (2026-08-13).** `RungScorer` implements 4a-4g as pure functions (the unit test reproduces the worked example exactly: 4.96/5.76/8.08, linearizable wins at lambda 0.02, eventual at 0.08); `SlaRegistry` holds the (appId, slaId) tables from the new config `slas` section; `PriceController` runs the multiplicative update in `MeasurementPlane`'s control interval and publishes lambda (now a column in occupancy_N.csv); `ReplicationRateBucket` is the leader's write backstop. `KvClientService` is now decided entirely by the scorer: legal set by role (an SLA with no rung satisfiable on a follower redirects rather than rejects), argmax with ties to the weakest level, the uncalibrated-rider cap on cold cells, rejection when max V <= 0 or a hard backstop trips (occupancy cap at 1.5x S_max - which the first shedding-test draft tripped on every request, S_max must exceed typical in-flight; replication bucket for writes), and the step 6 wait bound is now the chosen level's d_max instead of the global maxWaitMs. The forced-level protocol fields died (reserved), KvResponse gained `rejected`, phases lost their level distributions entirely: the mix is emergent. Client rejections are terminal (retrying would defeat shedding) and land in a new RejectedTotal ledger column. Verification: 32 tests green including deterministic value-order shedding (price injected at floor/mid/extreme operating points after warming every cell past the cold-start rule). Three full runs told the tuning story: with S_max=1000 and 15-60 ms thresholds, all shedding was rho-driven (client-side congestion consumed the tight budgets per 4b - gold/silver shed, bronze untouched, an instructive inversion since expected profit died with the deadlines) and lambda never left the floor; with S_max=150 and worked-example-scale thresholds (75-500 ms), the final run shows the intended economics: lambda floored through Light/Medium, rising to ~0.1 as Heavy pushes u past 0.85, regulating utilization back toward target, collapsing to the floor in Recovery; 10.48M requests, 8.90M served, 1.59M rejected, 0 lost, 0 violations. At its peak lambda stayed below the gold app's LIN-to-CM flip point (~0.24 at the measured omegas), so the ramp's visible effect was value-ordered shedding rather than a mix shift; the flip arithmetic is pinned by the unit tests and both shedding mechanisms (rho-driven and price-driven) are continuous with no mode boundary.

### Stage 5: client and grading

- SLA-driven workload in `WorkloadSimulator`: applications with registered SLAs issuing reads and writes over a keyspace with a uniform/zipfian skew knob; phases now vary load and mix, not per-request consistency choices.
- Client RTT estimation built on the `LatencyAwareRouter` sliding-window machinery: RTT = end-to-end latency minus reported service time, sampled only from replies that involved no server-side waiting (the response can flag this); the estimate rides each request.
- Server-side grading (step 7): delivered level from the returned value's index vs commit index and session indices; realized profit as the best rung met on both consistency and total time.
- Instrumentation, the four streams: predicted vs realized profit per request; free vs waiting upgrades against write load and skew; per-level tail latency (client-observed); satisfied rung per request. Client-observed comparison metrics live in `ClientMetricsTracker`; server CSVs may remain for debugging but cross-arm comparison numbers come from the client.
- Tests: RTT estimator excludes waited replies; grading cases (value index below commit index upgrades local to majority; replica ahead of session index upgrades eventual to causal); profit accounting reconciles predicted vs realized on a deterministic sequence.

Exit: a full run emits all four streams and the numbers reconcile (sum of realized profit matches the grading of the per-request records).

**Status: done (2026-08-13).** `Grading` implements step 7 as pure functions: the graded strength starts at the executed level (its mechanics guaranteed it) and rises to the strongest level whose own requirement the delivered result meets - eventual-majority when the returned value's index is at or below the commit index, causal-local when the serving view's frontier covered the session's uncommitted anchor, causal-majority when the value is committed and the commit index covered the committed anchor, and linearizable never, since it requires the confirmation round only the executed path performs. Realized profit is the highest-profit rung met on both the graded strength and total time (service time plus rho); writes grade on the replication count at acknowledgment. Responses carry (satisfiedRung, predictedProfit, realizedProfit, gradedReadStrength). The four streams live in `ClientMetricsTracker` as cumulative per-(node, chosen, executed) cells rather than per-request rows (at 70k TPS a per-request log would distort the experiment; the sums still reconcile exactly): predicted and realized profit sums, free-vs-waiting upgrade counts (a graded delivery above the SLA's floor, split by the response's waited flag, with the floor map handed to each client at construction), client-observed P50/P95/P99 per cell from a geometric histogram, and satisfied-rung counts (SLAs are capped at 4 rungs so the columns stay fixed). The RTT estimator that stage 2 pulled forward was extracted into `Client/RttEstimator` with the excludes-waited-replies rule now unit-tested, and the workload gained the `workload` config section (keySpace, uniform/zipfian, exponent) with `KeySampler` driving both read and write key draws; the write path previously round-robined keys and now draws from the same distribution. Verification: 55 tests green, including the plan's two grading cases (committed value upgrades local to majority; replica at or ahead of the session index upgrades eventual to causal), both also exercised end to end through the real protocol, and a reconciliation test that regrades every response record from its own fields and matches the server's sums exactly. Full run (runs/stage5-verify_20260813_091619, exit 0): 10.47M sent, 8.67M served, 1.80M rejected, 0 lost, 0 violations; predicted 39.96M vs realized 39.96M profit (ratio 1.000 - with worked-example-scale thresholds the CDFs sit near 1, so E_c approaches the suffix-max and realized slightly exceeds predicted through free upgrades paying above the chosen level's expectation); 86.6% of read upgrades were free, and the split is structurally sensible (the LIN/CM app's upgrades wait on confirmation rounds, the weak-floor apps upgrade for free via committed values); satisfied-rung counts 6.59M rung-0 and 2.09M rung-1 with zero unmet among served requests. In this run the shedding was rho- and hard-cap-driven (leader u peaked at 1.11 but lambda barely left the floor before load fell back), consistent with the machine-dependence documented in stage 4.

### Stage 6: calibration and options

- Load sweep script to place S_max at the latency knee and to measure the maximum commit rate for the replication budget; both land in config with the sweep documented.
- Follower linearizable reads behind a config flag: follower obtains the read index from the leader and waits for its own apply, relieving leader concentration. Off by default until measured.
- Decide the optional wc:1-with-later-confirmation write rung: implement only if loss detection (term+index check) needs no per-session server state, else drop it as the spec allows.
- Sweep the instrumentation matrix (write load x skew) for the free-vs-waiting upgrade fraction.

Exit: calibrated defaults committed; a documented baseline run demonstrating price-based degradation end to end.

**Status: done (2026-08-13).** `sweep_smax.sh` runs the load sweep with every shedding mechanism disabled (sMax and the replication budget set huge, thresholds x100 so rho cannot spend a budget) and one 100%-write point for the commit rate; `sweep_upgrades.sh` runs the write-load x skew matrix. Sweep (runs/sweeps/smax_20260813_094938): latency grows near-linearly from 9.6 ms avg / 43.8 ms P95 at 10k TPS to 35.7 / 88.1 at 70k with the steepest slope break between 30k and 40k, so the knee was placed in the 40-50k band and S_max set to 130 (the leader's time-weighted in-flight at 50k; the 0.85 target then regulates in-flight to ~110, right at the slope break) - the script's mechanical 3x-P95 rule suggested 181 and was overridden by judgment, documented here. Max commit rate measured 38,300 entries/s, so replicationBudgetPerSecond is 30,000 (~80%). The first sweep attempt failed at 60k with 11 session violations and truncation-guard exceptions, which exposed a real Raft deviation present since before the redesign: followers truncated unconditionally at prevLogIndex+1, so the duplicate and reordered rounds that a 30 ms fire-and-forget replication schedule routinely produces would wipe and re-append the log - a stale empty probe could wipe it outright, after which the monotonic wait registries let causal reads serve the emptied view. Fixed per Raft 5.3 (truncate only at the first (index, term) conflict; matching prefixes and heartbeats never truncate) with a deterministic regression test; the re-run swept cleanly through 70k with zero violations. Follower linearizable reads landed behind `chameleon.followerLinearizableReads` (default false): a ConfirmReadIndex RPC runs the leader's ReadIndex round on behalf of the follower, which then waits for its own commit index to reach the returned index and serves from committed state; integration-tested with the flag on (a LIN-only SLA is served by the follower itself, no redirect). The wc:1-with-later-confirmation rung is dropped: the term+index check itself is stateless, but delivering the confirmation needs either a new client-facing term-at-index query path with per-write client tracking, or per-session server state (the spec's disqualifier) - for a rung that does not close the loss window and is worth less by the spec's own reading, the extra path is not justified. Upgrade matrix (runs/sweeps/upgrades_20260813_095635, 30k TPS): the overall free fraction falls with write load (87.5% at 5% writes, 81.2% at 25%, 72.4% at 50%) because wc:2 write upgrades always wait on commit, while the read free fraction rises (90.9% to 96.3%) as the faster-advancing commit index covers session anchors sooner; key skew is a null result (uniform vs zipfian within 0.3%) because upgrade waits key on log and commit indices, not key hotness. Baseline with calibrated defaults (runs/stage6-baseline_20260813_100141, exit 0): 10.17M sent, 8.46M served, 1.71M rejected, 0 lost, 0 violations; the leader's Heavy-phase utilization regulated to mean 0.851 against the 0.85 target (lambda mean 0.106, peak 0.228, floored in Light/Medium and collapsing in Recovery), write shedding in strict value order (gold 24.0%, silver 32.6%, bronze 35.4% rejected), read shedding near-uniform (15.1-15.7%, rho-driven at the client edge with the tilt favoring gold). The gold app's read mix held at ~20% linearizable through every phase: the LIN-to-CM flip sits at ~0.24 at the measured omegas, just above the price peak, so degradation manifested as value-ordered shedding rather than a mix shift - the calibrated controller now reaches the flip's doorstep where stage 4's uncalibrated run peaked at 0.1.

### Comparison arms: Pileus and static baselines

**Done (2026-08-13).** The config's `mode` selects the experiment arm - who resolves the subSLA target and how contact servers are chosen: `chameleon` (server scorer, lowest-RTT routing), `chameleonPileus` (server scorer, Pileus routing), `pileus` (client picks (server, rung) jointly by expected profit), `highestProfit` / `lowestProfit` (static max-profit or floor rung target, lowest-RTT routing). The config split into mode-independent `server` (maxWaitMs, sMax, replicationBudgetPerSecond, followerLinearizableReads) and `client` (rttWindowSize, retryLimit, lostTimeoutMs) blocks, `chameleon` keeping only the price controller and `pileus` holding the exploration fraction. Mode-independent mechanics: the occupancy hard cap (1.5x sMax) and the replication bucket are universal admission; every read reply carries both views (indices stamped before the views are read, so client-side claims can only be understated); grading moved client-side (`ClientGrader`, one code path for every arm, judged on client-observed latency against both views with session assertions riding whichever view makes a causal claim); the server's grading fields remain as chameleon-mode diagnostics. Non-chameleon requests carry the client-resolved target (`wantLinearizable`, `requestedWriteConcern`) plus a wait bound of threshold minus RTT - the client's d_max analog, server-clamped to maxWaitMs - and the dumb server path serves both views immediately, waiting only for explicit ack semantics. The Pileus client (`PileusSelector`) holds 2n + wc response-time sliding windows (per-server plain reads, per-server linearizable reads, per-concern writes), binary consistency feasibility from per-server high-water indices vs the session anchors, cold-start optimism, and a small exploration fraction; follower linearizable reads batch into shared read-index rounds (arrivals before a round's send share its index). Two real bugs surfaced during the smoke runs and are fixed with pinned tests: post-hoc grading upgrades are now cumulative (a session with wc:1-acked but no majority-acked writes was graded causal-majority vacuously on replicas that had never seen its acked writes - the assertion that caught it was right), and rejections now file penalty samples into the selector's windows (a rejecting node's empty windows read as cold-start optimism, locking the selector onto it). Verification: 76 tests green including per-arm smoke integration tests; 15 s binary smokes of all five modes at 5k TPS, all exit 0 with zero lost and zero violations, with the expected shedding fingerprint (chameleon and lowestProfit serve everything; highestProfit sheds most via top-rung concentration; the Pileus-routed arms in between). No comparison experiments have been run yet.

**5-node default (2026-08-13).** The default config moved from 3 to 5 nodes; the top write rungs for apps 1 and 2 moved from concern 2 to concern 3 because they were the majority rung under 3 nodes and would otherwise stop closing the loss window (committed session anchors would never advance and causal-majority would become vacuous). The sweep was re-run for 5 nodes (runs/sweeps/smax_20260813_110452): the same 40-50k knee band gives S_max 100 (leader in-flight 97.9 at 50k - lower per TPS than at 3 nodes because the leader now takes 1/5 of round-robin reads), and the max commit rate is essentially unchanged at 39,100 entries/s (leader-bound, not follower-count-bound), so replicationBudgetPerSecond is 31,000. Verification runs (runs/five-nodes_20260813_105839 and runs/five-nodes-calibrated_20260813_111132, both exit 0, 0 lost, 0 violations, ~10.5M requests each; W:3 dominates the write mix, confirming the anchors advance): at this cluster size the Heavy leader sits at mean u 0.77 with only burst excursions past target, because rho-driven shedding at the client edge (~1.5M rejections) trims load before occupancy sustains past 0.85 - so the price mechanism engages only in bursts and rho dominates, which is the documented machine- and topology-dependence of the split between the two continuous shedding mechanisms, not a calibration error. The 3-node baseline above remains the demonstration of price-driven degradation.

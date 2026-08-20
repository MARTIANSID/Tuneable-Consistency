# Experiment workload: topology, applications, SLAs, phases

Reference configuration for the evaluation.

Every application uses 50 sessions, each belonging to one application and one client site, and holding its own committed and uncommitted counters, shared across that application's SLAs. Key space is disjoint per application. Sessions are long-lived and never reset. An arriving request picks one session of its application uniformly, which also determines where it is issued from. Arrivals are Poisson, open loop, and there is no retry.

## Topology and expected level costs

Five nodes, one per region, leader in N. Virginia. All delays below are **one-way**, which is what netem injects; double them for a round trip. They are representative of real inter-region latencies rather than a uniform injected value, which matters because a reviewer already objected that uniform netem cannot reproduce asymmetric paths.

| one-way ms | A (N. Virginia) | B (Oregon) | C (Ireland) | D (Singapore) | E (São Paulo) |
|---|---|---|---|---|---|
| A | 0 | 30 | 40 | 115 | 60 |
| B | 30 | 0 | 65 | 85 | 90 |
| C | 40 | 65 | 0 | 90 | 95 |
| D | 115 | 85 | 90 | 0 | 160 |
| E | 60 | 90 | 95 | 160 | 0 |

## Client sites

One client site per region, colocated with that region's node at 0.5 ms one-way. A client reaching any other node pays the matrix delay above. Without distributed clients every client would see identical distances and the routing comparison would have nothing to measure, so this is load-bearing rather than decorative.

Sessions carry a site. A1, A2, and A3 spread their 50 sessions evenly across all five sites, ten each, since shopping, feed reading, and login are worldwide. A4 does not: payment processing is regional, so its sessions sit 60 percent in A and 40 percent in B. That is realistic and it also keeps A4's tight thresholds satisfiable, since a Singapore client is 230 ms round trip from the leader and could never meet a 200 ms payment deadline no matter what the system did.

## Expected level costs

With the leader at A, a majority is the leader plus two, so it waits for the second acknowledgement to return: 60 ms round trip from B, 80 ms from C.

| Operation | Expected cost | Why |
|---|---|---|
| eventual-local, eventual-majority | 1 to 3 ms | Local read, no coordination |
| causal-local, causal-majority | 1 to 3 ms, plus wait | Local read unless the session's floor is ahead of the replica |
| linearizable | 80 ms and up | ReadIndex confirmation round to a majority |
| wc:1 | 1 to 3 ms | Local append |
| wc:2 | 60 ms and up | One acknowledgement, from B |
| wc:majority | 80 ms and up | Two acknowledgements, from B and C |

Add the client's own round trip on top: a client in A pays 1 ms, one in D pays 230 ms to reach the leader at all.

Replicas lag by their one-way distance from the leader plus batching, so roughly 30 ms at B, 40 ms at C, 60 ms at E, and 115 ms at D. D and E are therefore where causal upgrades actually have to wait, and where a client reading locally will often be offered a weaker level than one sitting next to the leader. That heterogeneity across sites, from one SLA, is the Pileus story stated as a measurement.

The single realism error in the previous SLA set was thresholds below these costs. A linearizable rung at 30 ms is not a demanding SLA, it is a dead rung no arm can ever satisfy, and it silently removes the level from the experiment. Every linearizable and majority-write threshold is now above 80 ms plus a client round trip.

Leader placement is a cheap extra sweep worth running: moving the leader to D inverts the cost landscape, making writes expensive for the Americas and cheap for Southeast Asia, which is the natural configuration for the flash-sale scenario.

Also run a **single-region control** with all five nodes and all clients in one region at about 0.25 ms one-way, where majority operations cost 1 to 2 ms and every rung is achievable. There the price binds on capacity alone rather than on capacity and latency together, which separates the two effects and answers the suspicion that large WAN delays were chosen to make the problem look worse than it is. Scale all thresholds down by roughly 10x for that config.

## A1: online shopping

A shopper browsing a catalogue and adding items to a cart. Almost everything is browsing, worth little and stale-tolerant, but occasionally the same person checks out, and at that moment the cart must reflect what they just added and the request is worth far more. Terry's canonical latency-favouring class.

A1 carries three read SLAs, which is what makes value-ordered shedding observable inside one application, on the same node, from the same session pool, over the same key range. The third is active only during flash sale segments.

Write fraction 10 percent.

**slaId 1, browsing.** Prefers session-consistent catalogue data, accepts anything committed, accepts anything at all rather than nothing. This is the only place eventual-majority appears, so without it that level is never rewarded and never chosen.

| Level | Threshold | Profit |
|---|---|---|
| causal-local | 40 | 5 |
| eventual-majority | 40 | 4 |
| eventual-local | 200 | 1.5 |

**slaId 2, checkout.** The cart must reflect what the shopper just added, and it must not reflect state that later rolls back, because money is about to move.

| Level | Threshold | Profit |
|---|---|---|
| causal-majority | 60 | 50 |
| causal-local | 60 | 38 |
| eventual-local | 60 | 15 |

**slaId 3, flash checkout.** Active only during a flash sale. What changes during the event is not value but the correctness requirement: stock is a handful of units and thousands of people are racing for it, so a stale inventory read means selling something you do not have. The floor rises accordingly, from causal-local on an ordinary day to majority-committed here. Patience rises too, since a shopper will wait a second and a half rather than lose a one-dollar car. Profit is only modestly above ordinary checkout, on customer-acquisition grounds, because per unit of revenue a discounted item is worth less, not more.

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 300 | 70 |
| causal-majority | 300 | 52 |
| causal-majority | 1500 | 30 |

There is deliberately no eventual rung. The application would rather be refused than oversell, which is what real flash sales do when they put people in a queue. That makes this the one segment where the price cannot economise by downgrading, because the floor forbids it, and admission is the only lever left. Every other segment gives the price the option of serving something cheaper.

**Writes.** Adding to a cart should be acknowledged instantly; the order record itself must be durable.

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 200 | 12 |
| wc:1 | 40 | 8 |
| wc:1 | 200 | 2 |

## A2: advertising-funded search or news feed

Results may be a little stale without anyone noticing, but the reader must not see their own feed go backwards, so the requirement is session ordering rather than durability: causal-local, not causal-majority. A story that later rolls back is not a problem for a feed. Revenue falls as latency rises, so the ladder varies only in the deadline. Terry's consistency-favouring class.

Its writes are billing events, rare but never allowed to be lost, so every write rung demands full durability. That is where A2's greed lives: it never contributes a write upgrade decision, only expensive load, and the price has to keep it from crowding out everyone else.

Write fraction 5 percent.

**Reads.**

| Level | Threshold | Profit |
|---|---|---|
| causal-local | 20 | 5 |
| causal-local | 50 | 4 |
| causal-local | 120 | 2.5 |
| causal-local | 400 | 0.25 |

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 120 | 18 |
| wc:majority | 300 | 12 |
| wc:majority | 800 | 5 |
| wc:majority | 2000 | 0.6 |

## A3: login and credential checking

A password check reads credentials, and the standard pattern is a fast weak read first with an authoritative fallback only if the check fails. So the ladder prefers linearizable quickly, takes eventual quickly over linearizable slowly, and puts slow linearizable last. Two rungs share the 150 ms threshold, so a linearizable result must claim the higher-paying one; A3 is the only SLA exercising that deduplication.

Its writes are password changes and session records, where the durability tradeoff is real and graded, and A3 is the only place wc:2 appears.

Write fraction 30 percent.

**Reads.** One ladder, unchanged across scenarios. A login is a login; what a flash sale changes is how many of them arrive, not what each is worth.

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 150 | 9 |
| eventual-local | 150 | 4.5 |
| linearizable | 600 | 2 |

That produces an effect worth a paragraph in the discussion. During the flash segment, logins stay cheap at 9 while flash checkouts carry an expensive floor, so the price sheds logins to protect checkouts, and a customer who cannot log in never reaches the checkout the system was busy protecting. Per-request value allocation is blind to cross-request dependencies, so shedding a cheap prerequisite destroys the valuable thing downstream. It falls out of the workload rather than being arranged, which makes it a finding rather than a demonstration.

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 150 | 15 |
| wc:2 | 120 | 10 |
| wc:1 | 40 | 6 |
| wc:majority | 600 | 4 |

## A4: payments and account balances

Low volume, high stakes. A4's distinctiveness lives in its writes and its consistency floor rather than in a blanket multiplier: a balance read is worth about the same as a checkout read, since both gate the same transaction, but a transfer acknowledged and then lost is in a class of its own. A4 is the application that should still be served, and still upgraded, at prices that have shed everyone else.

Write fraction 20 percent.

**Reads.**

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 200 | 50 |
| causal-majority | 100 | 30 |
| causal-majority | 400 | 12 |

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 250 | 150 |
| wc:majority | 1000 | 60 |

## Scenario mixes

Weights are relative and normalised to 100. Each mix is a day in the life of the platform rather than a synthetic knob setting, which is why the composition and the rate move together in most of them.

**normal.** An ordinary day. Overwhelmingly browsing, a trickle of checkouts, steady logins, payments the thinnest stream in the system.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 32 }   # browse
- { applicationId: 1, type: read,  slaId: 2, weight: 4 }    # checkout
- { applicationId: 1, type: write, slaId: 1, weight: 4 }
- { applicationId: 2, type: read,  slaId: 1, weight: 33 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 14 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 4 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**holiday.** The weeks before Christmas. Shopping displaces feed reading, conversion creeps up, everything else follows proportionally. Nothing dramatic; the system simply runs hot for a long time.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 38 }
- { applicationId: 1, type: read,  slaId: 2, weight: 6 }
- { applicationId: 1, type: write, slaId: 1, weight: 5 }
- { applicationId: 2, type: read,  slaId: 1, weight: 26 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 12 }
- { applicationId: 3, type: write, slaId: 1, weight: 5 }
- { applicationId: 4, type: read,  slaId: 1, weight: 5 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**saleDay.** Prime Day or 11.11. Far more browsing converts, cart writes multiply, payments follow, and the feed is ignored. Volume and value rise together, which is the realistic case the earlier synthetic mixes were artificially separating.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 26 }
- { applicationId: 1, type: read,  slaId: 2, weight: 14 }
- { applicationId: 1, type: write, slaId: 1, weight: 12 }
- { applicationId: 2, type: read,  slaId: 1, weight: 16 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 12 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 9 }
- { applicationId: 4, type: write, slaId: 1, weight: 3 }
```

**preFlash.** The minutes before a flash sale. Everyone signs in to position themselves, so logins dominate and, because A3 is write-heavy, replication lag climbs right before the burst arrives. Roughly 25 percent writes.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 18 }
- { applicationId: 1, type: read,  slaId: 2, weight: 3 }
- { applicationId: 1, type: write, slaId: 1, weight: 2 }
- { applicationId: 2, type: read,  slaId: 1, weight: 8 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 40 }
- { applicationId: 3, type: write, slaId: 1, weight: 20 }
- { applicationId: 4, type: read,  slaId: 1, weight: 6 }
- { applicationId: 4, type: write, slaId: 1, weight: 2 }
```

**flash.** A one-dollar car at noon. Browsing collapses, the feed is abandoned, and almost everything is a checkout attempt on a single item under the flash SLA, with logins still arriving heavily under their ordinary ladder. Payments are a trickle because only the winners pay. Roughly 27 percent writes.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 5 }
- { applicationId: 1, type: read,  slaId: 3, weight: 45 }   # flash checkout
- { applicationId: 1, type: write, slaId: 1, weight: 20 }
- { applicationId: 2, type: read,  slaId: 1, weight: 2 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 18 }
- { applicationId: 3, type: write, slaId: 1, weight: 5 }
- { applicationId: 4, type: read,  slaId: 1, weight: 3 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**collapse.** Immediately after. Load falls off a cliff, the losers drift back to browsing, and the winners pay, so payments are briefly elevated.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 25 }
- { applicationId: 1, type: read,  slaId: 2, weight: 6 }
- { applicationId: 1, type: write, slaId: 1, weight: 5 }
- { applicationId: 2, type: read,  slaId: 1, weight: 20 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 15 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 15 }
- { applicationId: 4, type: write, slaId: 1, weight: 7 }
```

**payday.** End of month. Balances and transfers surge while browsing stays flat. The mirror image of the greedy-neighbour test: the surging tenant is the most valuable one, so the price should admit it and shed cheap browsing to make room.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 22 }
- { applicationId: 1, type: read,  slaId: 2, weight: 3 }
- { applicationId: 1, type: write, slaId: 1, weight: 3 }
- { applicationId: 2, type: read,  slaId: 1, weight: 22 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 10 }
- { applicationId: 3, type: write, slaId: 1, weight: 4 }
- { applicationId: 4, type: read,  slaId: 1, weight: 25 }
- { applicationId: 4, type: write, slaId: 1, weight: 10 }
```

## The day sequence

One continuous run of about six minutes per arm. Rates are fractions of measured capacity C so the run is portable. Sessions persist throughout, so history and lag carry across segments, which is realistic and is also why this is the dynamics figure rather than the quantitative one.

The cluster is provisioned the way a cost-conscious operator would provision it: for ordinary peak plus headroom, not for the flash sale. Nobody buys twenty times capacity to sit idle all year, so normal load sits at half of C and the sale day exceeds it.

| Segment | Duration | Rate | vs normal | Mix | What it shows |
|---|---|---|---|---|---|
| Normal day | 60 s | 0.5 C | 1x | normal | Baseline. The price rests on its floor and nearly everything upgrades free |
| Holiday plateau | 60 s | 0.8 C | 1.6x | holiday | Sustained seasonal lift. Tests equilibrium rather than reaction, and is where an oscillation would appear |
| Sale day | 60 s | 1.8 C | 3.6x | saleDay | Prime Day scale. Rate and value rise together, which is the realistic combined case |
| Pre-flash login surge | 30 s | 1.0 C | 2x | preFlash | People gathering and signing in. Write-heavy logins drive lag up just before the burst |
| Flash sale | 15 s | 2.5 C | 5x | flash | Roughly sixty percent of arrivals must be refused, and the floor is expensive, so downgrading is unavailable and admission is the only lever |
| Collapse | 15 s | 0.7 C | 1.4x | collapse | Load vanishes. Does the price come down promptly or strand capacity |
| Payday | 45 s | 0.6 C | 1.2x | payday | Composition shifts while volume barely does. The valuable tenant is the one surging |
| Return to normal | 60 s | 0.5 C | 1x | normal | Recovery, compared directly against the opening segment |

Two deliberate compressions to state in the methodology. A holiday plateau lasts weeks in reality and sixty seconds here, which is honest because the system's slowest adaptation timescale is a few seconds, set by the histogram and acceptance-rate decay; segments meant to settle get about ten times that, and the flash sale is deliberately shorter than settling time because its point is the transient. And real flash sales reach ten to fifty times normal traffic rather than five, which was capped for load-generation reasons. The binding quantity there is the rejection fraction rather than the multiple of capacity, and at 2.5 C it is already high enough to separate value-ordered from arrival-ordered shedding; a higher peak changes the size of the gap, not its existence or its ordering.

This sequence does not replace a static load sweep, which is the quantitative backbone and is immune to the carryover objection: hold the mix at normal and step the rate from 0.2 C to 2.5 C, 45 seconds per step, reporting delivered profit, rejection rate, and per-level tail latency. The arms are indistinguishable below saturation, so a sweep that never crosses C will show four flat lines.

If experiment time is short, the priority order is the load sweep on the geo topology for all four arms, then the day sequence on geo for all four arms, then the load sweep on the single-region control, then failure injection for the write concern result. That is roughly twelve minutes per arm per topology, so under an hour for one full replication of the geo config.

## Notes

Replication lag rises with total write load, and the fraction of causal reads that must wait is roughly `min(1, (w * T / N) * lag)` per application, where `w` is its write fraction, `T` the total rate, and `N` its session count. That is why the write surge changes behaviour without changing the rate.

With 50 sessions for every application regardless of its share of load, per-session activity differs: A2 at 35 percent of traffic has busy sessions with high floors, while A4 at 5 percent has quiet sessions whose counters fall behind, so more of A4's causal upgrades will be free. Its linearizable rung stays expensive regardless, so the A4 story survives, but it will show in the free-versus-waiting split.

One modelling detail to pin down in the implementation: the levels are not a total order. Causal-majority does not imply causal-local, since one is a statement about committed state and the other about log presence, and a session that wrote at wc:1 can have an uncommitted index above the committed one. For scoring, count only the rungs a level definitely satisfies, which is conservative. Post-hoc grading will award anything extra that was actually delivered.

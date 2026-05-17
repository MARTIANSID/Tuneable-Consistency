# Install Java 17

```bash
sudo apt update
sudo apt install openjdk-17-jdk maven -y
```

---

# Verify Java + Maven

```bash
java -version
mvn -v
```

---

# Configure Java 17

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Persist it:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

# Install Redis

```bash
sudo apt install redis-server -y
```

---

# Start Redis Server

```bash
sudo systemctl start redis-server
```

Enable Redis on boot:

```bash
sudo systemctl enable redis-server
```

Verify Redis is running:

```bash
redis-cli ping
```

Expected output:

```text
PONG
```

---

# Redis Configuration Used by the Project

The token bucket implementation connects to Redis using:

```text
Host: 127.0.0.1
Port: 6379
```

Make sure Redis is running on port `6379` before starting the experiment.

You can verify using:

```bash
redis-cli -p 6379 ping
```

---

# Build the Project

From the repository root:

```bash
mvn clean install
```

---

`Servers.java` is the main entry point for running the full distributed consistency experiment framework in [Tuneable-Consistency](https://github.com/MARTIANSID/Tuneable-Consistency?utm_source=chatgpt.com).

It automatically:

* Starts multiple gRPC servers
* Elects a leader
* Injects transactions into the system
* Runs workload phases (Light → Medium → Heavy → Light)
* Collects TPS/latency/backlog CSV metrics
* Simulates consistency upgrades
* Optionally simulates node failures and geo latency

---

# Run the Experiment

From the repository root:

```bash
mvn clean install
mvn exec:java -Dexec.mainClass="org.example.Server.Servers"
```

---

# What Happens When You Run It

## 1. Starts Multiple Servers

This section:

```java
public static final int NUM_OF_SERVERS = 3;
```

creates 3 distributed servers.

They run on:

| Server  | Port |
| ------- | ---- |
| Server0 | 8001 |
| Server1 | 8002 |
| Server2 | 8003 |

You will see:

```text
Server0 started on port 8001
Server1 started on port 8002
Server2 started on port 8003
```

---

## 2. Starts Client Callback Server

The framework also starts a callback listener:

```text
Client callback server started on port 9000
```

This is used for:

* ACK collection
* latency tracking
* timestamp synchronization

---

# Workload Phases

The experiment automatically changes workload over time.

Defined here:

```java
PHASES.add(new Phase(...))
```

Current setup:

| Phase  | Duration | TPS     | Characteristics           |
| ------ | -------- | ------- | ------------------------- |
| Light  | 70s      | 30k TPS | Mostly eventual reads     |
| Medium | 60s      | 30k TPS | Balanced consistency      |
| Heavy  | 60s      | 70k TPS | Mostly linearizable reads |
| Light  | 60s      | 30k TPS | Recovery phase            |

---

# What Gets Tested

The framework tests:

* Tunable consistency
* Dynamic transaction upgrades
* Throughput under load
* Latency behavior
* Queue pressure
* Read/write consistency tradeoffs
* Failure handling
* Token bucket admission control

---

# Important Toggles

## Transaction Upgrading

```java
private static final boolean UPGRADE_TRANSACTIONS = true;
```

When enabled:

* transactions can be upgraded dynamically
* stronger consistency may be applied during execution

---

## Pressure Mode

```java
private static final boolean PRESSURE_MODE_ENABLED = false;
```

Controls:

* pressure-aware admission
* deferrals
* token bucket behavior

---

## Node Failure Simulation

Enable:

```java
private static final boolean ENABLE_NODE_NETWORK_FAILURE = true;
```

Then choose:

```java
private static final int FAILED_NODE_ID = 0;
```

This simulates a failed node by dropping inter-server RPCs.

---

## Timed Failure Injection

Enable:

```java
private static final boolean ENABLE_TIMED_NODE_FAILURE = true;
```

You can fail:

* leader
* follower

after:

```java
private static final int FAILURE_AFTER_SECONDS = 40;
```

Useful for:

* re-election testing
* liveness evaluation
* fault tolerance experiments

---

# Geo Latency Simulation

Enable:

```java
private static final boolean ENABLE_GEO_SETTINGS = true;
```

Uses:

```bash
simulate_geo_latency.sh
```

to inject artificial network delay using Linux `tc/netem`.

Example:

```java
private static final int GEO_LATENCY_MS = 50;
```

Useful for:

* WAN simulations
* geo-distributed consistency analysis

---

# Generated CSV Metrics

The experiment automatically creates CSV files like:

| File                                   | Description           |
| -------------------------------------- | --------------------- |
| `tps_0.csv`                            | TPS metrics           |
| `system_latency_0.csv`                 | end-to-end latency    |
| `backlog_0.csv`                        | queue backlog         |
| `avg_latencies_0.csv`                  | average latency       |
| `read_latencies_0.csv`                 | read latency          |
| `process_batch_duration_0.csv`         | batch execution time  |
| `incoming_transaction_rate_global.csv` | global injection rate |

These are used later for plotting and analysis.

---

# Transaction Types Generated

The framework injects:

## Writes

With tunable:

* write concern
* consistency level

Examples:

* W:1
* majority

---

## Reads

Supports:

* EVENTUAL
* CAUSAL_LOCAL
* CAUSAL_MAJORITY
* LINEARIZABLE

Configured in:

```java
Map<ReadClass, Double> ...
```

---

# Injection Model

Transactions are injected in batches:

```java
private static final int BATCH_SIZE = 1000;
```

The injector:

* distributes reads/writes
* routes linearizable reads to leader
* routes weaker reads to followers
* measures throughput continuously

---

# Typical Workflow

## 1. Configure Experiment

Edit:

* TPS
* phases
* read/write ratios
* consistency distributions
* failure toggles

---

## 2. Run Servers.java

```bash
mvn exec:java -Dexec.mainClass="org.example.Server.Servers"
```

---

## 3. Wait for Experiment Completion

The framework automatically:

* cycles phases
* collects metrics
* shuts down

---

## 4. Plot Results

Use the Python plotting script to visualize:

* TPS
* latency
* token usage
* upgraded transactions
* profit
* backlog

---

# Useful Places to Modify

## Change Number of Servers

```java
NUM_OF_SERVERS
```

---

## Change TPS

Inside:

```java
new Phase(...)
```

---

## Change Read/Write Mix

```java
0.90, 0.10
```

---

## Change Consistency Distribution

```java
Map<ReadClass, Double>
```

---

## Change Experiment Duration

```java
TOTAL_EXPERIMENT_DURATION_MS
```

---

# Example Experiment Ideas

## Tunable Consistency Under Load

* Light load → eventual reads dominate
* Heavy load → linearizable reads dominate
* Measure latency + throughput

---

## Failure Recovery

Enable timed leader failure:

* observe re-election
* measure TPS drop
* analyze recovery latency

---

## Geo Distributed Experiment

Enable geo latency:

* compare eventual vs linearizable performance
* observe WAN latency amplification

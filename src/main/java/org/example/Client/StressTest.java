package org.example.Client;

import java.util.ArrayList;
import java.util.List;

import static org.example.Client.WorkloadSimulator.mapOf;


public class StressTest {
    public static void main(String[] args) {
        int numServers = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 16;
        List<Integer> ports = new ArrayList<>();
        for (int i = 1; i <= numServers; i++) {
            ports.add(8000 + i);
        }

        WorkloadSimulator simulator = new WorkloadSimulator(ports, numServers, concurrency);
        // Single stress phase: max throughput, writeConcern = majority
        int majority = (numServers / 2) + 1;
        simulator.addPhase(new WorkloadSimulator.Phase(
                "MaxThroughput",
                70,         // duration in seconds
                20000,       // target TPS (adjust as needed)
                0.0,        // no jitter
                mapOf(1,0.4,2,0.3,majority, 0.3), // 100% transactions at writeConcern=majority
                0.0, 0.0   // no extra profit needed for stress test
        ));
        simulator.run();

    }
}

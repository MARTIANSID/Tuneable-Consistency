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
        // simulator.addPhase(new WorkloadSimulator.Phase(
        //         "MaxThroughput",
        //         200,         // duration in seconds
        //         10000,       // target TPS (adjust as needed)
        //         0.1,        // no jitter
        //         mapOf(1,0.2,2,0.3,3,0.5), // 100% transactions at writeConcern=majority
        //         0.0, 0.0   // no extra profit needed for stress test
        // ));
        // simulator.run();
        simulator.addPhase(new WorkloadSimulator.Phase(
                "WarmUp", 100, 10000, 0,
                mapOf(
                        1, 0.75,
                        2, 0.10,
                        3, 0.15
                                    ),
                0.0, // extraIntermediateProfit
                0.0  // extraMajorityProfit
        ));

        // Phase 2: Heavy Spike (dominantly higher write concerns 3 & majority to drain tokens and cause backlog)
        simulator.addPhase(new WorkloadSimulator.Phase(
                "Spike", 150, 12000, 0,
                mapOf(
                        1, 0.10,
                        2, 0.00,
                        3, 0.90
                ),
                0.0,
                0.0
        ));

        // Phase 3: Stabilization (mixed workload, moderate rate)
        simulator.addPhase(new WorkloadSimulator.Phase(
                "Stabilize", 100, 10000, 0,
                mapOf(
                        1, 0.50,
                        2, 0.30,
                        3, 0.20                
                    ),
                0.0,
                0.0
        ));

        // Run phases in a continuous cycle
        while (true){
            simulator.run();
        }

    }
}

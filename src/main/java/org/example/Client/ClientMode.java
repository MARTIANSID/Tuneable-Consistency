package org.example.Client;

/**
 * The experiment arms, mirroring the config `mode` values. Each bundles who
 * resolves the subSLA target with how contact servers are chosen.
 */
public enum ClientMode {
    /** Server scorer decides; reads go to the lowest-RTT node. */
    CHAMELEON,
    /** Server scorer decides; reads routed by the Pileus selector. */
    CHAMELEON_PILEUS,
    /** Client picks (server, rung) jointly by expected profit. */
    PILEUS,
    /** Client statically targets the max-profit rung; lowest-RTT routing. */
    HIGHEST_PROFIT,
    /** Client statically targets the floor rung; lowest-RTT routing. */
    LOWEST_PROFIT;

    public static ClientMode fromConfig(String mode) {
        return switch (mode) {
            case "chameleon" -> CHAMELEON;
            case "chameleonPileus" -> CHAMELEON_PILEUS;
            case "pileus" -> PILEUS;
            case "highestProfit" -> HIGHEST_PROFIT;
            case "lowestProfit" -> LOWEST_PROFIT;
            default -> throw new IllegalArgumentException("Unknown mode '" + mode + "'");
        };
    }

    /** True when the server-side scorer resolves the subSLA target. */
    public boolean chameleonDecision() {
        return this == CHAMELEON || this == CHAMELEON_PILEUS;
    }

    /** True when reads are routed by the Pileus selector. */
    public boolean pileusRouting() {
        return this == CHAMELEON_PILEUS || this == PILEUS;
    }
}

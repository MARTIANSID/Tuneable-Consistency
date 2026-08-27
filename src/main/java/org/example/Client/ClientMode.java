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
    /**
     * Client statically targets each SLA's strongest consistency rung
     * (reads by level, writes by concern; profit breaks ties within a
     * level); lowest-RTT routing.
     */
    STRONGEST,
    /**
     * Client statically targets each SLA's weakest consistency rung (the
     * floor; upgrades happen only by luck); lowest-RTT routing.
     */
    WEAKEST;

    public static ClientMode fromConfig(String mode) {
        return switch (mode) {
            case "chameleon" -> CHAMELEON;
            case "chameleonPileus" -> CHAMELEON_PILEUS;
            case "pileus" -> PILEUS;
            case "strongest" -> STRONGEST;
            case "weakest" -> WEAKEST;
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

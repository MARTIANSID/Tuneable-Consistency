package org.example.Utility;

public class HybridClock {

    static class TimeStamp {
        long physical;
        long logical;

        public TimeStamp(long p, long l) {
            this.physical = p;
            this.logical = l;
        }
    }

    static class PhysicalClock {
        /*
        This class is an abstraction of local physical clock in the node.
        */
        long physical;
        public PhysicalClock() {}

        public long now() {
            this.physical = System.currentTimeMillis();
            return physical;
        }
    }

    private PhysicalClock physicalClock;
    private long lastPhysical;
    private long nextLogical;

    public HybridClock() {
        this.physicalClock = new PhysicalClock();
    }

    public TimeStamp now() {
        TimeStamp now;
        long currentPhysical = this.physicalClock.now();

        if (currentPhysical > this.lastPhysical) {
            now = new TimeStamp(currentPhysical, 0);
            this.lastPhysical = currentPhysical;
            this.nextLogical = 1;

        } else {
            now = new TimeStamp(this.lastPhysical, this.nextLogical);
            this.nextLogical++;
        }

        return now;
    }

    public void update(TimeStamp in ) {
        TimeStamp now = now();

        if (now.physical > in.physical) {
            return;
        }

        this.lastPhysical = in.physical;
        this.nextLogical = in.logical + 1;
    }
}
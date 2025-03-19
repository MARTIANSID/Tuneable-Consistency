package org.example.Utility;

import org.ds.paxos.TimeStampProto;

import java.util.Objects;

public class HybridClock {

    public static class TimeStamp implements Comparable<TimeStamp> {
        public long physical;
        public long logical;

        public TimeStamp(long p, long l) {
            this.physical = p;
            this.logical = l;
        }

        public static TimeStampProto convertToProto(TimeStamp timeStamp) {
            return TimeStampProto.newBuilder().setP(timeStamp.physical).setL(timeStamp.logical).build();
        }

        public static TimeStamp convertToTimeStamp(TimeStampProto timeStamp) {
            return new TimeStamp(timeStamp.getP(), timeStamp.getL());
        }

        @Override
        public String toString() {
            return physical + " " + logical;
        }

        @Override
        public int compareTo(TimeStamp other) {
            // First compare physical time
            if (this.physical != other.physical) {
                return Long.compare(this.physical, other.physical);
            }
            // If physical times are equal, compare logical time
            return Long.compare(this.logical, other.logical);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true; // Same object reference
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false; // Different class or null object
            }
            TimeStamp other = (TimeStamp) obj;
            return this.physical == other.physical && this.logical == other.logical;
        }

        @Override
        public int hashCode() {
            return Objects.hash(physical, logical); // Create hash code based on physical and logical
        }
    }

    static class PhysicalClock {
        long physical;

        public PhysicalClock() {
        }

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

    public void update(TimeStamp in) {
        TimeStamp now = now();

        if (now.physical > in.physical) {
            return;
        }

        this.lastPhysical = in.physical;
        this.nextLogical = Math.max(in.logical, now.logical) + 1;
    }
}
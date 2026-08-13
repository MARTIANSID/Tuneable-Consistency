package org.example.Utility;

import org.example.raft.TimeStampProto;

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
//    public void update(TimeStamp in) {
//        // If remote physical < local physical, ignore (local is ahead).
//        if (in.physical < lastPhysical) {
//            return;
//        }
//
//        if (in.physical > lastPhysical) {
//            // Adopt remote physical; set logical to remote.logical + 1
//            lastPhysical = in.physical;
//            nextLogical = in.logical + 1;
//        } else {
//            // Equal physical: set logical to max(localLogical, remoteLogical) + 1
//            // Note: nextLogical is the next value to be emitted by now() when physical is tied.
//            nextLogical = Math.max(nextLogical, in.logical) + 1;
//        }
//    }

    // some test cases
    public static void main(String[] args) {
        HybridClock clock = new HybridClock();
        // Test 1: Basic Functionality
        TimeStamp ts1 = clock.now();
        TimeStamp ts2 = clock.now();
        System.out.println("Test 1: Basic Functionality");
        System.out.println("Timestamp 1: " + ts1);
        System.out.println("Timestamp 2: " + ts2);
        System.out.println("Timestamps should be ordered correctly: " + (ts1.compareTo(ts2) < 0));
        System.out.println();
        // Test 2: Logical Counter Increment
        TimeStamp ts3 = clock.now();
        TimeStamp ts4 = clock.now();
        System.out.println("Test 2: Logical Counter Increment");
        System.out.println("Timestamp 3: " + ts3);
        System.out.println("Timestamp 4: " + ts4);
        System.out.println("Logical counter should increment: " + (ts4.logical == ts3.logical + 1));
        System.out.println();
        // Test 3: Update Method
        TimeStamp ts5 = clock.now();
        TimeStamp ts6 = new TimeStamp(ts5.physical, ts5.logical + 5);
        clock.update(ts6);
        TimeStamp ts7 = clock.now();
        System.out.println("Test 3: Update Method");
        System.out.println("Timestamp 5: " + ts5);
        System.out.println("Timestamp 6: " + ts6);
        System.out.println("Timestamp 7: " + ts7);
        System.out.println("Logical counter should update correctly: " + (ts7.logical == ts6.logical + 1));
    }
}

package dev.ledgertable.harness;

import dev.ledgertable.fault.FaultInjectingObjectStore;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.fault.SimulatedCrash;
import dev.ledgertable.fault.StorageFault;
import dev.ledgertable.store.ObjectStore;

import java.util.Random;

/**
 * Random faults for one simulated process, drawn from a seeded generator so a process's own
 * fault schedule is reproducible from the run seed (thread interleavings are not).
 *
 * @param crashRate probability of dying at each crash point and at each storage operation
 * @param errorRate probability that a storage operation fails, half before and half after taking effect
 */
public record FaultPlan(double crashRate, double errorRate) {
    public static final FaultPlan NONE = new FaultPlan(0, 0);

    /** Faults for one process incarnation; both hooks share one generator. */
    public Process incarnation(long seed) {
        return new Process(new Random(seed));
    }

    public final class Process {
        private final Random rng;

        private Process(Random rng) {
            this.rng = rng;
        }

        public Faults crashPoints() {
            return point -> {
                if (draw(crashRate)) {
                    throw new SimulatedCrash(point.name());
                }
            };
        }

        public ObjectStore wrap(ObjectStore store) {
            if (crashRate == 0 && errorRate == 0) {
                return store;
            }
            return new FaultInjectingObjectStore(store, (op, key) -> {
                if (draw(errorRate)) {
                    return draw(0.5) ? StorageFault.ERROR_BEFORE : StorageFault.ERROR_AFTER;
                }
                if (draw(crashRate / 4)) {
                    return draw(0.5) ? StorageFault.CRASH_BEFORE : StorageFault.CRASH_AFTER;
                }
                return StorageFault.NONE;
            });
        }

        private boolean draw(double p) {
            if (p <= 0) {
                return false;
            }
            synchronized (rng) {
                return rng.nextDouble() < p;
            }
        }
    }
}

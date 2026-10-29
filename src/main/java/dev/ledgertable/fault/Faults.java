package dev.ledgertable.fault;

/** Hook the write path calls at every {@link CrashPoint}; fault injectors throw {@link SimulatedCrash} from here. */
@FunctionalInterface
public interface Faults {
    Faults NONE = point -> {
    };

    void at(CrashPoint point);
}

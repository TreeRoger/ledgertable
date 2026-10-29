package dev.ledgertable.fault;

/**
 * Models the process dying. It is an {@link Error} so that no code in the write path can catch it
 * and tidy up: whatever the store holds at the moment it is thrown is what a real crash would leave behind.
 */
public final class SimulatedCrash extends Error {
    public SimulatedCrash(String where) {
        super("simulated crash at " + where, null, false, false);
    }
}

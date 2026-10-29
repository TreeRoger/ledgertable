package dev.ledgertable.store;

import org.junit.jupiter.api.Assumptions;

import java.util.UUID;

/** Runs the store contract against an S3-compatible service; set {@code LEDGER_S3_ENDPOINT} (e.g. MinIO) to enable. */
class S3ObjectStoreTest extends ObjectStoreContract {
    @Override
    protected ObjectStore createStore() {
        S3Config config = S3Config.fromEnv("contract-test/" + UUID.randomUUID());
        Assumptions.assumeTrue(config != null, "LEDGER_S3_ENDPOINT not set");
        return S3ObjectStore.connect(config);
    }
}

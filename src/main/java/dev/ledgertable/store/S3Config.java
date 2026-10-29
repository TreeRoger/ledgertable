package dev.ledgertable.store;

/** Connection settings for an S3-compatible endpoint. */
public record S3Config(String endpoint, String region, String accessKey, String secretKey, String bucket, String prefix) {

    /**
     * Reads {@code LEDGER_S3_ENDPOINT}, {@code LEDGER_S3_ACCESS_KEY}, {@code LEDGER_S3_SECRET_KEY},
     * {@code LEDGER_S3_BUCKET} and optionally {@code LEDGER_S3_REGION}. Returns null when no endpoint is configured.
     */
    public static S3Config fromEnv(String prefix) {
        String endpoint = System.getenv("LEDGER_S3_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        return new S3Config(endpoint,
                System.getenv().getOrDefault("LEDGER_S3_REGION", "us-east-1"),
                System.getenv().getOrDefault("LEDGER_S3_ACCESS_KEY", "minioadmin"),
                System.getenv().getOrDefault("LEDGER_S3_SECRET_KEY", "minioadmin"),
                System.getenv().getOrDefault("LEDGER_S3_BUCKET", "ledger"),
                prefix);
    }
}

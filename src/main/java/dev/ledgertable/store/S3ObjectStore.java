package dev.ledgertable.store;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Object store over an S3-compatible service. {@link #putIfAbsent} uses a conditional write
 * ({@code If-None-Match: *}): the service returns 412 if the key exists, which is the same
 * single-winner guarantee the local store gets from {@code link(2)}.
 */
public final class S3ObjectStore implements ObjectStore {
    private static final int PRECONDITION_FAILED = 412;
    /** Returned when a concurrent conditional write to the same key is in flight; the request had no effect. */
    private static final int CONDITIONAL_CONFLICT = 409;
    private static final int MAX_CONFLICT_RETRIES = 20;

    private final S3Client s3;
    private final String bucket;
    private final String prefix;

    public S3ObjectStore(S3Client s3, String bucket, String prefix) {
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
    }

    /** Connects to an S3-compatible endpoint such as MinIO using static credentials and path-style addressing. */
    public static S3ObjectStore connect(S3Config config) {
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create(config.endpoint()))
                .region(Region.of(config.region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.accessKey(), config.secretKey())))
                .forcePathStyle(true)
                .httpClient(UrlConnectionHttpClient.create())
                .build();
        try {
            client.headBucket(b -> b.bucket(config.bucket()));
        } catch (NoSuchBucketException e) {
            try {
                client.createBucket(b -> b.bucket(config.bucket()));
            } catch (BucketAlreadyOwnedByYouException raced) {
                // Another client created it first.
            }
        }
        return new S3ObjectStore(client, config.bucket(), config.prefix());
    }

    @Override
    public boolean putIfAbsent(String key, byte[] data) throws IOException {
        for (int attempt = 0; ; attempt++) {
            try {
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(prefix + key).ifNoneMatch("*").build(),
                        RequestBody.fromBytes(data));
                return true;
            } catch (S3Exception e) {
                if (e.statusCode() == PRECONDITION_FAILED) {
                    return false;
                }
                if (e.statusCode() == CONDITIONAL_CONFLICT && attempt < MAX_CONFLICT_RETRIES) {
                    continue;
                }
                throw new IOException("putIfAbsent " + key, e);
            } catch (RuntimeException e) {
                throw new IOException("putIfAbsent " + key, e);
            }
        }
    }

    @Override
    public void put(String key, byte[] data) throws IOException {
        try {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(prefix + key).build(), RequestBody.fromBytes(data));
        } catch (RuntimeException e) {
            throw new IOException("put " + key, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        try {
            return Optional.of(s3.getObjectAsBytes(b -> b.bucket(bucket).key(prefix + key)).asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (RuntimeException e) {
            throw new IOException("get " + key, e);
        }
    }

    @Override
    public List<String> list(String keyPrefix, String startAfter) throws IOException {
        try {
            var request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix + keyPrefix);
            if (!startAfter.isEmpty()) {
                request.startAfter(prefix + startAfter);
            }
            return s3.listObjectsV2Paginator(request.build()).contents().stream()
                    .map(S3Object::key)
                    .map(k -> k.substring(prefix.length()))
                    .sorted()
                    .toList();
        } catch (RuntimeException e) {
            throw new IOException("list " + keyPrefix, e);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            s3.deleteObject(b -> b.bucket(bucket).key(prefix + key));
        } catch (RuntimeException e) {
            throw new IOException("delete " + key, e);
        }
    }

    @Override
    public void close() {
        s3.close();
    }
}

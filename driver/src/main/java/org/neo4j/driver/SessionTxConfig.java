package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public record SessionTxConfig(
        TxClusterMemberAccess clusterMemberAccess,
        java.time.Duration timeout,
        Map<String, Object> metadata,
        Integer maxRetries,
        Function<RetryInfo, RetryDelay> retryFunction) {

    public static SessionTxConfigBuilder builder() {
        return new SessionTxConfigBuilder();
    }

    public static SessionTxConfigBuilder builder(SessionTxConfig from) {
        return new SessionTxConfigBuilder(from);
    }

    public TransactionConfig transactionConfig() {
        var txCfgBuilder = TransactionConfig.builder();

        if (this.metadata() != null) {
            txCfgBuilder.withMetadata(this.metadata());
        }

        if (this.timeout() != null) {
            txCfgBuilder.withTimeout(this.timeout());
        }

        return txCfgBuilder.build();
    }

    public static final SessionTxConfig read = new SessionTxConfig(
            TxClusterMemberAccess.Readers,
            Duration.ofDays(1),
            Map.of(),
            2,
            Retries.transientFunctions);

    public static final SessionTxConfig write = new SessionTxConfig(
            TxClusterMemberAccess.Writers, Duration.ofDays(1), Map.of(), 2, Retries.transientFunctions);


    public Optional<IllegalStateException> validate() {
        if (metadata == null) {
            return Optional.of(new IllegalStateException("Metadata for transaction should not be null."));
        }

        if (maxRetries < 0) {
            return Optional.of(new IllegalStateException("Can not define negative maximum retries."));
        }

        return Optional.empty();
    }
}

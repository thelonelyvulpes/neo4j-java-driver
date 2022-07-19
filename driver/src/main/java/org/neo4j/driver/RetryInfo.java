package org.neo4j.driver;

public record RetryInfo(Exception exception, Integer attempts, Integer maxRetry) {

}

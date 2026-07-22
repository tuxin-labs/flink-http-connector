package com.getindata.connectors.http.internal.table.scan.source;

import java.time.Duration;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

/**
 * 从 scan 的 {@link ReadableConfig} 构造 resilience4j {@link RetryConfig}。
 *
 * <p>镜像 lookup 的 RetryConfigProvider，但读 scan 专属 ConfigOption（因 RetryConfigProvider
 * 与 lookup 的 ConfigOption 绑定，无法直接复用）。
 */
public final class HttpScanRetryConfigProvider {

    private HttpScanRetryConfigProvider() {
    }

    public static RetryConfig create(ReadableConfig config) {
        String strategy = config.get(HttpScanConnectorOptions.RETRY_STRATEGY_TYPE);
        int maxRetries = config.get(HttpScanConnectorOptions.MAX_RETRIES);
        RetryConfig.Builder<?> builder;
        if ("exponential-delay".equalsIgnoreCase(strategy)) {
            Duration initial = config.get(HttpScanConnectorOptions.RETRY_EXP_INITIAL_BACKOFF);
            Duration max = config.get(HttpScanConnectorOptions.RETRY_EXP_MAX_BACKOFF);
            double mult = config.get(HttpScanConnectorOptions.RETRY_EXP_MULTIPLIER);
            builder = RetryConfig.custom()
                .intervalFunction(IntervalFunction.ofExponentialBackoff(initial, mult, max));
        } else {
            Duration delay = config.get(HttpScanConnectorOptions.RETRY_FIXED_DELAY);
            builder = RetryConfig.custom().intervalFunction(IntervalFunction.of(delay));
        }
        return builder.maxAttempts(maxRetries + 1).build();
    }
}

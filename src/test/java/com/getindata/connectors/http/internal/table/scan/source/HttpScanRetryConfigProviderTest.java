package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class HttpScanRetryConfigProviderTest {

    @Test
    void shouldBuildFixedDelay() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.MAX_RETRIES, 3);
        assertThat(HttpScanRetryConfigProvider.create(conf)).isNotNull();
    }

    @Test
    void shouldBuildExponentialDelay() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.RETRY_STRATEGY_TYPE, "exponential-delay");
        conf.set(HttpScanConnectorOptions.MAX_RETRIES, 2);
        assertThat(HttpScanRetryConfigProvider.create(conf)).isNotNull();
    }
}

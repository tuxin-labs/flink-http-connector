package com.getindata.connectors.http.internal.table.scan.source;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class HttpScanHttpClientFactoryTest {

    @Test
    void shouldCreateClientWithProxyAndIgnoredCodes() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        conf.set(HttpScanConnectorOptions.PROXY_HOST, "proxy.local");
        conf.set(HttpScanConnectorOptions.PROXY_PORT, 8080);
        conf.set(HttpScanConnectorOptions.IGNORED_RESPONSE_CODES, "404");
        conf.set(HttpScanConnectorOptions.RETRY_CODES, "5XX");
        var cfg = HttpScanConfig.from(conf, new Properties());

        HttpClientWithRetry client = HttpScanHttpClientFactory.create(cfg);
        assertThat(client).isNotNull();
    }

    @Test
    void shouldCreateClientWithMinimalConfig() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var cfg = HttpScanConfig.from(conf, new Properties());

        assertThat(HttpScanHttpClientFactory.create(cfg)).isNotNull();
    }

    @Test
    void shouldCreateClientWithProxyHostOnlyAndBlankRetryCodes() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        conf.set(HttpScanConnectorOptions.PROXY_HOST, "proxy.local");
        // 仅 host 无 port -> 不配置代理（覆盖 && 短路分支）
        conf.set(HttpScanConnectorOptions.RETRY_CODES, "");
        conf.set(HttpScanConnectorOptions.IGNORED_RESPONSE_CODES, "");
        var cfg = HttpScanConfig.from(conf, new Properties());

        assertThat(HttpScanHttpClientFactory.create(cfg)).isNotNull();
    }
}

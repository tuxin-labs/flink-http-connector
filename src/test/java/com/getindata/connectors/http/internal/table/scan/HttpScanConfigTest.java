package com.getindata.connectors.http.internal.table.scan;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HttpScanConfigTest {

    @Test
    void shouldBuildFromReadableConfig() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/{cid}/orders");
        conf.set(HttpScanConnectorOptions.URL_VARS, "cid:C1");
        conf.set(HttpScanConnectorOptions.METHOD, "POST");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        conf.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        conf.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, 100);

        var cfg = HttpScanConfig.from(conf, new Properties());
        assertThat(cfg.getUrl()).isEqualTo("https://x/{cid}/orders");
        assertThat(cfg.getMethod()).isEqualTo("POST");
        assertThat(cfg.getUrlVars()).containsEntry("cid", "C1");
        assertThat(cfg.getPaginationType()).isEqualTo("page-number");
        assertThat(cfg.getBatchSize()).isEqualTo(100);
        assertThat(cfg.getQueryParamsTemplate()).isEqualTo("page=${page}");
    }

    @Test
    void shouldApplyDefaults() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());
        assertThat(cfg.getMethod()).isEqualTo("GET");
        assertThat(cfg.getPaginationType()).isEqualTo("none");
        assertThat(cfg.getBatchSize()).isNull();
        assertThat(cfg.getTotalPages()).isNull();
        assertThat(cfg.getUrlVars()).isEmpty();
        assertThat(cfg.getBodyContentType()).isEqualTo("application/json");
    }

    @Test
    void shouldCarryProperties() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var props = new Properties();
        props.setProperty("gid.connector.http.security.cert.server", "/tmp/cert.pem");
        var cfg = HttpScanConfig.from(conf, props);
        assertThat(cfg.getProperties())
            .containsEntry("gid.connector.http.security.cert.server", "/tmp/cert.pem");
    }
}

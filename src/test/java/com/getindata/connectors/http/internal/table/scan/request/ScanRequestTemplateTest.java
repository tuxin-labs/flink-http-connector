package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class ScanRequestTemplateTest {

    @Test
    void shouldBuildGetWithQueryParamsAndPathVars() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/{cid}/orders");
        conf.set(HttpScanConnectorOptions.URL_VARS, "cid:C1");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}&size=100");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of("page", "3"));
        assertThat(req.uri()).isEqualTo(new URI("https://x/C1/orders?page=3&size=100"));
        assertThat(req.method()).isEqualTo("GET");
    }

    @Test
    void shouldBuildPostWithBody() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/search");
        conf.set(HttpScanConnectorOptions.METHOD, "POST");
        conf.set(HttpScanConnectorOptions.BODY, "{\"page\":${page}}");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of("page", "2"));
        assertThat(req.method()).isEqualTo("POST");
        assertThat(req.bodyPublisher()).isPresent();
        assertThat(req.headers().firstValue("Content-Type")).hasValue("application/json");
    }

    @Test
    void shouldEncodeQueryValueWithSpaces() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "q=${q}");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of("q", "a b"));
        assertThat(req.uri()).isEqualTo(new URI("https://x?q=a+b"));
    }

    @Test
    void shouldBuildGetWithoutQueryWhenNotConfigured() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of());
        assertThat(req.uri()).isEqualTo(URI.create("https://x/items"));
        assertThat(req.method()).isEqualTo("GET");
        assertThat(req.bodyPublisher()).isEmpty();
    }
}

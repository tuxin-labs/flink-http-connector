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

    @Test
    void shouldAppendQueryWithAmpersandWhenUrlAlreadyHasQuery() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items?fixed=1");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of("page", "2"));
        assertThat(req.uri()).isEqualTo(new URI("https://x/items?fixed=1&page=2"));
    }

    @Test
    void shouldSkipEmptyQueryPairs() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}&&size=10");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of("page", "1"));
        // 中间空 pair 被跳过
        assertThat(req.uri().getQuery()).isEqualTo("page=1&size=10");
    }

    @Test
    void shouldBuildPostWithoutBody() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        conf.set(HttpScanConnectorOptions.METHOD, "POST");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var req = new ScanRequestTemplate(cfg).build(Map.of());
        assertThat(req.method()).isEqualTo("POST");
        assertThat(req.bodyPublisher()).isPresent();
    }

    @Test
    void shouldAddConfiguredHeaders() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var props = new Properties();
        props.setProperty("gid.connector.http.scan.header.X-Custom", "my-value");
        props.setProperty("gid.connector.http.scan.header.Accept", "application/json");
        var cfg = HttpScanConfig.from(conf, props);

        var req = new ScanRequestTemplate(cfg).build(Map.of());
        assertThat(req.headers().firstValue("X-Custom")).hasValue("my-value");
        assertThat(req.headers().firstValue("Accept")).hasValue("application/json");
    }

    @Test
    void shouldAutoEncodeBasicAuthByDefault() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var props = new Properties();
        // 原始值 "user:pass" 应自动编码为 "Basic dXNlcjpwYXNz"
        props.setProperty("gid.connector.http.scan.header.Authorization", "user:pass");
        var cfg = HttpScanConfig.from(conf, props);

        var req = new ScanRequestTemplate(cfg).build(Map.of());
        String authValue = req.headers().firstValue("Authorization").orElseThrow();
        assertThat(authValue).startsWith("Basic ");
        // 解码验证
        String decoded = new String(java.util.Base64.getDecoder()
            .decode(authValue.substring(6)), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(decoded).isEqualTo("user:pass");
    }

    @Test
    void shouldPassThroughRawAuthHeaderWhenConfigured() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        conf.set(HttpScanConnectorOptions.USE_RAW_AUTH_HEADER, true);
        var props = new Properties();
        props.setProperty("gid.connector.http.scan.header.Authorization", "Bearer my-token");
        var cfg = HttpScanConfig.from(conf, props);

        var req = new ScanRequestTemplate(cfg).build(Map.of());
        assertThat(req.headers().firstValue("Authorization")).hasValue("Bearer my-token");
    }
}

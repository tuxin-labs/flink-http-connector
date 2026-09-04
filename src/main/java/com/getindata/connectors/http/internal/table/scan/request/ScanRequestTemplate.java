package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import lombok.RequiredArgsConstructor;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

/**
 * 从 {@link HttpScanConfig} 与分页占位符取值装配最终的 {@link java.net.http.HttpRequest}。
 *
 * <p>装配顺序：
 * <ol>
 *     <li>替换 URL 路径变量 {name} 与分页占位符 ${page}/${cursor}</li>
 *     <li>解析并替换 query-params 模板（先按 &amp; 切分再做占位符替换），对每个值 URL 编码后追加到 URL</li>
 *     <li>注入配置的自定义请求头（gid.connector.http.scan.header.*），含 Basic Auth 自动编码</li>
 *     <li>若配置 body 模板，替换后作为请求体；否则无 body</li>
 *     <li>按 method 装配 GET/POST/PUT，并应用 request.timeout</li>
 * </ol>
 */
@RequiredArgsConstructor
public class ScanRequestTemplate {

    private final HttpScanConfig config;

    public HttpRequest build(Map<String, String> requestValues) {
        String baseUrl = PlaceholderResolver.resolvePathVars(config.getUrl(), config.getUrlVars());
        // URL 同样支持 ${page}/${cursor} 占位符（需在路径变量之后替换，避免 {name} 误匹配 ${name}）
        baseUrl = PlaceholderResolver.replace(baseUrl, requestValues);
        String url = appendQueryParams(baseUrl, requestValues);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(config.getRequestTimeout()));

        // 注入自定义请求头
        addConfiguredHeaders(builder);

        if (config.getBodyTemplate() != null) {
            String body = PlaceholderResolver.replace(config.getBodyTemplate(), requestValues);
            builder.header("Content-Type", config.getBodyContentType());
            builder.method(config.getMethod(), BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        } else if ("GET".equalsIgnoreCase(config.getMethod())) {
            builder.GET();
        } else {
            builder.method(config.getMethod(), BodyPublishers.noBody());
        }
        return builder.build();
    }

    /**
     * 从 Properties 中提取 gid.connector.http.scan.header.* 并注入到请求头。
     * 对 Authorization 头，若 use-raw-authorization-header=false（默认），自动做 Basic Auth Base64 编码。
     */
    private void addConfiguredHeaders(HttpRequest.Builder builder) {
        Properties props = config.getProperties();
        if (props == null) {
            return;
        }
        boolean useRawAuth = config.getReadableConfig()
            .get(HttpScanConnectorOptions.USE_RAW_AUTH_HEADER);

        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = (String) entry.getKey();
            if (!key.startsWith(HttpScanConnectorOptions.HEADER_PREFIX)) {
                continue;
            }
            // 提取 header 名：gid.connector.http.scan.header.Authorization → Authorization
            String headerName = key.substring(HttpScanConnectorOptions.HEADER_PREFIX.length());
            String headerValue = (String) entry.getValue();

            // Basic Auth 自动编码：Authorization 值不是 "Basic " 开头时自动编码
            if ("Authorization".equalsIgnoreCase(headerName) && !useRawAuth) {
                if (!headerValue.startsWith("Basic ")) {
                    headerValue = "Basic "
                        + Base64.getEncoder().encodeToString(
                            headerValue.getBytes(StandardCharsets.UTF_8));
                }
            }
            builder.header(headerName, headerValue);
        }
    }

    private String appendQueryParams(String baseUrl, Map<String, String> requestValues) {
        if (config.getQueryParamsTemplate() == null) {
            return baseUrl;
        }
        // 先按模板结构切分 pair，再对每段做占位符替换与编码，
        // 使占位符值（如 cursor token）中出现的 & / = 不会被误认为分隔符
        StringBuilder sb = new StringBuilder(baseUrl);
        String separator = baseUrl.contains("?") ? "&" : "?";
        for (String pair : config.getQueryParamsTemplate().split("&")) {
            if (pair.isBlank()) {
                continue;
            }
            String[] kv = pair.split("=", 2);
            String key = PlaceholderResolver.encodeQueryValue(
                PlaceholderResolver.replace(kv[0], requestValues));
            String val = kv.length > 1
                ? PlaceholderResolver.encodeQueryValue(
                    PlaceholderResolver.replace(kv[1], requestValues))
                : "";
            sb.append(separator).append(key).append("=").append(val);
            separator = "&";
        }
        return sb.toString();
    }
}

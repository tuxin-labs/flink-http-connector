package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * 从 {@link HttpScanConfig} 与分页占位符取值装配最终的 {@link java.net.http.HttpRequest}。
 *
 * <p>装配顺序：
 * <ol>
 *     <li>替换 URL 路径变量 {name}</li>
 *     <li>解析并替换 query-params 模板，对每个值做 URL 编码后追加到 URL</li>
 *     <li>若配置 body 模板，替换后作为请求体；否则无 body</li>
 *     <li>按 method 装配 GET/POST/PUT</li>
 * </ol>
 */
@RequiredArgsConstructor
public class ScanRequestTemplate {

    private final HttpScanConfig config;

    public HttpRequest build(Map<String, String> requestValues) {
        String baseUrl = PlaceholderResolver.resolvePathVars(config.getUrl(), config.getUrlVars());
        String url = appendQueryParams(baseUrl, requestValues);

        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url));

        if (config.getBodyTemplate() != null) {
            String body = PlaceholderResolver.replace(config.getBodyTemplate(), requestValues);
            builder.header("Content-Type", config.getBodyContentType());
            builder.method(config.getMethod(), BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        } else if ("GET".equalsIgnoreCase(config.getMethod())) {
            builder.GET();
        } else {
            // POST/PUT 无 body（少见但允许）
            builder.method(config.getMethod(), BodyPublishers.noBody());
        }
        return builder.build();
    }

    private String appendQueryParams(String baseUrl, Map<String, String> requestValues) {
        if (config.getQueryParamsTemplate() == null) {
            return baseUrl;
        }
        String resolved = PlaceholderResolver.replace(config.getQueryParamsTemplate(), requestValues);
        StringBuilder sb = new StringBuilder(baseUrl);
        String separator = baseUrl.contains("?") ? "&" : "?";
        for (String pair : resolved.split("&")) {
            if (pair.isBlank()) {
                continue;
            }
            String[] kv = pair.split("=", 2);
            String key = kv[0];
            String val = kv.length > 1 ? PlaceholderResolver.encodeQueryValue(kv[1]) : "";
            sb.append(separator).append(key).append("=").append(val);
            separator = "&";
        }
        return sb.toString();
    }
}

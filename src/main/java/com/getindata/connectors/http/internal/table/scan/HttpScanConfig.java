package com.getindata.connectors.http.internal.table.scan;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import lombok.Builder;
import lombok.Value;
import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.table.scan.request.PlaceholderResolver;

/**
 * http-scan 运行期不可变配置，由 {@link HttpScanTableSourceFactory} 通过 {@link #from} 构造。
 *
 * <p>{@link #urlVars} 在构造期预解析为 Map，避免运行期重复解析。
 * {@link #properties} 携带所有 gid.connector.http.* 原始键值，供 security/auth/retry/status 复用。
 */
@Value
@Builder
public class HttpScanConfig implements Serializable {

    String url;
    String method;
    Map<String, String> urlVars;
    String queryParamsTemplate;
    String bodyTemplate;
    String bodyContentType;
    String contentField;

    String paginationType;
    String pageField;
    int startPage;
    Integer batchSize;
    Integer totalPages;
    String totalCountJsonPath;
    String hasMoreJsonPath;
    String cursorField;
    String cursorResponseJsonPath;
    String initialCursor;

    Properties properties;
    ReadableConfig readableConfig;

    public static HttpScanConfig from(ReadableConfig readable, Properties properties) {
        return HttpScanConfig.builder()
            .url(readable.get(HttpScanConnectorOptions.URL))
            .method(readable.get(HttpScanConnectorOptions.METHOD))
            .urlVars(PlaceholderResolver.parseUrlVars(readable.get(HttpScanConnectorOptions.URL_VARS)))
            .queryParamsTemplate(readable.get(HttpScanConnectorOptions.QUERY_PARAMS))
            .bodyTemplate(readable.get(HttpScanConnectorOptions.BODY))
            .bodyContentType(readable.get(HttpScanConnectorOptions.BODY_CONTENT_TYPE))
            .contentField(readable.get(HttpScanConnectorOptions.CONTENT_FIELD))
            .paginationType(readable.get(HttpScanConnectorOptions.PAGINATION_TYPE))
            .pageField(readable.get(HttpScanConnectorOptions.PAGINATION_PAGE_FIELD))
            .startPage(readable.get(HttpScanConnectorOptions.PAGINATION_START_PAGE))
            .batchSize(readable.getOptional(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE).orElse(null))
            .totalPages(readable.getOptional(HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES).orElse(null))
            .totalCountJsonPath(readable.get(HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH))
            .hasMoreJsonPath(readable.get(HttpScanConnectorOptions.PAGINATION_HAS_MORE_JSONPATH))
            .cursorField(readable.get(HttpScanConnectorOptions.PAGINATION_CURSOR_FIELD))
            .cursorResponseJsonPath(readable.get(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH))
            .initialCursor(readable.get(HttpScanConnectorOptions.PAGINATION_INITIAL_CURSOR))
            .properties(properties)
            .readableConfig(readable)
            .build();
    }
}

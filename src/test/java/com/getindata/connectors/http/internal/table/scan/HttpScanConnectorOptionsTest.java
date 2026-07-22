package com.getindata.connectors.http.internal.table.scan;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HttpScanConnectorOptionsTest {

    @Test
    void shouldDefineCoreOptions() {
        assertThat(HttpScanConnectorOptions.URL.key()).isEqualTo("url");
        assertThat(HttpScanConnectorOptions.METHOD.key()).isEqualTo("method");
        assertThat(HttpScanConnectorOptions.METHOD.defaultValue()).isEqualTo("GET");
        assertThat(HttpScanConnectorOptions.QUERY_PARAMS.key())
            .isEqualTo("gid.connector.http.scan.query-params");
        assertThat(HttpScanConnectorOptions.URL_VARS.key())
            .isEqualTo("gid.connector.http.scan.url-vars");
    }

    @Test
    void shouldDefinePaginationOptions() {
        assertThat(HttpScanConnectorOptions.PAGINATION_TYPE.key())
            .isEqualTo("gid.connector.http.scan.pagination.type");
        assertThat(HttpScanConnectorOptions.PAGINATION_TYPE.defaultValue()).isEqualTo("none");
        assertThat(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE.key())
            .isEqualTo("gid.connector.http.scan.pagination.batch-size");
        assertThat(HttpScanConnectorOptions.PAGINATION_START_PAGE.defaultValue()).isEqualTo(1);
        assertThat(HttpScanConnectorOptions.PAGINATION_PAGE_FIELD.defaultValue()).isEqualTo("page");
    }

    @Test
    void shouldDefineBodyAndContentOptions() {
        assertThat(HttpScanConnectorOptions.BODY.key())
            .isEqualTo("gid.connector.http.scan.body");
        assertThat(HttpScanConnectorOptions.CONTENT_FIELD.key())
            .isEqualTo("gid.connector.http.scan.content-field");
        assertThat(HttpScanConnectorOptions.BODY_CONTENT_TYPE.defaultValue())
            .isEqualTo("application/json");
    }

    @Test
    void shouldDefineHttpTransportOptions() {
        assertThat(HttpScanConnectorOptions.REQUEST_TIMEOUT.defaultValue()).isEqualTo(30);
        assertThat(HttpScanConnectorOptions.MAX_RETRIES.defaultValue()).isEqualTo(3);
        assertThat(HttpScanConnectorOptions.SUCCESS_CODES.defaultValue()).isEqualTo("2XX");
        assertThat(HttpScanConnectorOptions.RETRY_STRATEGY_TYPE.defaultValue())
            .isEqualTo("fixed-delay");
    }
}

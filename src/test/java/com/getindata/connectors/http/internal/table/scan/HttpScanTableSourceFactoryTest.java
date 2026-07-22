package com.getindata.connectors.http.internal.table.scan;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpScanTableSourceFactoryTest {

    private final HttpScanTableSourceFactory factory = new HttpScanTableSourceFactory();

    private Configuration conf() {
        var c = new Configuration();
        c.set(HttpScanConnectorOptions.URL, "https://x/items");
        return c;
    }

    private void validate(Configuration c) {
        factory.validateHttpScanOptions(c);
    }

    @Test
    void shouldAcceptValidNoPaginationConfig() {
        assertThatCode(() -> validate(conf())).doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptValidPageNumberConfig() {
        var c = conf();
        c.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        c.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, 100);
        assertThatCode(() -> validate(c)).doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptValidCursorConfig() {
        var c = conf();
        c.set(HttpScanConnectorOptions.QUERY_PARAMS, "cursor=${cursor}");
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "cursor");
        c.set(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH, "$.next");
        assertThatCode(() -> validate(c)).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectInvalidMethod() {
        var c = conf();
        c.set(HttpScanConnectorOptions.METHOD, "DELETE");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported HTTP method");
    }

    @Test
    void shouldRejectGetWithBody() {
        var c = conf();
        c.set(HttpScanConnectorOptions.METHOD, "GET");
        c.set(HttpScanConnectorOptions.BODY, "{\"x\":1}");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("GET method cannot have a body");
    }

    @Test
    void shouldRejectInvalidPaginationType() {
        var c = conf();
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "offset");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported pagination.type");
    }

    @Test
    void shouldRejectCursorWithoutResponseJsonPath() {
        var c = conf();
        c.set(HttpScanConnectorOptions.QUERY_PARAMS, "cursor=${cursor}");
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "cursor");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cursor-response-jsonpath is required");
    }

    @Test
    void shouldRejectPageNumberWithoutPlaceholder() {
        var c = conf();
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        c.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, 100);
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("${page} placeholder");
    }

    @Test
    void shouldRejectCursorWithoutPlaceholder() {
        var c = conf();
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "cursor");
        c.set(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH, "$.next");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("${cursor} placeholder");
    }

    @Test
    void shouldRejectNoneWithPagePlaceholder() {
        var c = conf();
        c.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not allow ${page}");
    }

    @Test
    void shouldRejectUrlVarMissingValue() {
        var c = conf();
        c.set(HttpScanConnectorOptions.URL, "https://x/{cid}/items");
        // url-vars 未提供 cid 取值
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("has no value in url-vars");
    }

    @Test
    void shouldRejectUrlVarNotUsedInUrl() {
        var c = conf();
        c.set(HttpScanConnectorOptions.URL, "https://x/items");
        c.set(HttpScanConnectorOptions.URL_VARS, "unused:val");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not used in url");
    }

    @Test
    void shouldRejectInvalidJsonPath() {
        var c = conf();
        c.set(HttpScanConnectorOptions.CONTENT_FIELD, "badpath");
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid JSONPath");
    }

    @Test
    void shouldRejectNegativeStartPage() {
        var c = conf();
        c.set(HttpScanConnectorOptions.PAGINATION_START_PAGE, -1);
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("start-page must be >= 0");
    }

    @Test
    void shouldRejectZeroBatchSize() {
        var c = conf();
        c.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        c.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        c.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, 0);
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("batch-size must be > 0");
    }

    @Test
    void shouldRejectNegativeMaxRetries() {
        var c = conf();
        c.set(HttpScanConnectorOptions.MAX_RETRIES, -1);
        assertThatThrownBy(() -> validate(c))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("max-retries must be >= 0");
    }

    @Test
    void shouldHaveHttpScanFactoryIdentifier() {
        assertThat(factory.factoryIdentifier()).isEqualTo("http-scan");
    }

    @Test
    void shouldDeclareRequiredAndOptionalOptions() {
        assertThat(factory.requiredOptions()).contains(HttpScanConnectorOptions.URL);
        assertThat(factory.optionalOptions()).contains(HttpScanConnectorOptions.PAGINATION_TYPE);
        assertThat(factory.optionalOptions()).contains(HttpScanConnectorOptions.CONTENT_FIELD);
    }
}

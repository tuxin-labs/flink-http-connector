package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class PageNumberPaginationTest {

    private final PageNumberPagination strategy = new PageNumberPagination();

    private HttpScanConfig cfg(Integer batchSize, Integer totalPages, String totalCount, String hasMore) {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        conf.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        if (batchSize != null) {
            conf.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, batchSize);
        }
        if (totalPages != null) {
            conf.set(HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES, totalPages);
        }
        if (totalCount != null) {
            conf.set(HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH, totalCount);
        }
        if (hasMore != null) {
            conf.set(HttpScanConnectorOptions.PAGINATION_HAS_MORE_JSONPATH, hasMore);
        }
        return HttpScanConfig.from(conf, new Properties());
    }

    @Test
    void shouldIncrementPageNumber() {
        var c = cfg(null, null, null, null);
        var st = strategy.initialState(c);
        assertThat(strategy.nextRequestValues(st, c).get()).containsEntry("page", "1");
        var after = strategy.afterResponse(st, "[]", 100, c);
        assertThat(strategy.nextRequestValues(after.getNextState(), c).get()).containsEntry("page", "2");
    }

    @Test
    void shouldStopOnBatchSize() {
        var c = cfg(100, null, null, null);
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "[]", 50, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldContinueWhenBatchSizeMet() {
        var c = cfg(100, null, null, null);
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "[]", 100, c);
        assertThat(after.isShouldStop()).isFalse();
    }

    @Test
    void shouldStopOnTotalPages() {
        var c = cfg(null, 2, null, null);
        var st = strategy.initialState(c);
        // 第1页后未停
        var after1 = strategy.afterResponse(st, "[]", 100, c);
        assertThat(after1.isShouldStop()).isFalse();
        // 第2页后停止（requestCount==2>=2）
        var after2 = strategy.afterResponse(after1.getNextState(), "[]", 100, c);
        assertThat(after2.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopOnTotalCount() {
        var c = cfg(null, null, "$.total", null);
        var st = strategy.initialState(c);
        var after1 = strategy.afterResponse(st, "{\"total\":150}", 100, c);
        assertThat(after1.isShouldStop()).isFalse(); // 累计100 < 150
        var after2 = strategy.afterResponse(after1.getNextState(), "{\"total\":150}", 50, c);
        assertThat(after2.isShouldStop()).isTrue(); // 累计150 >= 150
    }

    @Test
    void shouldStopOnHasMoreFalse() {
        var c = cfg(null, null, null, "$.hasMore");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{\"hasMore\":false}", 100, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopOnHasMoreMissing() {
        var c = cfg(null, null, null, "$.hasMore");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{}", 100, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldContinueWhenHasMoreTrue() {
        var c = cfg(null, null, null, "$.hasMore");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{\"hasMore\":true}", 100, c);
        assertThat(after.isShouldStop()).isFalse();
    }
}

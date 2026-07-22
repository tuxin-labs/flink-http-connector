package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class CursorPaginationTest {

    private final CursorPagination strategy = new CursorPagination();

    private HttpScanConfig cfg(String initial, String respPath) {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        conf.set(HttpScanConnectorOptions.PAGINATION_TYPE, "cursor");
        conf.set(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH, respPath);
        conf.set(HttpScanConnectorOptions.PAGINATION_INITIAL_CURSOR, initial);
        return HttpScanConfig.from(conf, new Properties());
    }

    @Test
    void shouldContinueUntilCursorEmpty() {
        var c = cfg("", "$.next");
        var st = strategy.initialState(c);
        assertThat(strategy.nextRequestValues(st, c).get()).containsEntry("cursor", "");

        var after1 = strategy.afterResponse(st, "{\"next\":\"page2\"}", 10, c);
        assertThat(after1.isShouldStop()).isFalse();
        assertThat(after1.getNextState().getCursor()).isEqualTo("page2");

        var after2 = strategy.afterResponse(after1.getNextState(), "{\"next\":\"\"}", 10, c);
        assertThat(after2.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopWhenCursorMissing() {
        var c = cfg("", "$.next");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{}", 10, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopWhenCursorNull() {
        var c = cfg("", "$.next");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{\"next\":null}", 10, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldNotProduceRequestAfterStop() {
        var c = cfg("", "$.next");
        var st = strategy.initialState(c);
        var after = strategy.afterResponse(st, "{}", 10, c);
        // 已停止后 cursor 为空 → 不再产出请求
        assertThat(strategy.nextRequestValues(after.getNextState(), c)).isEmpty();
    }
}

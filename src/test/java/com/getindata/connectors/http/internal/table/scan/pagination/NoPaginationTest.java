package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class NoPaginationTest {

    @Test
    void shouldProduceOneRequestThenStop() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var strategy = new NoPagination();
        var state = strategy.initialState(cfg);
        assertThat(strategy.nextRequestValues(state, cfg)).isPresent();

        var after = strategy.afterResponse(state, "[{\"id\":1}]", 1, cfg);
        assertThat(after.isShouldStop()).isTrue();
        assertThat(strategy.nextRequestValues(after.getNextState(), cfg)).isEmpty();
    }
}

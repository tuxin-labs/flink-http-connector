package com.getindata.connectors.http.internal.table.scan.source;

import java.util.Properties;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class HttpScanSourceTest {

    private HttpScanConfig cfg() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        return HttpScanConfig.from(conf, new Properties());
    }

    @Test
    void shouldBeBounded() {
        var source = new HttpScanSource(cfg(), null);
        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCreateEnumerator() {
        var source = new HttpScanSource(cfg(), null);
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        assertThat(source.createEnumerator(ctx)).isNotNull();
    }

    @Test
    void shouldProvideSerializers() {
        var source = new HttpScanSource(cfg(), null);
        assertThat(source.getSplitSerializer()).isNotNull();
        assertThat(source.getEnumeratorCheckpointSerializer()).isNotNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRestoreEnumeratorForCheckpointFailover() {
        var source = new HttpScanSource(cfg(), null);
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        // v1 无断点续传：failover 恢复为重新创建 enumerator 从头扫描，而不是抛异常
        assertThat(source.restoreEnumerator(ctx, null)).isNotNull();
    }
}

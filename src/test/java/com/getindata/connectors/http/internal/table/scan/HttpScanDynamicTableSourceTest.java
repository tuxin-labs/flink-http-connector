package com.getindata.connectors.http.internal.table.scan;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;

class HttpScanDynamicTableSourceTest {

    private HttpScanDynamicTableSource source() {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());
        DataType rowType = DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()));
        DecodingFormat<?> format = Mockito.mock(DecodingFormat.class);
        return new HttpScanDynamicTableSource(cfg, (DecodingFormat) format, rowType);
    }

    @Test
    void shouldReportInsertOnlyChangelog() {
        assertThat(source().getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void shouldNotSupportNestedProjection() {
        assertThat(source().supportsNestedProjection()).isFalse();
    }

    @Test
    void shouldBeCopyable() {
        var s = source();
        var copy = s.copy();
        assertThat(copy).isInstanceOf(HttpScanDynamicTableSource.class);
        assertThat(copy.asSummaryString()).isEqualTo("HttpScanDynamicTableSource");
    }

    @Test
    void shouldImplementScanTableSourceAndProjectionPushDown() {
        var s = source();
        assertThat(s).isInstanceOf(ScanTableSource.class);
        assertThat(s).isInstanceOf(SupportsProjectionPushDown.class);
    }

    @Test
    void shouldApplyProjection() {
        var s = source();
        // 投影只取第 1 列（name）
        s.applyProjection(new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())));
        // copy 保留投影后的 schema
        var copy = (HttpScanDynamicTableSource) s.copy();
        assertThat(copy.asSummaryString()).isEqualTo("HttpScanDynamicTableSource");
    }
}

package com.getindata.connectors.http.internal.table.scan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.getindata.connectors.http.internal.table.scan.source.HttpScanSource;

/**
 * http-scan 的 {@link ScanTableSource} 实现。
 *
 * <p>支持列投影下推（非嵌套）；changelog 模式为 insert-only（有界扫描仅产出插入）。
 * 运行期通过 {@link SourceProvider} 暴露 {@link HttpScanSource}。
 */
@Slf4j
public class HttpScanDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown {

    private final HttpScanConfig config;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType physicalRowDataType;

    public HttpScanDynamicTableSource(HttpScanConfig config,
                                     DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                     DataType physicalRowDataType) {
        this.config = config;
        this.decodingFormat = decodingFormat;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer =
            decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalRowDataType);
        return SourceProvider.of(new HttpScanSource(config, deserializer));
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpScanDynamicTableSource(config, decodingFormat, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "HttpScanDynamicTableSource";
    }
}

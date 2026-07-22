package com.getindata.connectors.http.internal.table.scan.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.pagination.CursorPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.NoPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.PageNumberPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationStrategy;
import com.getindata.connectors.http.internal.table.scan.request.ScanRequestTemplate;

/**
 * http-scan 的 FLIP-27 {@link Source} 入口，声明 {@link Boundedness#BOUNDED}。
 *
 * <p>单并行度：SplitEnumerator 只发一个 {@link HttpScanSplit}，Reader 串行分页拉取。
 */
@Slf4j
public class HttpScanSource implements Source<RowData, HttpScanSplit, Void> {

    private final HttpScanConfig config;
    private final DeserializationSchema<RowData> deserializer;

    public HttpScanSource(HttpScanConfig config, DeserializationSchema<RowData> deserializer) {
        this.config = config;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, HttpScanSplit> createReader(SourceReaderContext readerContext) {
        try {
            return new HttpScanSourceReader(
                HttpScanHttpClientFactory.create(config),
                new ScanRequestTemplate(config),
                strategyFor(config),
                config,
                deserializer,
                readerContext);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Failed to create http-scan SourceReader", e);
        }
    }

    @Override
    public SplitEnumerator<HttpScanSplit, Void> createEnumerator(
            SplitEnumeratorContext<HttpScanSplit> enumContext) {
        return new HttpScanSplitEnumerator(enumContext, config);
    }

    @Override
    public SplitEnumerator<HttpScanSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<HttpScanSplit> enumContext, Void checkpoint) {
        throw new UnsupportedOperationException(
            "http-scan does not support checkpoint restore in v1");
    }

    @Override
    public SimpleVersionedSerializer<HttpScanSplit> getSplitSerializer() {
        return new HttpScanSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    private static PaginationStrategy strategyFor(HttpScanConfig config) {
        String type = config.getPaginationType();
        if ("page-number".equals(type)) {
            return new PageNumberPagination();
        } else if ("cursor".equals(type)) {
            return new CursorPagination();
        }
        return new NoPagination();
    }
}

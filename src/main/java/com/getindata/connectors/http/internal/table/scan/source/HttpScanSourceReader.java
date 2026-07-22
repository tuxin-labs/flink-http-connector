package com.getindata.connectors.http.internal.table.scan.source;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.UserCodeClassLoader;

import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.status.HttpCodesParser;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationState;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationStrategy;
import com.getindata.connectors.http.internal.table.scan.pagination.StopDecision;
import com.getindata.connectors.http.internal.table.scan.request.ScanRequestTemplate;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

/**
 * http-scan 的 {@link SourceReader}，单并行度串行分页拉取。
 *
 * <p>每次 {@link #pollNext} 执行一次分页请求：构造请求 -> 发送（含重试）-> 剥壳 ->
 * 逐条反序列化 -> collect -> 推进分页状态。分页结束返回 {@link InputStatus#END_OF_INPUT}。
 * ignored-response-codes 命中时跳过内容但仍推进分页；未分类错误码由
 * {@link HttpClientWithRetry} 抛异常导致作业失败。
 */
@Slf4j
public class HttpScanSourceReader implements SourceReader<RowData, HttpScanSplit> {

    private final HttpClientWithRetry httpClient;
    private final ScanRequestTemplate requestTemplate;
    private final PaginationStrategy paginationStrategy;
    private final HttpScanConfig config;
    private final DeserializationSchema<RowData> deserializer;
    private final SourceReaderContext readerContext;
    private final Set<Integer> ignoredCodes;

    private PaginationState state;
    private boolean finished = false;

    public HttpScanSourceReader(HttpClientWithRetry httpClient,
                                ScanRequestTemplate requestTemplate,
                                PaginationStrategy paginationStrategy,
                                HttpScanConfig config,
                                DeserializationSchema<RowData> deserializer,
                                SourceReaderContext readerContext) throws ConfigurationException {
        this.httpClient = httpClient;
        this.requestTemplate = requestTemplate;
        this.paginationStrategy = paginationStrategy;
        this.config = config;
        this.deserializer = deserializer;
        this.readerContext = readerContext;
        String ignoredExpr = config.getReadableConfig()
            .get(HttpScanConnectorOptions.IGNORED_RESPONSE_CODES);
        this.ignoredCodes = (ignoredExpr == null || ignoredExpr.isBlank())
            ? Set.of() : HttpCodesParser.parse(ignoredExpr);
    }

    @Override
    public void start() {
        log.info("Starting http-scan SourceReader for url={}", config.getUrl());
        // flink-json 的 DeserializationSchema 需在反序列化前 open，初始化 ObjectMapper
        try {
            deserializer.open(new DeserializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return readerContext.metricGroup();
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return readerContext.getUserCodeClassLoader();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to open deserializer for http-scan", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (finished) {
            return InputStatus.END_OF_INPUT;
        }
        if (state == null) {
            state = paginationStrategy.initialState(config);
        }

        Optional<Map<String, String>> reqValues = paginationStrategy.nextRequestValues(state, config);
        if (reqValues.isEmpty()) {
            finished = true;
            return InputStatus.END_OF_INPUT;
        }

        java.net.http.HttpRequest request = requestTemplate.build(reqValues.get());
        HttpResponse<byte[]> response =
            httpClient.send(() -> request, HttpResponse.BodyHandlers.ofByteArray());

        int status = response.statusCode();
        StopDecision decision;
        if (ignoredCodes.contains(status)) {
            log.debug("Ignoring response with status {} for url {}", status, config.getUrl());
            decision = paginationStrategy.afterResponse(state, "", 0, config);
        } else {
            String body = new String(response.body(), StandardCharsets.UTF_8);
            List<byte[]> records = JsonPathExtractor.extractRecords(body, config.getContentField());
            for (byte[] rec : records) {
                RowData row = deserializer.deserialize(rec);
                if (row != null) {
                    output.collect(row);
                }
            }
            decision = paginationStrategy.afterResponse(state, body, records.size(), config);
        }

        state = decision.getNextState();
        if (decision.isShouldStop()) {
            finished = true;
            return InputStatus.END_OF_INPUT;
        }
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<HttpScanSplit> splits) {
        // reader 已在构造期持有 config，split 仅作为分配信号
        if (!splits.isEmpty()) {
            log.debug("Received {} split(s)", splits.size());
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // 无动作
    }

    @Override
    public List<HttpScanSplit> snapshotState(long checkpointId) {
        return List.of();
    }

    @Override
    public void close() {
        // HttpClient 复用，无显式关闭
    }
}

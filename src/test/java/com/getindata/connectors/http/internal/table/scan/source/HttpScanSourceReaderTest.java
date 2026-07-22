package com.getindata.connectors.http.internal.table.scan.source;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;
import com.getindata.connectors.http.internal.table.scan.pagination.NoPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.PageNumberPagination;
import com.getindata.connectors.http.internal.table.scan.request.ScanRequestTemplate;

class HttpScanSourceReaderTest {

    @SuppressWarnings("unchecked")
    private HttpResponse<byte[]> mockResponse(int status, String body) {
        var resp = (HttpResponse<byte[]>) Mockito.mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(status);
        when(resp.body()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        return resp;
    }

    private DeserializationSchema<RowData> stubDeserializer() {
        return new DeserializationSchema<>() {
            @Override
            public RowData deserialize(byte[] message) {
                return new GenericRowData(1);
            }

            @Override
            public boolean isEndOfStream(RowData nextElement) {
                return false;
            }

            @Override
            public TypeInformation<RowData> getProducedType() {
                return TypeInformation.of(RowData.class);
            }
        };
    }

    private static class CollectingOutput implements ReaderOutput<RowData> {
        final List<RowData> rows = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            rows.add(record);
        }

        @Override
        public void collect(RowData record, long timestamp) {
            rows.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
        }

        @Override
        public void markIdle() {
        }

        @Override
        public void markActive() {
        }

        @Override
        public SourceOutput<RowData> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
        }
    }

    @Test
    void shouldPullSinglePageAndEndForNoPagination() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/items");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var httpClient = Mockito.mock(HttpClientWithRetry.class);
        Mockito.doReturn(mockResponse(200, "[{\"id\":1},{\"id\":2}]"))
            .when(httpClient).send(any(), any());

        var reader = new HttpScanSourceReader(
            httpClient, new ScanRequestTemplate(cfg), new NoPagination(), cfg, stubDeserializer());
        var output = new CollectingOutput();

        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.rows).hasSize(2);
    }

    @Test
    void shouldPaginateMultiplePagesUntilBatchSize() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x/orders");
        conf.set(HttpScanConnectorOptions.QUERY_PARAMS, "page=${page}");
        conf.set(HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
        conf.set(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, 1);
        var cfg = HttpScanConfig.from(conf, new Properties());

        var httpClient = Mockito.mock(HttpClientWithRetry.class);
        // page1: 2 rows (>= batch-size 1 -> continue); page2: 0 rows (< 1 -> stop)
        // doReturn 避免 thenReturn 多参数的泛型推断问题
        Mockito.doReturn(
                mockResponse(200, "[{\"id\":1},{\"id\":2}]"),
                mockResponse(200, "[]"))
            .when(httpClient).send(any(), any());

        var reader = new HttpScanSourceReader(
            httpClient, new ScanRequestTemplate(cfg), new PageNumberPagination(), cfg, stubDeserializer());
        var output = new CollectingOutput();

        InputStatus s1 = reader.pollNext(output);
        assertThat(s1).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(output.rows).hasSize(2);

        InputStatus s2 = reader.pollNext(output);
        assertThat(s2).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.rows).hasSize(2);
    }
}

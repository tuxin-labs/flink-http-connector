package com.getindata.connectors.http.internal.table.scan;

import java.util.ArrayList;
import java.util.List;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * http-scan 端到端 SQL 测试：WireMock + Flink BATCH 模式，验证 CREATE TABLE + SELECT 全链路。
 */
class HttpScanTableSourceITCaseTest {

    private WireMockServer wireMockServer;

    @BeforeEach
    void startServer() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
    }

    @AfterEach
    void stopServer() {
        wireMockServer.stop();
    }

    private StreamTableEnvironment tableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    @Test
    void shouldScanHttpApiNoPagination() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/items"))
            .willReturn(okJson("{\"data\":[{\"id\":1,\"name\":\"alice\"},{\"id\":2,\"name\":\"bob\"}]}")));

        var tableEnv = tableEnv();
        tableEnv.executeSql(
            "CREATE TABLE http_scan (\n"
                + "  id INT,\n"
                + "  name STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'http-scan',\n"
                + "  'url' = '" + wireMockServer.baseUrl() + "/items',\n"
                + "  'format' = 'json',\n"
                + "  'gid.connector.http.scan.content-field' = '$.data.*'\n"
                + ")");

        List<Row> rows = collect(tableEnv.executeSql("SELECT * FROM http_scan"));
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getField(0)).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void shouldScanWithPageNumberPagination() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/orders")).withQueryParam("page", equalTo("1"))
            .willReturn(okJson("{\"data\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]}")));
        wireMockServer.stubFor(get(urlPathEqualTo("/orders")).withQueryParam("page", equalTo("2"))
            .willReturn(okJson("{\"data\":[]}")));

        var tableEnv = tableEnv();
        tableEnv.executeSql(
            "CREATE TABLE http_paged (\n"
                + "  id INT,\n"
                + "  name STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'http-scan',\n"
                + "  'url' = '" + wireMockServer.baseUrl() + "/orders',\n"
                + "  'format' = 'json',\n"
                + "  'gid.connector.http.scan.query-params' = 'page=${page}',\n"
                + "  'gid.connector.http.scan.content-field' = '$.data.*',\n"
                + "  'gid.connector.http.scan.pagination.type' = 'page-number',\n"
                + "  'gid.connector.http.scan.pagination.batch-size' = '1'\n"
                + ")");

        List<Row> rows = collect(tableEnv.executeSql("SELECT * FROM http_paged"));
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getField(0)).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void shouldScanWithCursorPagination() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/cursor")).withQueryParam("cursor", equalTo(""))
            .willReturn(aResponse().withHeader("Content-Type", "application/json")
                .withBody("{\"data\":[{\"id\":1,\"name\":\"a\"}],\"next\":\"page2\"}")));
        wireMockServer.stubFor(get(urlPathEqualTo("/cursor")).withQueryParam("cursor", equalTo("page2"))
            .willReturn(aResponse().withHeader("Content-Type", "application/json")
                .withBody("{\"data\":[{\"id\":2,\"name\":\"b\"}],\"next\":null}")));

        var tableEnv = tableEnv();
        tableEnv.executeSql(
            "CREATE TABLE http_cursor (\n"
                + "  id INT,\n"
                + "  name STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'http-scan',\n"
                + "  'url' = '" + wireMockServer.baseUrl() + "/cursor',\n"
                + "  'format' = 'json',\n"
                + "  'gid.connector.http.scan.query-params' = 'cursor=${cursor}',\n"
                + "  'gid.connector.http.scan.content-field' = '$.data.*',\n"
                + "  'gid.connector.http.scan.pagination.type' = 'cursor',\n"
                + "  'gid.connector.http.scan.pagination.cursor-response-jsonpath' = '$.next',\n"
                + "  'gid.connector.http.scan.pagination.initial-cursor' = ''\n"
                + ")");

        List<Row> rows = collect(tableEnv.executeSql("SELECT * FROM http_cursor"));
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getField(0)).containsExactlyInAnyOrder(1, 2);
    }
}

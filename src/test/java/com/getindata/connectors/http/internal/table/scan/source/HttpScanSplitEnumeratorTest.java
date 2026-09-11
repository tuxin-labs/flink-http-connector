package com.getindata.connectors.http.internal.table.scan.source;

import java.util.Properties;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class HttpScanSplitEnumeratorTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldAssignSingleSplitOnce() {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        enumerator.start();
        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(HttpScanSplit.class), eq(0));
        verify(ctx, times(1)).signalNoMoreSplits(0);

        // 第二次请求不再分配
        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(), eq(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldReturnNullSnapshot() throws Exception {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());
        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        assertThat(enumerator.snapshotState(1L)).isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldReassignAfterSplitsBack() {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        enumerator.start();
        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(), eq(0));

        // split 退回后允许重新分配
        enumerator.addSplitsBack(java.util.List.of(new HttpScanSplit(cfg)), 0);
        Mockito.clearInvocations(ctx);
        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(), eq(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldAssignOnAddReader() {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());
        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        enumerator.addReader(0);
        verify(ctx, times(1)).assignSplit(any(), eq(0));
        verify(ctx, times(1)).signalNoMoreSplits(0);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldSignalNoMoreSplitsToSecondRequester() {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new Properties());

        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        enumerator.start();
        // 并行度 > 1：subtask 0 拿到 split，subtask 1 只收到结束信号（防止多余子任务挂住）
        enumerator.handleSplitRequest(0, "host");
        enumerator.handleSplitRequest(1, "host");
        verify(ctx, times(1)).assignSplit(any(), eq(0));
        verify(ctx, times(1)).signalNoMoreSplits(0);
        verify(ctx, times(1)).signalNoMoreSplits(1);
    }
}

package com.getindata.connectors.http.internal.table.scan.source;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * 单并行度枚举器：构造时创建唯一的 {@link HttpScanSplit}，首次请求时分配并标记无更多 split。
 */
@Slf4j
public class HttpScanSplitEnumerator implements SplitEnumerator<HttpScanSplit, Void> {

    private final SplitEnumeratorContext<HttpScanSplit> context;
    private final HttpScanSplit split;
    private boolean assigned = false;

    public HttpScanSplitEnumerator(SplitEnumeratorContext<HttpScanSplit> context, HttpScanConfig config) {
        this.context = context;
        this.split = new HttpScanSplit(config);
    }

    @Override
    public void start() {
        log.info("Starting http-scan SplitEnumerator for url={}", split.getConfig().getUrl());
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!assigned) {
            context.assignSplit(split, subtaskId);
            context.signalNoMoreSplits(subtaskId);
            assigned = true;
            log.info("Assigned http-scan split to subtask {}", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HttpScanSplit> splits, int subtaskId) {
        // split 退回时重置分配标记，允许重新分配
        if (!splits.isEmpty()) {
            assigned = false;
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // reader 注册时主动触发一次分配
        handleSplitRequest(subtaskId, "enumerator-internal");
    }

    @Override
    public Void snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public void close() {
        // 无资源需释放
    }
}

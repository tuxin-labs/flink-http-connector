package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.api.connector.source.SourceSplit;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * http-scan 的单一 split，携带 {@link HttpScanConfig}。
 * 单并行度场景下 SplitEnumerator 只发一个此实例。
 */
public class HttpScanSplit implements SourceSplit {

    public static final String SPLIT_ID = "http-scan-split-0";

    private final HttpScanConfig config;

    public HttpScanSplit(HttpScanConfig config) {
        this.config = config;
    }

    public HttpScanConfig getConfig() {
        return config;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }
}

package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * 无分页：仅发起一次请求即结束。
 */
public class NoPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState(HttpScanConfig config) {
        return PaginationState.initialPage(0);
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        if (state.getRequestCount() == 0) {
            return Optional.of(Map.of());
        }
        return Optional.empty();
    }

    @Override
    public StopDecision afterResponse(
        PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        return StopDecision.stop(
            state.incrementRequest().accumulate(rowsInPage));
    }
}

package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

/**
 * 游标分页。下一页 cursor 从响应体 cursor-response-jsonpath 抽取；为 null/空即停。
 * 首次请求使用 initial-cursor；total-pages 可作为硬上限兜底（优先级同页码模式）。
 */
public class CursorPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState(HttpScanConfig config) {
        return PaginationState.initialCursor(config.getInitialCursor());
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        if (state.getRequestCount() > 0) {
            String cur = state.getCursor();
            if (cur == null || cur.isBlank()) {
                return Optional.empty();
            }
        }
        String cur = state.getCursor() == null ? "" : state.getCursor();
        return Optional.of(Map.of(config.getCursorField(), cur));
    }

    @Override
    public StopDecision afterResponse(
        PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        PaginationState updated = state.incrementRequest().accumulate(rowsInPage);

        // total-pages 硬上限兜底
        if (config.getTotalPages() != null && updated.getRequestCount() >= config.getTotalPages()) {
            return StopDecision.stop(updated);
        }

        String next = JsonPathExtractor.extractString(
            responseBody, config.getCursorResponseJsonPath());
        if (next == null || next.isBlank()) {
            return StopDecision.stop(updated);
        }
        return StopDecision.continueWith(updated.withCursor(next));
    }
}

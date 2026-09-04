package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

/**
 * 游标分页。下一页 cursor 从响应体 cursor-response-jsonpath 抽取；为 null/空即停。
 * 首次请求使用 initial-cursor；max-requests 安全阀与 total-pages 硬上限兜底。
 */
@Slf4j
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

        // 优先级0: max-requests 安全阀（防止 API 异常持续返回非空 cursor 导致无限扫描）
        if (updated.getRequestCount() >= config.getMaxRequests()) {
            log.warn("http-scan reached pagination.max-requests={} and stopped; "
                + "increase it if more data is expected.", config.getMaxRequests());
            return StopDecision.stop(updated);
        }

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

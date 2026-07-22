package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

/**
 * 页码分页。停止策略优先级（任一命中即停）：
 * total-pages → total-count-jsonpath → has-more-jsonpath → batch-size。
 */
public class PageNumberPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState(HttpScanConfig config) {
        return PaginationState.initialPage(config.getStartPage());
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        return Optional.of(Map.of(config.getPageField(), String.valueOf(state.getPageNumber())));
    }

    @Override
    public StopDecision afterResponse(
        PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        PaginationState updated =
            state.incrementRequest().accumulate(rowsInPage);

        // 优先级1: total-pages（已发请求达到总数即停）
        if (config.getTotalPages() != null && updated.getRequestCount() >= config.getTotalPages()) {
            return StopDecision.stop(updated);
        }
        // 优先级2: total-count-jsonpath
        if (config.getTotalCountJsonPath() != null) {
            Long total = JsonPathExtractor.extractLong(responseBody, config.getTotalCountJsonPath());
            if (total != null && updated.getRowsSeenTotal() >= total) {
                return StopDecision.stop(updated);
            }
        }
        // 优先级3: has-more-jsonpath
        if (config.getHasMoreJsonPath() != null) {
            Boolean hasMore = JsonPathExtractor.extractBoolean(responseBody, config.getHasMoreJsonPath());
            if (hasMore == null || !hasMore) {
                return StopDecision.stop(updated);
            }
        }
        // 优先级4: batch-size
        if (config.getBatchSize() != null && rowsInPage < config.getBatchSize()) {
            return StopDecision.stop(updated);
        }
        return StopDecision.continueWith(updated.nextPage());
    }
}

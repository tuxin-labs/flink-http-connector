package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * 分页策略接口。每种分页方式（none/page-number/cursor）独立实现。
 *
 * <p>协议：Reader 先 {@link #initialState} 得到初始状态，循环调用：
 * {@link #nextRequestValues}（empty 表示无更多请求）→ 发请求 → {@link #afterResponse} 判定停止并推进状态。
 */
public interface PaginationStrategy {

    PaginationState initialState(HttpScanConfig config);

    /**
     * 返回当前 state 对应的占位符取值（如 {"page":"3"}）。empty 表示无更多请求。
     */
    Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config);

    /**
     * 根据本页响应体与返回行数，返回停止决策与新状态。停止策略优先级见设计文档 §2.2。
     */
    StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config);
}

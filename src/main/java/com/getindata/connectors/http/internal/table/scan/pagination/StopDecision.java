package com.getindata.connectors.http.internal.table.scan.pagination;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 分页停止判定结果。{@code shouldStop=true} 表示拉取结束；{@code nextState} 为推进后的状态。
 */
@Getter
@RequiredArgsConstructor
public final class StopDecision {

    private final boolean shouldStop;
    private final PaginationState nextState;

    public static StopDecision continueWith(PaginationState state) {
        return new StopDecision(false, state);
    }

    public static StopDecision stop(PaginationState state) {
        return new StopDecision(true, state);
    }
}

package com.getindata.connectors.http.internal.table.scan.pagination;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 不可变分页状态。每次推进都返回新实例，便于在单并行度串行流程中追踪进度。
 */
@Getter
@RequiredArgsConstructor
public final class PaginationState {

    private final int pageNumber;
    private final String cursor;
    private final long rowsSeenTotal;
    private final int requestCount;

    public static PaginationState initialPage(int startPage) {
        return new PaginationState(startPage, null, 0L, 0);
    }

    public static PaginationState initialCursor(String cursor) {
        return new PaginationState(0, cursor, 0L, 0);
    }

    public PaginationState nextPage() {
        return new PaginationState(pageNumber + 1, cursor, rowsSeenTotal, requestCount);
    }

    public PaginationState withCursor(String newCursor) {
        return new PaginationState(pageNumber, newCursor, rowsSeenTotal, requestCount);
    }

    public PaginationState accumulate(int rowsInPage) {
        return new PaginationState(pageNumber, cursor, rowsSeenTotal + rowsInPage, requestCount);
    }

    public PaginationState incrementRequest() {
        return new PaginationState(pageNumber, cursor, rowsSeenTotal, requestCount + 1);
    }
}

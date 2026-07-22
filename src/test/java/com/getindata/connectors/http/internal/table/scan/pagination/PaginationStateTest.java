package com.getindata.connectors.http.internal.table.scan.pagination;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PaginationStateTest {

    @Test
    void shouldCreateInitialPageState() {
        var s = PaginationState.initialPage(3);
        assertThat(s.getPageNumber()).isEqualTo(3);
        assertThat(s.getRowsSeenTotal()).isZero();
        assertThat(s.getRequestCount()).isZero();
        assertThat(s.getCursor()).isNull();
    }

    @Test
    void shouldCreateInitialCursorState() {
        var s = PaginationState.initialCursor("abc");
        assertThat(s.getCursor()).isEqualTo("abc");
        assertThat(s.getPageNumber()).isZero();
    }

    @Test
    void shouldAdvancePageAndAccumulateRows() {
        var s = PaginationState.initialPage(1).incrementRequest();
        s = s.nextPage().accumulate(100).incrementRequest();
        assertThat(s.getPageNumber()).isEqualTo(2);
        assertThat(s.getRowsSeenTotal()).isEqualTo(100);
        assertThat(s.getRequestCount()).isEqualTo(2);
    }

    @Test
    void shouldUpdateCursor() {
        var s = PaginationState.initialCursor("").withCursor("next").incrementRequest();
        assertThat(s.getCursor()).isEqualTo("next");
        assertThat(s.getRequestCount()).isEqualTo(1);
    }

    @Test
    void shouldRemainImmutable() {
        var original = PaginationState.initialPage(1);
        original.nextPage().accumulate(50);
        assertThat(original.getPageNumber()).isEqualTo(1);
        assertThat(original.getRowsSeenTotal()).isZero();
    }
}

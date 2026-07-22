package com.getindata.connectors.http.internal.table.scan.request;

import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlaceholderResolverTest {

    @Test
    void shouldParseUrlVars() {
        assertThat(PlaceholderResolver.parseUrlVars("cid:C1001,oid:O9"))
            .containsEntry("cid", "C1001").containsEntry("oid", "O9");
        assertThat(PlaceholderResolver.parseUrlVars(null)).isEmpty();
        assertThat(PlaceholderResolver.parseUrlVars("")).isEmpty();
    }

    @Test
    void shouldRejectMalformedUrlVar() {
        assertThatThrownBy(() -> PlaceholderResolver.parseUrlVars("badpair"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldResolvePathVars() {
        String url = "https://x/{cid}/orders/{oid}";
        assertThat(PlaceholderResolver.resolvePathVars(url, Map.of("cid", "C1", "oid", "O2")))
            .isEqualTo("https://x/C1/orders/O2");
    }

    @Test
    void shouldReplacePagePlaceholder() {
        assertThat(PlaceholderResolver.replace("page=${page}", Map.of("page", "3")))
            .isEqualTo("page=3");
    }

    @Test
    void shouldReplaceNullValueWithEmpty() {
        java.util.Map<String, String> values = new java.util.HashMap<>();
        values.put("cursor", null);
        assertThat(PlaceholderResolver.replace("c=${cursor}", values)).isEqualTo("c=");
    }

    @Test
    void shouldEncodeQueryValue() {
        assertThat(PlaceholderResolver.encodeQueryValue("a b&c")).isEqualTo("a+b%26c");
    }

    @Test
    void shouldHandleRepeatedPlaceholders() {
        assertThat(PlaceholderResolver.replace("${p}-${p}", Map.of("p", "1")))
            .isEqualTo("1-1");
    }

    @Test
    void shouldReturnNullForNullTemplate() {
        assertThat(PlaceholderResolver.replace(null, Map.of())).isNull();
    }
}

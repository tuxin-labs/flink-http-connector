package com.getindata.connectors.http.internal.table.scan.response;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonPathExtractorTest {

    @Test
    void shouldExtractTopLevelArrayWhenNoContentField() {
        String body = "[{\"id\":1},{\"id\":2}]";
        List<byte[]> recs = JsonPathExtractor.extractRecords(body, null);
        assertThat(recs).hasSize(2);
    }

    @Test
    void shouldExtractTopLevelSingleObjectWhenNoContentField() {
        List<byte[]> recs = JsonPathExtractor.extractRecords("{\"id\":1}", null);
        assertThat(recs).hasSize(1);
    }

    @Test
    void shouldExtractArrayByDotPath() {
        String body = "{\"data\":[{\"id\":1},{\"id\":2}]}";
        List<byte[]> recs = JsonPathExtractor.extractRecords(body, "$.data");
        assertThat(recs).hasSize(2);
    }

    @Test
    void shouldExtractNestedArray() {
        String body = "{\"resp\":{\"data\":{\"list\":[{\"id\":1}]}}}";
        assertThat(JsonPathExtractor.extractRecords(body, "$.resp.data.list")).hasSize(1);
    }

    @Test
    void shouldReturnEmptyWhenPathMissing() {
        String body = "{\"data\":[]}";
        assertThat(JsonPathExtractor.extractRecords(body, "$.data")).isEmpty();
        assertThat(JsonPathExtractor.extractRecords("{\"other\":1}", "$.data")).isEmpty();
    }

    @Test
    void shouldExtractSingleObjectAtLeaf() {
        String body = "{\"data\":{\"id\":1}}";
        List<byte[]> recs = JsonPathExtractor.extractRecords(body, "$.data");
        assertThat(recs).hasSize(1);
    }

    @Test
    void shouldExtractLong() {
        assertThat(JsonPathExtractor.extractLong("{\"total\":250}", "$.total")).isEqualTo(250L);
        assertThat(JsonPathExtractor.extractLong("{\"total\":250}", "$.missing")).isNull();
    }

    @Test
    void shouldExtractBoolean() {
        assertThat(JsonPathExtractor.extractBoolean("{\"hasMore\":true}", "$.hasMore")).isTrue();
        assertThat(JsonPathExtractor.extractBoolean("{\"hasMore\":false}", "$.hasMore")).isFalse();
        assertThat(JsonPathExtractor.extractBoolean("{}", "$.hasMore")).isNull();
    }

    @Test
    void shouldExtractStringCursor() {
        assertThat(JsonPathExtractor.extractString("{\"paging\":{\"next\":\"abc\"}}", "$.paging.next"))
            .isEqualTo("abc");
        assertThat(JsonPathExtractor.extractString("{}", "$.paging.next")).isNull();
    }

    @Test
    void shouldThrowOnNonObjectArrayElements() {
        String body = "{\"data\":[\"x\",\"y\"]}";
        assertThatThrownBy(() -> JsonPathExtractor.extractRecords(body, "$.data"))
            .isInstanceOf(JsonPathExtractionException.class);
    }

    @Test
    void shouldCompileCheckValidAndInvalid() {
        JsonPathExtractor.compileCheck("$.a.b");
        JsonPathExtractor.compileCheck("$");
        assertThatThrownBy(() -> JsonPathExtractor.compileCheck(null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonPathExtractor.compileCheck("a.b"))
            .isInstanceOf(IllegalArgumentException.class);
    }
}

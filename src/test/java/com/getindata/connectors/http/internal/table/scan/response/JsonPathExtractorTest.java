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
    void shouldExtractArrayWithWildcardSuffix() {
        // $.data.* 语法（与 SeaTunnel 一致）：* 表示取该字段全部元素
        String body = "{\"data\":[{\"id\":1},{\"id\":2}]}";
        assertThat(JsonPathExtractor.extractRecords(body, "$.data.*")).hasSize(2);
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

    @Test
    void shouldThrowWhenContentFieldPointsToScalar() {
        String body = "{\"data\":\"notanarray\"}";
        assertThatThrownBy(() -> JsonPathExtractor.extractRecords(body, "$.data"))
            .isInstanceOf(JsonPathExtractionException.class);
    }

    @Test
    void shouldThrowWhenTopLevelIsScalar() {
        assertThatThrownBy(() -> JsonPathExtractor.extractRecords("\"juststring\"", null))
            .isInstanceOf(JsonPathExtractionException.class);
    }

    @Test
    void shouldExtractLongFromNumericStringField() {
        // asLong 对数字字符串也能解析
        assertThat(JsonPathExtractor.extractLong("{\"total\":\"42\"}", "$.total")).isEqualTo(42L);
    }

    @Test
    void shouldReturnEmptyWhenBodyIsInvalidJson() {
        assertThatThrownBy(() -> JsonPathExtractor.extractRecords("not json", null))
            .isInstanceOf(JsonPathExtractionException.class);
    }

    @Test
    void shouldExtractBooleanFromNonBooleanField() {
        // 非 boolean 字段走 asBoolean 转换（"true" -> true）
        assertThat(JsonPathExtractor.extractBoolean("{\"flag\":\"true\"}", "$.flag")).isTrue();
    }

    @Test
    void shouldExtractStringFromNumericField() {
        assertThat(JsonPathExtractor.extractString("{\"code\":200}", "$.code")).isEqualTo("200");
    }

    @Test
    void shouldExtractRecordsFromWildcardOnNestedPath() {
        String body = "{\"resp\":{\"items\":[{\"id\":1}]}}";
        assertThat(JsonPathExtractor.extractRecords(body, "$.resp.items.*")).hasSize(1);
    }

    @Test
    void shouldReturnEmptyWhenContentFieldIsJsonNull() {
        assertThat(JsonPathExtractor.extractRecords("{\"data\":null}", "$.data")).isEmpty();
    }

    @Test
    void shouldReturnNullWhenIntermediatePathMissing() {
        // 中间字段缺失 -> navigate 返回 null
        assertThat(JsonPathExtractor.extractLong("{\"x\":1}", "$.a.b")).isNull();
        assertThat(JsonPathExtractor.extractBoolean("{\"x\":1}", "$.a.b")).isNull();
        assertThat(JsonPathExtractor.extractString("{\"x\":1}", "$.a.b")).isNull();
    }

    @Test
    void shouldReturnNullWhenFieldIsJsonNull() {
        assertThat(JsonPathExtractor.extractLong("{\"total\":null}", "$.total")).isNull();
        assertThat(JsonPathExtractor.extractBoolean("{\"flag\":null}", "$.flag")).isNull();
        assertThat(JsonPathExtractor.extractString("{\"name\":null}", "$.name")).isNull();
    }

    @Test
    void shouldRejectBlankJsonPathInCompileCheck() {
        assertThatThrownBy(() -> JsonPathExtractor.compileCheck(""))
            .isInstanceOf(IllegalArgumentException.class);
    }
}

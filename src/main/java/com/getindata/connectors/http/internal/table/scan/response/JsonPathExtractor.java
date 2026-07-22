package com.getindata.connectors.http.internal.table.scan.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于 Jackson 的轻量 JSONPath 抽取器，避免引入 jayway/JsonPath 依赖。
 *
 * <p>支持语法：$（整体）、$.a、$.a.b.c（点分路径）。剥壳到数组时逐元素返回，剥壳到单对象返回单条。
 * 路径未命中或为 null 时返回空列表（不视为错误）。
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public final class JsonPathExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<byte[]> extractRecords(String body, String contentField) {
        JsonNode root = parse(body);
        JsonNode target = (contentField == null || contentField.isBlank() || "$".equals(contentField))
            ? root : navigate(root, contentField);
        if (target == null || target.isNull()) {
            return List.of();
        }
        List<byte[]> records = new ArrayList<>();
        if (target.isArray()) {
            for (JsonNode el : target) {
                if (!el.isObject()) {
                    throw new JsonPathExtractionException(
                        "记录数组元素必须是 JSON 对象，实际为: " + el.getNodeType());
                }
                records.add(toBytes(el));
            }
        } else if (target.isObject()) {
            records.add(toBytes(target));
        } else {
            throw new JsonPathExtractionException(
                "content-field 指向的节点必须是对象或对象数组，实际为: " + target.getNodeType());
        }
        return records;
    }

    public static Long extractLong(String body, String jsonPath) {
        JsonNode node = navigate(parse(body), jsonPath);
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return node.asLong();
    }

    public static Boolean extractBoolean(String body, String jsonPath) {
        JsonNode node = navigate(parse(body), jsonPath);
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return node.asBoolean();
    }

    public static String extractString(String body, String jsonPath) {
        JsonNode node = navigate(parse(body), jsonPath);
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return node.asText();
    }

    public static void compileCheck(String jsonPath) {
        if (jsonPath == null || jsonPath.isBlank()) {
            throw new IllegalArgumentException("JSONPath 不能为空");
        }
        if (!jsonPath.startsWith("$")) {
            throw new IllegalArgumentException("JSONPath 必须以 $ 开头: " + jsonPath);
        }
    }

    private static JsonNode navigate(JsonNode root, String jsonPath) {
        String path = jsonPath.replaceAll("^\\$\\.?", "");
        if (path.isEmpty()) {
            return root;
        }
        JsonNode current = root;
        for (String segment : path.split("\\.")) {
            if (segment.isBlank() || "*".equals(segment)) {
                // "*" 通配（如 $.data.*）表示取该字段全部元素，跳过即可
                continue;
            }
            if (current == null) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    private static JsonNode parse(String body) {
        try {
            return MAPPER.readTree(body);
        } catch (IOException e) {
            throw new JsonPathExtractionException("无法解析响应体为 JSON", e);
        }
    }

    private static byte[] toBytes(JsonNode node) {
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (IOException e) {
            throw new JsonPathExtractionException("无法序列化 JSON 节点", e);
        }
    }
}

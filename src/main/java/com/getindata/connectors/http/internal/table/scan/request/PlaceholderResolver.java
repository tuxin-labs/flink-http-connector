package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * 占位符与 URL 编码工具：
 * <ul>
 *     <li>parseUrlVars: 解析 key1:v1,key2:v2 为 Map</li>
 *     <li>resolvePathVars: 替换 URL 中的 {name} 路径变量</li>
 *     <li>replace: 替换模板中的 ${key}，null 值替换为空串</li>
 *     <li>encodeQueryValue: URL 编码 query 值</li>
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class PlaceholderResolver {

    public static Map<String, String> parseUrlVars(String raw) {
        Map<String, String> map = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return map;
        }
        for (String pair : raw.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] kv = trimmed.split(":", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid url-vars entry '" + pair + "', expected key:value");
            }
            map.put(kv[0].trim(), kv[1].trim());
        }
        return map;
    }

    public static String resolvePathVars(String url, Map<String, String> urlVars) {
        String result = url;
        for (Map.Entry<String, String> e : urlVars.entrySet()) {
            result = result.replace("{" + e.getKey() + "}", e.getValue());
        }
        return result;
    }

    public static String replace(String template, Map<String, String> values) {
        if (template == null) {
            return null;
        }
        String result = template;
        for (Map.Entry<String, String> e : values.entrySet()) {
            String v = e.getValue() == null ? "" : e.getValue();
            result = result.replace("${" + e.getKey() + "}", v);
        }
        return result;
    }

    public static String encodeQueryValue(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}

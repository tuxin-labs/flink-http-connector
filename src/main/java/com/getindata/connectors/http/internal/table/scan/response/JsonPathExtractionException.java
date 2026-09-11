package com.getindata.connectors.http.internal.table.scan.response;

/**
 * JSONPath 剥壳或字段抽取失败时抛出。
 */
public class JsonPathExtractionException extends RuntimeException {

    public JsonPathExtractionException(String message) {
        super(message);
    }

    public JsonPathExtractionException(String message, Throwable cause) {
        super(message, cause);
    }
}

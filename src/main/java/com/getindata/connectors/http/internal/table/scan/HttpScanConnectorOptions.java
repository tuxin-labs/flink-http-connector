package com.getindata.connectors.http.internal.table.scan;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * 所有 http-scan 连接器的 {@link ConfigOption} 集中定义。
 *
 * <p>key 前缀遵循 gid.connector.http.scan.*（共性短名 url/method/format 除外），与 lookup/sink 并列。
 * 安全/日志相关 key 沿用 gid.connector.http.security.* 与 gid.connector.http.logging.level。
 */
public final class HttpScanConnectorOptions {

    private static final String SCAN_PREFIX = "gid.connector.http.scan.";
    private static final String PAGINATION_PREFIX = SCAN_PREFIX + "pagination.";

    // ---- 基础请求 ----
    public static final ConfigOption<String> URL =
        ConfigOptions.key("url").stringType().noDefaultValue()
            .withDescription("HTTP 端点 URL，可含 {path_var} 占位符。");

    public static final ConfigOption<String> METHOD =
        ConfigOptions.key("method").stringType().defaultValue("GET")
            .withDescription("HTTP 方法：GET/POST/PUT，默认 GET。");

    public static final ConfigOption<String> URL_VARS =
        ConfigOptions.key(SCAN_PREFIX + "url-vars").stringType().noDefaultValue()
            .withDescription("URL 路径变量静态取值，格式 key1:v1,key2:v2。");

    public static final ConfigOption<String> QUERY_PARAMS =
        ConfigOptions.key(SCAN_PREFIX + "query-params").stringType().noDefaultValue()
            .withDescription("URL query 参数，格式 k1=v1&k2=v2；& 与 = 为分隔符；值支持 ${page}/${cursor}。");

    public static final ConfigOption<String> BODY =
        ConfigOptions.key(SCAN_PREFIX + "body").stringType().noDefaultValue()
            .withDescription("POST/PUT 请求体模板，支持 ${page}/${cursor} 占位符。");

    public static final ConfigOption<String> BODY_CONTENT_TYPE =
        ConfigOptions.key(SCAN_PREFIX + "body-content-type").stringType()
            .defaultValue("application/json")
            .withDescription("请求体 Content-Type，默认 application/json。");

    public static final ConfigOption<String> CONTENT_FIELD =
        ConfigOptions.key(SCAN_PREFIX + "content-field").stringType().noDefaultValue()
            .withDescription("JSONPath，从响应体剥出记录数组或对象。");

    // ---- 分页 ----
    public static final ConfigOption<String> PAGINATION_TYPE =
        ConfigOptions.key(PAGINATION_PREFIX + "type").stringType().defaultValue("none")
            .withDescription("分页类型：none/page-number/cursor，默认 none。");

    public static final ConfigOption<String> PAGINATION_PAGE_FIELD =
        ConfigOptions.key(PAGINATION_PREFIX + "page-field").stringType().defaultValue("page")
            .withDescription("page-number 模式下页码占位符名。");

    public static final ConfigOption<Integer> PAGINATION_START_PAGE =
        ConfigOptions.key(PAGINATION_PREFIX + "start-page").intType().defaultValue(1)
            .withDescription("起始页码，默认 1。");

    public static final ConfigOption<Integer> PAGINATION_BATCH_SIZE =
        ConfigOptions.key(PAGINATION_PREFIX + "batch-size").intType().noDefaultValue()
            .withDescription("单页预期条数；本页行数 < batch-size 即停。");

    public static final ConfigOption<Integer> PAGINATION_TOTAL_PAGES =
        ConfigOptions.key(PAGINATION_PREFIX + "total-pages").intType().noDefaultValue()
            .withDescription("硬上限总页数；命中即最先生效。");

    public static final ConfigOption<String> PAGINATION_TOTAL_COUNT_JSONPATH =
        ConfigOptions.key(PAGINATION_PREFIX + "total-count-jsonpath").stringType().noDefaultValue()
            .withDescription("响应总数字段 JSONPath，如 $.total。");

    public static final ConfigOption<String> PAGINATION_HAS_MORE_JSONPATH =
        ConfigOptions.key(PAGINATION_PREFIX + "has-more-jsonpath").stringType().noDefaultValue()
            .withDescription("响应 has-more 字段 JSONPath；为 false/null 即停。");

    public static final ConfigOption<String> PAGINATION_CURSOR_FIELD =
        ConfigOptions.key(PAGINATION_PREFIX + "cursor-field").stringType().defaultValue("cursor")
            .withDescription("cursor 模式下请求侧游标占位符名。");

    public static final ConfigOption<String> PAGINATION_CURSOR_RESPONSE_JSONPATH =
        ConfigOptions.key(PAGINATION_PREFIX + "cursor-response-jsonpath").stringType()
            .noDefaultValue()
            .withDescription("从响应抽下一页 cursor 的 JSONPath。");

    public static final ConfigOption<String> PAGINATION_INITIAL_CURSOR =
        ConfigOptions.key(PAGINATION_PREFIX + "initial-cursor").stringType().defaultValue("")
            .withDescription("首次请求游标初始值。");

    // ---- HTTP 传输 ----
    public static final ConfigOption<Integer> REQUEST_TIMEOUT =
        ConfigOptions.key(SCAN_PREFIX + "request.timeout").intType().defaultValue(30)
            .withDescription("请求超时秒数，默认 30。");

    public static final ConfigOption<String> HTTP_VERSION =
        ConfigOptions.key(SCAN_PREFIX + "http-version").stringType().noDefaultValue()
            .withDescription("HTTP 版本：HTTP_1_1/HTTP_2。");

    public static final ConfigOption<String> RETRY_STRATEGY_TYPE =
        ConfigOptions.key(SCAN_PREFIX + "retry-strategy.type").stringType()
            .defaultValue("fixed-delay")
            .withDescription("重试策略：fixed-delay/exponential-delay。");

    public static final ConfigOption<Duration> RETRY_FIXED_DELAY =
        ConfigOptions.key(SCAN_PREFIX + "retry-strategy.fixed-delay.delay")
            .durationType().defaultValue(Duration.ofSeconds(1))
            .withDescription("fixed-delay 间隔。");

    public static final ConfigOption<Duration> RETRY_EXP_INITIAL_BACKOFF =
        ConfigOptions.key(SCAN_PREFIX + "retry-strategy.exponential-delay.initial-backoff")
            .durationType().defaultValue(Duration.ofSeconds(1))
            .withDescription("指数退避初始延迟。");

    public static final ConfigOption<Duration> RETRY_EXP_MAX_BACKOFF =
        ConfigOptions.key(SCAN_PREFIX + "retry-strategy.exponential-delay.max-backoff")
            .durationType().defaultValue(Duration.ofMinutes(1))
            .withDescription("指数退避最大延迟。");

    public static final ConfigOption<Double> RETRY_EXP_MULTIPLIER =
        ConfigOptions.key(SCAN_PREFIX + "retry-strategy.exponential-delay.backoff-multiplier")
            .doubleType().defaultValue(1.5d)
            .withDescription("指数退避乘数，默认 1.5。");

    public static final ConfigOption<Integer> MAX_RETRIES =
        ConfigOptions.key(SCAN_PREFIX + "max-retries").intType().defaultValue(3)
            .withDescription("每个请求最大重试次数，默认 3。");

    public static final ConfigOption<String> SUCCESS_CODES =
        ConfigOptions.key(SCAN_PREFIX + "success-codes").stringType().defaultValue("2XX")
            .withDescription("成功状态码，语法 2XX,404,!203。");

    public static final ConfigOption<String> RETRY_CODES =
        ConfigOptions.key(SCAN_PREFIX + "retry-codes").stringType().noDefaultValue()
            .withDescription("可重试状态码。");

    public static final ConfigOption<String> IGNORED_RESPONSE_CODES =
        ConfigOptions.key(SCAN_PREFIX + "ignored-response-codes").stringType().noDefaultValue()
            .withDescription("忽略响应状态码（跳过内容但仍推进分页）。");

    // ---- 连接与代理 ----
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
        ConfigOptions.key(SCAN_PREFIX + "connection.timeout").durationType().noDefaultValue()
            .withDescription("连接超时。");

    public static final ConfigOption<String> PROXY_HOST =
        ConfigOptions.key(SCAN_PREFIX + "proxy.host").stringType().noDefaultValue()
            .withDescription("代理主机名。");

    public static final ConfigOption<Integer> PROXY_PORT =
        ConfigOptions.key(SCAN_PREFIX + "proxy.port").intType().noDefaultValue()
            .withDescription("代理端口。");

    public static final ConfigOption<String> PROXY_USERNAME =
        ConfigOptions.key(SCAN_PREFIX + "proxy.username").stringType().noDefaultValue()
            .withDescription("代理用户名。");

    public static final ConfigOption<String> PROXY_PASSWORD =
        ConfigOptions.key(SCAN_PREFIX + "proxy.password").stringType().noDefaultValue()
            .withDescription("代理密码。");

    public static final ConfigOption<Boolean> USE_RAW_AUTH_HEADER =
        ConfigOptions.key(SCAN_PREFIX + "use-raw-authorization-header").booleanType()
            .defaultValue(false)
            .withDescription("Basic Auth 原样透传 Authorization header。");

    // ---- Header 前缀（非 ConfigOption，供 ScanRequestTemplate 读取 Properties 用） ----
    public static final String HEADER_PREFIX = SCAN_PREFIX + "header.";

    private HttpScanConnectorOptions() {
    }
}

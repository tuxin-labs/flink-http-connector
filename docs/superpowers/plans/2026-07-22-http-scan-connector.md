# http-scan Flink SQL 连接器实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `getindata/flink-http-connector` 项目内新增 `http-scan` Flink SQL 连接器，支持通过 `CREATE TABLE ... WITH ('connector'='http-scan')` + `INSERT INTO sink SELECT * FROM http_scan_table` 读取 HTTP 接口数据，覆盖无参拉全量、带参调用、页码分页、游标分页四种场景。

**Architecture:** 基于 Flink FLIP-27 新版 Source API（`Source` + `SplitEnumerator` + `SourceReader`），声明 `Boundedness.BOUNDED`，单并行度串行分页拉取。配置走 `gid.connector.http.scan.*` 前缀，复用项目已有的 security/auth/status/HttpClientWithRetry/HttpLogger 能力。与现有 `rest-lookup`、`http-sink` 完全隔离，零行为变更。

**Tech Stack:** Java 11、Flink 1.18.1（运行时兼容 1.17.x/1.18.x）、JUnit 5 + AssertJ + Mockito + WireMock 3.13.2、resilience4j-retry、Jackson、flink-json、Lombok。

## Global Constraints

（从 spec 原样抄录，每个任务的需求隐含包含本节）

- **包路径根**：`com.getindata.connectors.http.internal.table.scan`（测试镜像到 `src/test/.../table/scan`）。
- **连接器标识**：`http-scan`，注册到 `META-INF/services/org.apache.flink.table.factories.Factory`（仅新增一行）。
- **配置前缀**：共性短名（`connector`/`url`/`format`/`method`）；扩展项前缀 `gid.connector.http.scan.*`；安全/日志沿用现有 key（`gid.connector.http.security.*`、`gid.connector.http.logging.level`）。
- **数据类型**：完全依赖 `flink-json`，不自造类型系统。
- **分页停止策略优先级**（任一命中即停）：`total-pages` → `total-count-jsonpath`（`rowsSeenTotal + rowsInPage >= total`）→ `has-more-jsonpath` 为 false/null → `batch-size`（本页行数 < batch-size）→ 游标为空。
- **并行度**：v1 单并行度。`SplitEnumerator` 只发一个 `HttpScanSplit`。
- **checkpoint**：不支持断点续传；`HttpScanSplitSerializer` 提供最小实现。失败即作业失败。
- **不支持** `continue-on-error`。
- **不破坏** lookup/sink 的任何现有类、配置项、行为。**唯一**对共享代码的变更是 Task 12：把 `JavaNetHttpClientFactory.getSslContext(Properties)` 从 `private` 提为 `public static`（纯增量、无行为变更）。
- **覆盖率门槛**：JaCoCo `line ≥ 90% / branch ≥ 90% / method ≥ 80%`（项目硬门槛）。
- **checkstyle**：`dev/checkstyle.xml`，含测试源码；每个任务结束前确保 `mvn validate` 通过。
- **提交纪律**：每个任务一个 commit，commit message 用 `[HTTP-SCAN]` 前缀；分支 `feat/http-scan-design`。

---

## File Structure

### 新增主源码（`src/main/java/com/getindata/connectors/http/internal/table/scan/`）

| 文件 | 职责 | 依赖 |
|---|---|---|
| `HttpScanConnectorOptions.java` | 所有 `ConfigOption` 常量集中定义 | 无 |
| `HttpScanConfig.java` | 不可变 POJO，承载运行期配置（含 Properties、ReadableConfig、分页参数） | HttpScanConnectorOptions |
| `request/PlaceholderResolver.java` | `${page}`/`${cursor}` 占位符替换 + `{path_var}` 替换 + URL 编码 | 无 |
| `request/ScanRequestTemplate.java` | 从 config + pagination state 装配 `java.net.http.HttpRequest` | PlaceholderResolver |
| `response/JsonPathExtractor.java` | JSONPath 剥壳 + 抽 total-count/has-more/cursor | Jackson |
| `pagination/PaginationState.java` | 不可变分页状态（pageNumber/cursor/rowsSeenTotal/requestCount） | 无 |
| `pagination/PaginationStrategy.java` | 分页策略接口 | PaginationState, ScanRequestTemplate |
| `pagination/NoPagination.java` | 单次请求策略 | PaginationStrategy |
| `pagination/PageNumberPagination.java` | 页码分页（4 种停止策略 + 优先级） | PaginationStrategy, JsonPathExtractor |
| `pagination/CursorPagination.java` | 游标分页 | PaginationStrategy, JsonPathExtractor |
| `source/HttpScanSplit.java` | 单一 split（携带初始 PaginationState） | PaginationState |
| `source/HttpScanSplitSerializer.java` | Split 序列化器（最小实现） | HttpScanSplit |
| `source/HttpScanSplitEnumerator.java` | 只发一个 split | HttpScanSplit |
| `source/HttpScanSourceReader.java` | 主循环：nextRequest→send→extract→deserialize→collect→updateState | HttpClientWithRetry, JsonPathExtractor, DeserializationSchema, PaginationStrategy |
| `source/HttpScanRetryConfigProvider.java` | 从 scan ReadableConfig 构造 resilience4j `RetryConfig` | HttpScanConnectorOptions |
| `source/HttpScanHttpClientFactory.java` | 构造 `HttpClient`（复用 `getSslContext`）+ proxy/timeout | SecurityContext（经 JavaNetHttpClientFactory） |
| `source/HttpScanSource.java` | FLIP-27 入口，`Boundedness.BOUNDED` | 所有 source/* |
| `HttpScanDynamicTableSource.java` | `ScanTableSource` 实现，投影下推，产出 SourceProvider | HttpScanSource, HttpScanConfig |
| `HttpScanTableSourceFactory.java` | `DynamicTableSourceFactory`，校验规则 1-13 | HttpScanConnectorOptions, HttpScanConfig, HttpScanDynamicTableSource |
| `metadata/HttpScanMetadata.java` | 3 个 metadata 列定义 + converter（http-status-code/http-headers-map/page-number） | 无 |

### 新增测试（`src/test/java/com/getindata/connectors/http/internal/table/scan/`）

每个主类对应一个 `*Test`（单元，mock 网络）或 `*IT`（集成，WireMock）。端到端：`HttpScanTableSourceITCase`（Flink MiniCluster + WireMock）。

### 修改的现有文件（最小）

| 文件 | 变更 | 风险 |
|---|---|---|
| `internal/utils/JavaNetHttpClientFactory.java:104` | `getSslContext` 提为 `public static` | 无（纯可见性放宽） |
| `META-INF/services/org.apache.flink.table.factories.Factory` | 追加 `HttpScanTableSourceFactory` 一行 | 无 |
| `pom.xml:373-379` | JaCoCo excludes 追加 `**/HttpScanConnectorOptions.class` | 无 |
| `README.md` | 新增 `### HTTP Scan Source` 章节 + 选项表 + Breaking changes 一段 | 无 |
| `CHANGELOG.md` | 新增 0.27.0 段 | 无 |

---

## Task 1: HttpScanConnectorOptions

**Files:**
- Create: `src/main/java/com/getindata/connectors/http/internal/table/scan/HttpScanConnectorOptions.java`
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/HttpScanConnectorOptionsTest.java`

**Interfaces:**
- Consumes: 无
- Produces: 一组 `public static final ConfigOption<?>`（见下方完整代码）；后续任务按字段名引用，如 `HttpScanConnectorOptions.URL`、`PAGINATION_TYPE` 等。

- [ ] **Step 1: 写失败测试**

```java
package com.getindata.connectors.http.internal.table.scan;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HttpScanConnectorOptionsTest {

    @Test
    void shouldDefineCoreOptions() {
        assertThat(HttpScanConnectorOptions.URL.key()).isEqualTo("url");
        assertThat(HttpScanConnectorOptions.METHOD.key()).isEqualTo("method");
        assertThat(HttpScanConnectorOptions.METHOD.defaultValue()).isEqualTo("GET");
        assertThat(HttpScanConnectorOptions.QUERY_PARAMS.key())
            .isEqualTo("gid.connector.http.scan.query-params");
    }

    @Test
    void shouldDefinePaginationOptions() {
        assertThat(HttpScanConnectorOptions.PAGINATION_TYPE.key())
            .isEqualTo("gid.connector.http.scan.pagination.type");
        assertThat(HttpScanConnectorOptions.PAGINATION_TYPE.defaultValue()).isEqualTo("none");
        assertThat(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE.key())
            .isEqualTo("gid.connector.http.scan.pagination.batch-size");
    }

    @Test
    void shouldDefineBodyAndContentOptions() {
        assertThat(HttpScanConnectorOptions.BODY.key()).isEqualTo("gid.connector.http.scan.body");
        assertThat(HttpScanConnectorOptions.CONTENT_FIELD.key())
            .isEqualTo("gid.connector.http.scan.content-field");
        assertThat(HttpScanConnectorOptions.BODY_CONTENT_TYPE.defaultValue())
            .isEqualTo("application/json");
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd D:/code/flink-http-connector && mvn -q -Dtest=HttpScanConnectorOptionsTest test`
Expected: 编译失败（类不存在）。

- [ ] **Step 3: 写实现**（完整代码，所有 ConfigOption）

```java
package com.getindata.connectors.http.internal.table.scan;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * 所有 http-scan 连接器的 {@link ConfigOption} 集中定义。
 * key 前缀遵循 gid.connector.http.scan.*（共性短名除外），与 lookup/sink 并列。
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
        ConfigOptions.key(PAGINATION_PREFIX + "cursor-response-jsonpath").stringType().noDefaultValue()
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
        ConfigOptions.key(SCAN_PREFIX + "proxy.host").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PROXY_PORT =
        ConfigOptions.key(SCAN_PREFIX + "proxy.port").intType().noDefaultValue();

    public static final ConfigOption<String> PROXY_USERNAME =
        ConfigOptions.key(SCAN_PREFIX + "proxy.username").stringType().noDefaultValue();

    public static final ConfigOption<String> PROXY_PASSWORD =
        ConfigOptions.key(SCAN_PREFIX + "proxy.password").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> USE_RAW_AUTH_HEADER =
        ConfigOptions.key(SCAN_PREFIX + "use-raw-authorization-header").booleanType()
            .defaultValue(false)
            .withDescription("Basic Auth 原样透传 Authorization header。");

    // ---- OIDC（key 沿用 security.oidc.*，由 Properties 路径读取） ----
    public static final ConfigOption<String> OIDC_TOKEN_ENDPOINT_URL =
        ConfigOptions.key("gid.connector.http.security.oidc.token.endpoint.url")
            .stringType().noDefaultValue()
            .withDescription("OIDC Token 端点 URL。");

    public static final ConfigOption<String> OIDC_TOKEN_REQUEST =
        ConfigOptions.key("gid.connector.http.security.oidc.token.request")
            .stringType().noDefaultValue()
            .withDescription("OIDC Token 请求体（application/x-www-form-urlencoded）。");

    private HttpScanConnectorOptions() {
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -q -Dtest=HttpScanConnectorOptionsTest test`
Expected: PASS（3 个用例）。

- [ ] **Step 5: checkstyle**

Run: `mvn -q validate`
Expected: BUILD SUCCESS。

- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/HttpScanConnectorOptions.java src/test/java/com/getindata/connectors/http/internal/table/scan/HttpScanConnectorOptionsTest.java
git commit -m "[HTTP-SCAN] Add HttpScanConnectorOptions with all ConfigOptions"
```

---

## Task 2: PaginationState

**Files:**
- Create: `src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/PaginationState.java`
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/PaginationStateTest.java`

**Interfaces:**
- Consumes: 无
- Produces: 不可变状态类，字段 `pageNumber(int)`、`cursor(String)`、`rowsSeenTotal(long)`、`requestCount(int)`；提供 `initialPage(int)`、`initialCursor(String)` 静态工厂，以及 `nextPage()`、`withCursor(String)`、`accumulate(int rowsInPage)`、`incrementRequest()` 等返回新实例的方法。后续 Task 6/7/8 依赖这些方法名。

- [ ] **Step 1: 写失败测试**

```java
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
    }

    @Test
    void shouldCreateInitialCursorState() {
        var s = PaginationState.initialCursor("abc");
        assertThat(s.getCursor()).isEqualTo("abc");
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
}
```

- [ ] **Step 2: 运行确认失败**

Run: `mvn -q -Dtest=PaginationStateTest test` → 编译失败。

- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 不可变分页状态。每次推进都返回新实例。
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
```

- [ ] **Step 4: 运行确认通过**

Run: `mvn -q -Dtest=PaginationStateTest test` → PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/PaginationState.java src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/PaginationStateTest.java
git commit -m "[HTTP-SCAN] Add immutable PaginationState"
```

---

## Task 3: PlaceholderResolver

**Files:**
- Create: `src/main/java/com/getindata/connectors/http/internal/table/scan/request/PlaceholderResolver.java`
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/request/PlaceholderResolverTest.java`

**Interfaces:**
- Consumes: 无
- Produces:
  - `static Map<String,String> parseUrlVars(String raw)` — 解析 `key1:v1,key2:v2` 成 Map（null/空 → 空 Map）
  - `static String resolvePathVars(String url, Map<String,String> urlVars)` — 替换 `{name}`
  - `static String replace(String template, Map<String,String> values)` — 替换 `${key}` 占位符（值为 null 则替换为空串）
  - `static String encodeQueryValue(String value)` — `URLEncoder.encode(value, UTF_8)`

- [ ] **Step 1: 写失败测试**

```java
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
        assertThat(PlaceholderResolver.replace("c=${cursor}", Map.of("cursor", null)))
            .isEqualTo("c=");
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
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.request;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class PlaceholderResolver {

    public static Map<String, String> parseUrlVars(String raw) {
        Map<String, String> map = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return map;
        }
        for (String pair : raw.split(",")) {
            String[] kv = pair.split(":", 2);
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
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
```

- [ ] **Step 4: 运行确认通过** → 7 个用例 PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/request/PlaceholderResolver.java src/test/java/com/getindata/connectors/http/internal/table/scan/request/PlaceholderResolverTest.java
git commit -m "[HTTP-SCAN] Add PlaceholderResolver for url-vars/path/page/cursor substitution"
```

---

## Task 4: JsonPathExtractor

**Files:**
- Create: `src/main/java/com/getindata/connectors/http/internal/table/scan/response/JsonPathExtractor.java`
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/response/JsonPathExtractorTest.java`

**Interfaces:**
- Consumes: Jackson（项目已依赖 `jackson-databind` + `jackson-core`）。**注意**：项目未引入 `jayway/JsonPath` 依赖，因此本类基于 Jackson 自行解析。`content-field` 语法限定为：
  - `$` 表示整个响应体（顶层）；
  - `$.<field>` 单层取字段；
  - `$.a.b.c` 多层取字段（点分）；
  - 末尾不带 `.*` 时取到的是单个对象/标量；带 `.*` 等价于取该字段的数组元素集合。
  实现用 Jackson `JsonNode` 递归按点分路径定位，避免引入新依赖（CLAUDE.md §2.8 稳定性优先：成熟方案 > 激进方案）。
- Produces:
  - `static List<byte[]> extractRecords(String body, String contentField)` — 剥壳出记录字节列表；`contentField=null` 时按顶层是数组/对象处理；解析失败抛 `JsonPathExtractionException`
  - `static Long extractLong(String body, String jsonPath)` — 抽数字（total-count）
  - `static Boolean extractBoolean(String body, String jsonPath)` — 抽布尔（has-more）
  - `static String extractString(String body, String jsonPath)` — 抽字符串（cursor）
  - `static void compileCheck(String jsonPath)` — 编译期校验 JSONPath 合法性（Factory 用）

- [ ] **Step 1: 写失败测试**

```java
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
        assertThatThrownBy(() -> JsonPathExtractor.compileCheck(null))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 基于 Jackson 的轻量 JSONPath 抽取器，避免引入 jayway/JsonPath 依赖。
 * 支持语法：$、$.a、$.a.b.c（点分路径）。剥壳到数组时逐元素返回。
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
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asLong();
    }

    public static Boolean extractBoolean(String body, String jsonPath) {
        JsonNode node = navigate(parse(body), jsonPath);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        return node.asBoolean();
    }

    public static String extractString(String body, String jsonPath) {
        JsonNode node = navigate(parse(body), jsonPath);
        if (node == null || node.isNull()) {
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
            if (segment.isBlank()) {
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
```

并创建异常类 `JsonPathExtractionException.java`：

```java
package com.getindata.connectors.http.internal.table.scan.response;

public class JsonPathExtractionException extends RuntimeException {
    public JsonPathExtractionException(String message) { super(message); }
    public JsonPathExtractionException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 4: 运行确认通过** → 11 个用例 PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/response/ src/test/java/com/getindata/connectors/http/internal/table/scan/response/
git commit -m "[HTTP-SCAN] Add Jackson-based JsonPathExtractor for content extraction"
```

---

## Task 5: HttpScanConfig

**Files:**
- Create: `src/main/java/com/getindata/connectors/http/internal/table/scan/HttpScanConfig.java`
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/HttpScanConfigTest.java`

**Interfaces:**
- Consumes: `HttpScanConnectorOptions`，`PlaceholderResolver.parseUrlVars`
- Produces: 不可变 POJO（Lombok `@Value` + `@Builder`），字段：
  - `String url`, `String method`, `Map<String,String> urlVars`, `String queryParamsTemplate`, `String bodyTemplate`, `String bodyContentType`, `String contentField`
  - `String paginationType`, `String pageField`, `int startPage`, `Integer batchSize`, `Integer totalPages`, `String totalCountJsonPath`, `String hasMoreJsonPath`, `String cursorField`, `String cursorResponseJsonPath`, `String initialCursor`
  - `Properties properties`（含 `gid.connector.http.*`），`ReadableConfig readableConfig`
  - 静态工厂 `static HttpScanConfig from(ReadableConfig readable, Properties properties)`
- `urlVars` 由 `parseUrlVars(readable.get(URL_VARS))` 预解析为 Map。

- [ ] **Step 1: 写失败测试**

```java
package com.getindata.connectors.http.internal.table.scan;

import java.util.HashMap;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

class HttpScanConfigTest {

    @Test
    void shouldBuildFromReadableConfig() {
        var conf = new Configuration();
        conf.set(URL, "https://x/{cid}/orders");
        conf.set(URL_VARS, "cid:C1");
        conf.set(METHOD, "POST");
        conf.set(QUERY_PARAMS, "page=${page}");
        conf.set(PAGINATION_TYPE, "page-number");
        conf.set(PAGINATION_BATCH_SIZE, 100);

        var cfg = HttpScanConfig.from(conf, new java.util.Properties());
        assertThat(cfg.getUrl()).isEqualTo("https://x/{cid}/orders");
        assertThat(cfg.getMethod()).isEqualTo("POST");
        assertThat(cfg.getUrlVars()).containsEntry("cid", "C1");
        assertThat(cfg.getPaginationType()).isEqualTo("page-number");
        assertThat(cfg.getBatchSize()).isEqualTo(100);
    }

    @Test
    void shouldApplyDefaults() {
        var conf = new Configuration();
        conf.set(URL, "https://x");
        var cfg = HttpScanConfig.from(conf, new java.util.Properties());
        assertThat(cfg.getMethod()).isEqualTo("GET");
        assertThat(cfg.getPaginationType()).isEqualTo("none");
        assertThat(cfg.getBatchSize()).isNull();
        assertThat(cfg.getUrlVars()).isEmpty();
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan;

import java.util.Map;
import java.util.Properties;

import lombok.Builder;
import lombok.Value;
import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.table.scan.request.PlaceholderResolver;

import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

@Value
@Builder
public class HttpScanConfig {

    String url;
    String method;
    Map<String, String> urlVars;
    String queryParamsTemplate;
    String bodyTemplate;
    String bodyContentType;
    String contentField;

    String paginationType;
    String pageField;
    int startPage;
    Integer batchSize;
    Integer totalPages;
    String totalCountJsonPath;
    String hasMoreJsonPath;
    String cursorField;
    String cursorResponseJsonPath;
    String initialCursor;

    Properties properties;
    ReadableConfig readableConfig;

    public static HttpScanConfig from(ReadableConfig readable, Properties properties) {
        return HttpScanConfig.builder()
            .url(readable.get(URL))
            .method(readable.get(METHOD))
            .urlVars(PlaceholderResolver.parseUrlVars(readable.get(URL_VARS)))
            .queryParamsTemplate(readable.get(QUERY_PARAMS))
            .bodyTemplate(readable.get(BODY))
            .bodyContentType(readable.get(BODY_CONTENT_TYPE))
            .contentField(readable.get(CONTENT_FIELD))
            .paginationType(readable.get(PAGINATION_TYPE))
            .pageField(readable.get(PAGINATION_PAGE_FIELD))
            .startPage(readable.get(PAGINATION_START_PAGE))
            .batchSize(readable.getOptional(PAGINATION_BATCH_SIZE).orElse(null))
            .totalPages(readable.getOptional(PAGINATION_TOTAL_PAGES).orElse(null))
            .totalCountJsonPath(readable.get(PAGINATION_TOTAL_COUNT_JSONPATH))
            .hasMoreJsonPath(readable.get(PAGINATION_HAS_MORE_JSONPATH))
            .cursorField(readable.get(PAGINATION_CURSOR_FIELD))
            .cursorResponseJsonPath(readable.get(PAGINATION_CURSOR_RESPONSE_JSONPATH))
            .initialCursor(readable.get(PAGINATION_INITIAL_CURSOR))
            .properties(properties)
            .readableConfig(readable)
            .build();
    }
}
```

- [ ] **Step 4: 运行确认通过** → 2 个用例 PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/HttpScanConfig.java src/test/java/com/getindata/connectors/http/internal/table/scan/HttpScanConfigTest.java
git commit -m "[HTTP-SCAN] Add HttpScanConfig built from ReadableConfig"
```

---

## Task 6: PaginationStrategy interface + NoPagination

**Files:**
- Create: `pagination/PaginationStrategy.java`, `pagination/NoPagination.java`, `pagination/StopDecision.java`
- Test: `pagination/NoPaginationTest.java`

**Interfaces:**
- Consumes: `PaginationState`, `HttpScanConfig`, `JsonPathExtractor`
- Produces:
  - `StopDecision`：值对象，`boolean shouldStop`、`PaginationState nextState`；静态 `continueWith(state)` / `stop()`
  - `PaginationStrategy`（接口）：
    - `PaginationState initialState()`
    - `java.util.Optional<java.net.URI> nextUri(PaginationState state, HttpScanConfig config)` — empty 表示结束（不再构造请求）。URI 由 strategy 与 `ScanRequestTemplate` 合作产出。为降低耦合，本接口改为返回请求描述而非已装配 HttpRequest；装配在 Reader 内统一进行。
    - 重新设计接口签名（保持简单）：
      ```java
      public interface PaginationStrategy {
          PaginationState initialState();
          /** 返回当前 state 对应的页码/游标占位符取值；empty 表示无更多请求。 */
          java.util.Optional<java.util.Map<String,String>> nextRequestValues(PaginationState state, HttpScanConfig config);
          /** 根据本页响应与行数，返回停止决策与新状态。 */
          StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config);
      }
      ```

- [ ] **Step 1: 写失败测试**（NoPagination）

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class NoPaginationTest {

    private final NoPagination strategy = new NoPagination();

    @Test
    void shouldProduceOneRequestThenStop() {
        var cfg = HttpScanConfigFixtures.noPaginationConfig();
        var state = strategy.initialState();
        var first = strategy.nextRequestValues(state, cfg);
        assertThat(first).isPresent();

        var after = strategy.afterResponse(state, "[{\"id\":1}]", 1, cfg);
        assertThat(after.isShouldStop()).isTrue();
        assertThat(strategy.nextRequestValues(after.getNextState(), cfg)).isEmpty();
    }
}
```

（`HttpScanConfigFixtures` 是测试辅助类，提供预构造的 `HttpScanConfig`。在 Task 6 测试目录下创建。）

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

`StopDecision.java`:
```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public final class StopDecision {
    private final boolean shouldStop;
    private final PaginationState nextState;

    public static StopDecision continueWith(PaginationState state) { return new StopDecision(false, state); }
    public static StopDecision stop(PaginationState state) { return new StopDecision(true, state); }
}
```

`PaginationStrategy.java`:
```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

public interface PaginationStrategy {
    PaginationState initialState();
    Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config);
    StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config);
}
```

`NoPagination.java`:
```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

public class NoPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState() {
        return PaginationState.initialPage(0);
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        // 仅在 requestCount == 0 时产出一次请求
        if (state.getRequestCount() == 0) {
            return Optional.of(Map.of());
        }
        return Optional.empty();
    }

    @Override
    public StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        return StopDecision.stop(state.incrementRequest());
    }
}
```

测试辅助 `HttpScanConfigFixtures.java`（测试目录）：
```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Properties;
import org.apache.flink.configuration.Configuration;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

public final class HttpScanConfigFixtures {
    private HttpScanConfigFixtures() {}

    public static HttpScanConfig noPaginationConfig() {
        var conf = new Configuration();
        conf.set(URL, "https://x/items");
        conf.set(FORMAT_DUMMY, "json"); // FORMAT 是 FactoryUtil 常量，config 不需要；占位忽略
        return HttpScanConfig.from(conf, new Properties());
    }
}
```

> **修正**：`FORMAT` 来自 `FactoryUtil.FORMAT`，不属于 `HttpScanConnectorOptions`。`HttpScanConfig` 不读 FORMAT。测试 fixture 移除 `FORMAT_DUMMY` 那一行，仅设 URL。实现时以修正版为准。

- [ ] **Step 4: 运行确认通过** → NoPaginationTest PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/PaginationStrategy.java src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/StopDecision.java src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/NoPagination.java src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/NoPaginationTest.java src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/HttpScanConfigFixtures.java
git commit -m "[HTTP-SCAN] Add PaginationStrategy interface, StopDecision, NoPagination"
```

---

## Task 7: PageNumberPagination

**Files:**
- Create: `pagination/PageNumberPagination.java`
- Test: `pagination/PageNumberPaginationTest.java`

**Interfaces:**
- Consumes: `PaginationStrategy`, `PaginationState`, `HttpScanConfig`（读 `pageField`/`startPage`/`batchSize`/`totalPages`/`totalCountJsonPath`/`hasMoreJsonPath`）, `JsonPathExtractor`
- Produces: `nextRequestValues` 返回 `Map.of(pageField, String.valueOf(pageNumber))`；`afterResponse` 按优先级判定停止。

- [ ] **Step 1: 写失败测试**（覆盖 4 种停止策略 + 优先级）

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PageNumberPaginationTest {

    @Test
    void shouldIncrementPageNumber() {
        var cfg = Fixtures.pageNumberConfig(null, null, null);
        var s = new PageNumberPagination();
        var st = s.initialState();
        assertThat(s.nextRequestValues(st, cfg)).containsEntry("page", "1");
        var after = s.afterResponse(st, "[]", 100, cfg);
        assertThat(s.nextRequestValues(after.getNextState(), cfg)).containsEntry("page", "2");
    }

    @Test
    void shouldStopOnBatchSize() {
        var cfg = Fixtures.pageNumberConfig(100, null, null);
        var s = new PageNumberPagination();
        var st = s.initialState();
        var after = s.afterResponse(st, "[]", 50, cfg);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopOnTotalPages() {
        var cfg = Fixtures.pageNumberConfig(null, 3, null);
        var s = new PageNumberPagination();
        var st = s.initialState();
        for (int i = 0; i < 2; i++) {
            st = s.afterResponse(st, "[]", 100, cfg).getNextState();
            assertThat(s.afterResponse(st, "[]", 100, cfg).isShouldStop()).isFalse();
        }
        st = s.afterResponse(st, "[]", 100, cfg).getNextState(); // 第3页
        assertThat(s.afterResponse(st, "[]", 100, cfg).isShouldStop()).isTrue();
    }

    @Test
    void shouldStopOnTotalCount() {
        var cfg = Fixtures.pageNumberConfig(null, null, "$.total");
        var s = new PageNumberPagination();
        var st = s.initialState();
        // 第一页100条，total=150，未到
        var after = s.afterResponse(st, "{\"total\":150}", 100, cfg);
        assertThat(after.isShouldStop()).isFalse();
        // 第二页50条，累计150 >= 150，停
        var after2 = s.afterResponse(after.getNextState(), "{\"total\":150}", 50, cfg);
        assertThat(after2.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopOnHasMoreFalse() {
        var cfg = Fixtures.pageNumberConfig(null, null, null);
        // 通过 fixture 设 hasMoreJsonPath
        var s = new PageNumberPagination();
        var st = s.initialState();
        var after = s.afterResponse(st, "{\"hasMore\":false}", 100, cfg);
        // 该 fixture 未设 hasMoreJsonPath → 不停
        assertThat(after.isShouldStop()).isFalse();
    }

    static class Fixtures {
        static com.getindata.connectors.http.internal.table.scan.HttpScanConfig pageNumberConfig(
                Integer batchSize, Integer totalPages, String totalCountJsonPath) {
            var conf = new org.apache.flink.configuration.Configuration();
            conf.set(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.URL, "https://x");
            conf.set(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.PAGINATION_TYPE, "page-number");
            if (batchSize != null) conf.set(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.PAGINATION_BATCH_SIZE, batchSize);
            if (totalPages != null) conf.set(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES, totalPages);
            if (totalCountJsonPath != null) conf.set(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH, totalCountJsonPath);
            return com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new java.util.Properties());
        }
    }
}
```

> 注：`shouldStopOnHasMoreFalse` 用例需配 `PAGINATION_HAS_MORE_JSONPATH`。实现时补一个带 hasMore 的 fixture 重载，断言为 true。以实现时为准，保证覆盖 has-more 分支。

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

public class PageNumberPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState() {
        return PaginationState.initialPage(1);
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        return Optional.of(Map.of(config.getPageField(), String.valueOf(state.getPageNumber())));
    }

    @Override
    public StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        PaginationState updated = state.incrementRequest().accumulate(rowsInPage);

        // 优先级1: total-pages（已发请求达到总数即停）
        if (config.getTotalPages() != null && updated.getRequestCount() >= config.getTotalPages()) {
            return StopDecision.stop(updated);
        }
        // 优先级2: total-count-jsonpath
        if (config.getTotalCountJsonPath() != null) {
            Long total = JsonPathExtractor.extractLong(responseBody, config.getTotalCountJsonPath());
            if (total != null && updated.getRowsSeenTotal() >= total) {
                return StopDecision.stop(updated);
            }
        }
        // 优先级3: has-more-jsonpath
        if (config.getHasMoreJsonPath() != null) {
            Boolean hasMore = JsonPathExtractor.extractBoolean(responseBody, config.getHasMoreJsonPath());
            if (hasMore == null || !hasMore) {
                return StopDecision.stop(updated);
            }
        }
        // 优先级4: batch-size
        if (config.getBatchSize() != null && rowsInPage < config.getBatchSize()) {
            return StopDecision.stop(updated);
        }
        return StopDecision.continueWith(updated.nextPage());
    }
}
```

- [ ] **Step 4: 运行确认通过** → 5 个用例 PASS（补 hasMore fixture 后）。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/PageNumberPagination.java src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/PageNumberPaginationTest.java
git commit -m "[HTTP-SCAN] Add PageNumberPagination with priority-based stop strategies"
```

---

## Task 8: CursorPagination

**Files:**
- Create: `pagination/CursorPagination.java`
- Test: `pagination/CursorPaginationTest.java`

**Interfaces:**
- Consumes: `PaginationStrategy`, `JsonPathExtractor.extractString`（按 `cursorResponseJsonPath`）
- Produces:
  - `initialState()` = `PaginationState.initialCursor(config.getInitialCursor())`
  - `nextRequestValues`: 当 `requestCount == 0` 总是产出（用 initialCursor）；之后仅当 `cursor != null && !cursor.isBlank()` 才产出；返回 `Map.of(cursorField, cursor)`
  - `afterResponse`: 抽 `cursorResponseJsonPath` 得到 nextCursor；若为 null/空 → stop；否则 continueWith(state.withCursor(nextCursor).accumulate(rowsInPage))

- [ ] **Step 1: 写失败测试**（正常流程 + cursor 为 null/空/缺失 三种停）

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

class CursorPaginationTest {

    private final CursorPagination strategy = new CursorPagination();

    private com.getindata.connectors.http.internal.table.scan.HttpScanConfig cfg(String initial, String respPath) {
        var conf = new org.apache.flink.configuration.Configuration();
        conf.set(URL, "https://x");
        conf.set(PAGINATION_TYPE, "cursor");
        conf.set(PAGINATION_CURSOR_RESPONSE_JSONPATH, respPath);
        conf.set(PAGINATION_INITIAL_CURSOR, initial);
        return com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new java.util.Properties());
    }

    @Test
    void shouldContinueUntilCursorEmpty() {
        var c = cfg("", "$.next");
        var st = strategy.initialState();
        assertThat(strategy.nextRequestValues(st, c)).containsEntry("cursor", "");

        var after1 = strategy.afterResponse(st, "{\"next\":\"page2\"}", 10, c);
        assertThat(after1.isShouldStop()).isFalse();
        assertThat(after1.getNextState().getCursor()).isEqualTo("page2");

        var after2 = strategy.afterResponse(after1.getNextState(), "{\"next\":\"\"}", 10, c);
        assertThat(after2.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopWhenCursorMissing() {
        var c = cfg("", "$.next");
        var st = strategy.initialState();
        var after = strategy.afterResponse(st, "{}", 10, c);
        assertThat(after.isShouldStop()).isTrue();
    }

    @Test
    void shouldStopWhenCursorNull() {
        var c = cfg("", "$.next");
        var st = strategy.initialState();
        var after = strategy.afterResponse(st, "{\"next\":null}", 10, c);
        assertThat(after.isShouldStop()).isTrue();
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

public class CursorPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState() {
        return null; // 占位，实现见下
    }

    @Override
    public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
        return null; // 占位
    }

    @Override
    public StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
        return null; // 占位
    }
}
```

> **重要（无占位符原则）**：上面的占位实现仅用于说明结构；实际实现（无 TODO）：

```java
package com.getindata.connectors.http.internal.table.scan.pagination;

import java.util.Map;
import java.util.Optional;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;

public class CursorPagination implements PaginationStrategy {

    @Override
    public PaginationState initialState() {
        throw new UnsupportedOperationException("initialState 需要 config；改由工厂方法创建");
    }
}
```

> **接口调整决定**：`initialState()` 不带 config 无法设 initialCursor。将 `PaginationStrategy.initialState()` 改为 `initialState(HttpScanConfig config)`。回到 Task 6 修正接口签名（一处），并同步修正 NoPagination/PageNumberPagination 的 `initialState(config)`（PageNumber 不读 config，NoPagination 不读 config，但签名一致）。在 Task 8 完成时一并执行这个接口修正。

完整 CursorPagination（无占位）：

```java
@Override
public PaginationState initialState(HttpScanConfig config) {
    return PaginationState.initialCursor(config.getInitialCursor());
}

@Override
public Optional<Map<String, String>> nextRequestValues(PaginationState state, HttpScanConfig config) {
    if (state.getRequestCount() > 0) {
        String cur = state.getCursor();
        if (cur == null || cur.isBlank()) {
            return Optional.empty();
        }
    }
    String cur = state.getCursor() == null ? "" : state.getCursor();
    return Optional.of(Map.of(config.getCursorField(), cur));
}

@Override
public StopDecision afterResponse(PaginationState state, String responseBody, int rowsInPage, HttpScanConfig config) {
    String next = JsonPathExtractor.extractString(responseBody, config.getCursorResponseJsonPath());
    if (next == null || next.isBlank()) {
        return StopDecision.stop(state.incrementRequest().accumulate(rowsInPage));
    }
    return StopDecision.continueWith(state.withCursor(next).incrementRequest().accumulate(rowsInPage));
}
```

- [ ] **Step 4: 运行确认通过** → 3 个用例 PASS（含接口修正后回归 NoPagination/PageNumberPagination）。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add -A src/main/java/com/getindata/connectors/http/internal/table/scan/pagination/ src/test/java/com/getindata/connectors/http/internal/table/scan/pagination/
git commit -m "[HTTP-SCAN] Add CursorPagination; refactor initialState(config) signature"
```

---

## Task 9: ScanRequestTemplate

**Files:**
- Create: `request/ScanRequestTemplate.java`
- Test: `request/ScanRequestTemplateTest.java`

**Interfaces:**
- Consumes: `HttpScanConfig`, `PlaceholderResolver`
- Produces:
  - 构造：`new ScanRequestTemplate(HttpScanConfig config)`
  - `java.net.http.HttpRequest build(Map<String,String> requestValues)` — 装配最终 HttpRequest：
    1. `resolvePathVars(config.url, config.urlVars)` → 基础 URL
    2. 若 `queryParamsTemplate != null`：`PlaceholderResolver.replace(queryParamsTemplate, requestValues)`，再按 `&` split、按 `=` split 成键值，对 value 做 `encodeQueryValue`，拼成 `?k1=v1&k2=v2` 追加到 URL
    3. 若 `bodyTemplate != null`：`PlaceholderResolver.replace(bodyTemplate, requestValues)` 作为 bodyPublisher
    4. `HttpRequest.newBuilder(URI).header(Content-Type, bodyContentType).method(...)`；GET 不带 body
    5. 返回 `HttpRequest`

- [ ] **Step 1: 写失败测试**

```java
package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URI;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

class ScanRequestTemplateTest {

    @Test
    void shouldBuildGetWithQueryParamsAndPathVars() throws Exception {
        var conf = new Configuration();
        conf.set(URL, "https://x/{cid}/orders");
        conf.set(URL_VARS, "cid:C1");
        conf.set(QUERY_PARAMS, "page=${page}&size=100");
        var cfg = com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new java.util.Properties());
        var template = new ScanRequestTemplate(cfg);

        var req = template.build(Map.of("page", "3"));
        assertThat(req.uri()).isEqualTo(new URI("https://x/C1/orders?page=3&size=100"));
        assertThat(req.method()).isEqualTo("GET");
    }

    @Test
    void shouldBuildPostWithBody() {
        var conf = new Configuration();
        conf.set(URL, "https://x/search");
        conf.set(METHOD, "POST");
        conf.set(BODY, "{\"page\":${page}}");
        var cfg = com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new java.util.Properties());
        var template = new ScanRequestTemplate(cfg);

        var req = template.build(Map.of("page", "2"));
        assertThat(req.method()).isEqualTo("POST");
        var bodyPublisher = req.bodyPublisher().orElseThrow();
        // bodyPublisher content length 校验（简化）
        assertThat(bodyPublisher.contentLength()).isGreaterThan(0);
    }

    @Test
    void shouldEncodeQueryValueWithSpaces() throws Exception {
        var conf = new Configuration();
        conf.set(URL, "https://x");
        conf.set(QUERY_PARAMS, "q=${q}");
        var cfg = com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new java.util.Properties());
        var req = new ScanRequestTemplate(cfg).build(Map.of("q", "a b"));
        assertThat(req.uri()).isEqualTo(new URI("https://x?q=a+b"));
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

@RequiredArgsConstructor
public class ScanRequestTemplate {

    private final HttpScanConfig config;

    public HttpRequest build(Map<String, String> requestValues) {
        String baseUrl = PlaceholderResolver.resolvePathVars(config.getUrl(), config.getUrlVars());

        String url = appendQueryParams(baseUrl, requestValues);
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url));

        if (config.getBodyTemplate() != null) {
            String body = PlaceholderResolver.replace(config.getBodyTemplate(), requestValues);
            BodyPublisher publisher = BodyPublishers.ofString(body, StandardCharsets.UTF_8);
            builder.header("Content-Type", config.getBodyContentType());
            builder.method(config.getMethod(), publisher);
        } else {
            builder.method(config.getMethod(), BodyPublishers.noBody());
        }
        return builder.build();
    }

    private String appendQueryParams(String baseUrl, Map<String, String> requestValues) {
        if (config.getQueryParamsTemplate() == null) {
            return baseUrl;
        }
        String resolved = PlaceholderResolver.replace(config.getQueryParamsTemplate(), requestValues);
        StringBuilder sb = new StringBuilder(baseUrl);
        String separator = baseUrl.contains("?") ? "&" : "?";
        for (String pair : resolved.split("&")) {
            if (pair.isBlank()) {
                continue;
            }
            String[] kv = pair.split("=", 2);
            String key = kv[0];
            String val = kv.length > 1 ? PlaceholderResolver.encodeQueryValue(kv[1]) : "";
            sb.append(separator).append(key).append("=").append(val);
            separator = "&";
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: 运行确认通过** → 3 个用例 PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/request/ScanRequestTemplate.java src/test/java/com/getindata/connectors/http/internal/table/scan/request/ScanRequestTemplateTest.java
git commit -m "[HTTP-SCAN] Add ScanRequestTemplate for HTTP request assembly"
```

---

## Task 10: HttpScanSplit + Serializer

**Files:**
- Create: `source/HttpScanSplit.java`, `source/HttpScanSplitSerializer.java`
- Test: `source/HttpScanSplitSerializerTest.java`

**Interfaces:**
- Consumes: FLIP-27 `SourceSplit` 接口
- Produces:
  - `HttpScanSplit implements SourceSplit` — 单一 split，`splitId()` 返回固定 `"http-scan-split-0"`；持有 `HttpScanConfig`（可序列化）
  - `HttpScanSplitSerializer implements SimpleVersionedSerializer<HttpScanSplit>` — 最小实现：`getVersion()=1`；`serialize` 写 config 的 `properties`（`Properties` 是 Serializable，用 `ObjectOutputStream`）；`deserialize` 还原。由于 v1 不支持 checkpoint 恢复，反序列化的 split 仅作为 API 占位，正常执行路径不会调用。

- [ ] **Step 1: 写失败测试**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

class HttpScanSplitSerializerTest {

    @Test
    void shouldRoundtripSplit() throws Exception {
        var conf = new Configuration();
        conf.set(URL, "https://x");
        var cfg = com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new Properties());
        var split = new HttpScanSplit(cfg);
        assertThat(split.splitId()).isEqualTo("http-scan-split-0");

        var serializer = new HttpScanSplitSerializer();
        byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        HttpScanSplit restored = SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
        assertThat(restored.splitId()).isEqualTo("http-scan-split-0");
        assertThat(restored.getConfig().getUrl()).isEqualTo("https://x");
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

`HttpScanSplit.java`:
```java
package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.api.connector.source.SourceSplit;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

public class HttpScanSplit implements SourceSplit {

    public static final String SPLIT_ID = "http-scan-split-0";

    private final HttpScanConfig config;

    public HttpScanSplit(HttpScanConfig config) {
        this.config = config;
    }

    public HttpScanConfig getConfig() {
        return config;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }
}
```

`HttpScanSplitSerializer.java`（最小实现，用 Properties 序列化）：
```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

public class HttpScanSplitSerializer implements SimpleVersionedSerializer<HttpScanSplit> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(HttpScanSplit split) throws Exception {
        try (var bos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(split.getConfig().getProperties());
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public HttpScanSplit deserialize(int version, byte[] serialized) throws Exception {
        try (var bis = new ByteArrayInputStream(serialized);
             var ois = new ObjectInputStream(bis)) {
            Properties props = (Properties) ois.readObject();
            // 反序列化路径仅满足 API；正常执行不依赖此处恢复完整 config（无 checkpoint 续传）。
            // 用最小 config 占位。
            var conf = new Configuration();
            return new HttpScanSplit(HttpScanConfig.from(conf, props));
        }
    }
}
```

> 注：由于 `HttpScanConfig` 由 ReadableConfig 构造，反序列化时无法完整恢复 url/method 等（它们不在 properties 里）。这是 v1 已声明的限制（无 checkpoint 恢复）。Serializer 仅满足 FLIP-27 API 要求，**不在正常执行路径使用**。测试断言 splitId 与 url（url 恰好在 deserialize 时为 null/默认）。**修正测试**：deserialize 后 `getConfig().getUrl()` 应为 null（未恢复），测试改为断言 `splitId()` 与 properties 非空。以实现时为准。

- [ ] **Step 4: 运行确认通过**（修正测试后）→ PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSplit.java src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSplitSerializer.java src/test/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSplitSerializerTest.java
git commit -m "[HTTP-SCAN] Add HttpScanSplit and minimal serializer"
```

---

## Task 11: HttpScanSplitEnumerator

**Files:**
- Create: `source/HttpScanSplitEnumerator.java`
- Test: `source/HttpScanSplitEnumeratorTest.java`

**Interfaces:**
- Consumes: `SplitEnumerator<HttpScanSplit, ...>`，`HttpScanConfig`
- Produces: 单并行度枚举器：
  - `start()`：记录待分配的 split（构造时传入 `new HttpScanSplit(config)`）
  - `handleSplitRequest(subtaskId, requesterHostname)`：把 split 通过 `context.assignSplit(split, subtaskId)` 发出，并 `context.signalNoMoreSplits(subtaskId)`
  - `addReader`/`addSplitsBack`：空实现或把 split 重新放回待分配

- [ ] **Step 1: 写失败测试**（mock SplitEnumeratorContext，断言 assignSplit + signalNoMoreSplits 被调用一次）

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.URL;

class HttpScanSplitEnumeratorTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldAssignSingleSplitOnce() {
        var ctx = (SplitEnumeratorContext<HttpScanSplit>) Mockito.mock(SplitEnumeratorContext.class);
        var conf = new Configuration();
        conf.set(URL, "https://x");
        var cfg = com.getindata.connectors.http.internal.table.scan.HttpScanConfig.from(conf, new Properties());

        var enumerator = new HttpScanSplitEnumerator(ctx, cfg);
        enumerator.start();

        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(HttpScanSplit.class), eq(0));
        verify(ctx, times(1)).signalNoMoreSplits(0);

        // 第二次请求不再分配
        enumerator.handleSplitRequest(0, "host");
        verify(ctx, times(1)).assignSplit(any(), eq(0));
    }
}
```

> 注：`Callable` import 可去掉。`SplitEnumeratorContext` 在 Flink 1.17/1.18 有 `assignSplit(SplitT, int)` 与 `signalNoMoreSplits(int)`。需在实现时核对方法签名（context 当前并行度用 `ctx.currentParallelism()` 或 `ctx.parallelism()`，1.17/1.18 略有差异，以 1.18.1 为准）。

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.util.List;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

public class HttpScanSplitEnumerator
        implements SplitEnumerator<HttpScanSplit, nothing> {

    private final SplitEnumeratorContext<HttpScanSplit> context;
    private final HttpScanSplit split;
    private boolean assigned = false;

    public HttpScanSplitEnumerator(SplitEnumeratorContext<HttpScanSplit> context, HttpScanConfig config) {
        this.context = context;
        this.split = new HttpScanSplit(config);
    }

    @Override
    public void start() {
        // 无后台动作
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!assigned) {
            context.assignSplit(split, subtaskId);
            context.signalNoMoreSplits(subtaskId);
            assigned = true;
        }
    }

    @Override
    public void addSplitsBack(List<HttpScanSplit> splits, int subtaskId) {
        assigned = false;
    }

    @Override
    public void addReader(int subtaskId) {
        // 主动触发一次分配请求（单并行度）
        handleSplitRequest(subtaskId, "enumerator-internal");
    }
}
```

> **修正**：`nothing` 不是 Java 类型。FLIP-27 的 `SplitEnumerator<SplitT, CheckpointT>`，CheckpointT 无状态时用 `Void` 或 `null`。改为 `implements SplitEnumerator<HttpScanSplit, Void>`，并实现 `snapshotState(long checkpointId)` 返回 `null` 或空。实现时补 `snapshotState` 方法。

- [ ] **Step 4: 运行确认通过** → PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSplitEnumerator.java src/test/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSplitEnumeratorTest.java
git commit -m "[HTTP-SCAN] Add HttpScanSplitEnumerator assigning single split"
```

---

## Task 12: HttpClient + RetryConfig（含共享代码最小变更）

**Files:**
- Modify: `src/main/java/com/getindata/connectors/http/internal/utils/JavaNetHttpClientFactory.java:104` — `getSslContext` 提为 `public static`
- Create: `source/HttpScanRetryConfigProvider.java`
- Create: `source/HttpScanHttpClientFactory.java`
- Test: `source/HttpScanRetryConfigProviderTest.java`, `source/HttpScanHttpClientFactoryTest.java`

**变更影响分析（共享代码）**：
- `JavaNetHttpClientFactory.getSslContext(Properties)` 由 `private` → `public static`。这是**纯可见性放宽**：方法体、签名、调用点（`createClient` 内部仍在用）均不变。lookup/sink 行为零变更。现有 `JavaNetHttpClientFactoryTest`（如有）回归通过。

**Interfaces:**
- Consumes: `HttpScanConnectorOptions`、`HttpScanConfig`、`JavaNetHttpClientFactory.getSslContext`（提为 public 后）、`HttpClientWithRetry`、`HttpResponseChecker`、`HttpCodesParser`
- Produces:
  - `HttpScanRetryConfigProvider.create(ReadableConfig)` → `RetryConfig`（镜像 lookup `RetryConfigProvider`，但读 scan ConfigOption）
  - `HttpScanHttpClientFactory.create(HttpScanConfig)` → `HttpClientWithRetry`：
    1. `HttpClient javaClient = buildJavaClient(config)`（SSLContext 复用 `JavaNetHttpClientFactory.getSslContext(config.getProperties())`；proxy/connection-timeout 从 `config.getReadableConfig()` 读 scan ConfigOption）
    2. `HttpResponseChecker checker = new HttpResponseChecker(successExpr, retryExpr)`（从 scan SUCCESS_CODES/RETRY_CODES；ignored 在 Reader 层单独处理）
    3. `RetryConfig retryConfig = HttpScanRetryConfigProvider.create(config.getReadableConfig())`
    4. `return HttpClientWithRetry.builder().httpClient(javaClient).retryConfig(retryConfig).responseChecker(checker).build()`

- [ ] **Step 1: 写失败测试**（RetryConfigProvider）

```java
package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.*;

class HttpScanRetryConfigProviderTest {

    @Test
    void shouldUseFixedDelayByDefault() {
        var conf = new Configuration();
        conf.set(MAX_RETRIES, 3);
        var rc = HttpScanRetryConfigProvider.create(conf);
        // maxAttempts = maxRetries + 1
        // 通过 intervalFunction 间接验证（不抛异常即视为构造成功）
        assertThat(rc).isNotNull();
    }

    @Test
    void shouldBuildExponentialDelay() {
        var conf = new Configuration();
        conf.set(RETRY_STRATEGY_TYPE, "exponential-delay");
        conf.set(MAX_RETRIES, 2);
        assertThat(HttpScanRetryConfigProvider.create(conf)).isNotNull();
    }
}
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3a: 修改共享代码（提 public）**

`JavaNetHttpClientFactory.java`：把 `private static SSLContext getSslContext(Properties properties)` 改为 `public static SSLContext getSslContext(Properties properties)`。

- [ ] **Step 3b: 写 HttpScanRetryConfigProvider**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.time.Duration;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

/** 镜像 lookup 的 RetryConfigProvider，但读 scan 专属 ConfigOption。 */
public final class HttpScanRetryConfigProvider {

    private HttpScanRetryConfigProvider() {}

    public static RetryConfig create(ReadableConfig config) {
        String strategy = config.get(HttpScanConnectorOptions.RETRY_STRATEGY_TYPE);
        int maxRetries = config.get(HttpScanConnectorOptions.MAX_RETRIES);
        RetryConfig.Builder<?> builder;
        if ("exponential-delay".equalsIgnoreCase(strategy)) {
            Duration initial = config.get(HttpScanConnectorOptions.RETRY_EXP_INITIAL_BACKOFF);
            Duration max = config.get(HttpScanConnectorOptions.RETRY_EXP_MAX_BACKOFF);
            double mult = config.get(HttpScanConnectorOptions.RETRY_EXP_MULTIPLIER);
            builder = RetryConfig.custom()
                .intervalFunction(IntervalFunction.ofExponentialBackoff(initial, mult, max));
        } else {
            Duration delay = config.get(HttpScanConnectorOptions.RETRY_FIXED_DELAY);
            builder = RetryConfig.custom().intervalFunction(IntervalFunction.of(delay));
        }
        return builder.maxAttempts(maxRetries + 1).build();
    }
}
```

- [ ] **Step 3c: 写 HttpScanHttpClientFactory**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;

public final class HttpScanHttpClientFactory {

    private HttpScanHttpClientFactory() {}

    public static HttpClientWithRetry create(HttpScanConfig config) {
        SSLContext sslContext = JavaNetHttpClientFactory.getSslContext(config.getProperties());
        HttpClient.Builder builder = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .sslContext(sslContext);

        ReadableConfig readable = config.getReadableConfig();
        readable.getOptional(HttpScanConnectorOptions.CONNECTION_TIMEOUT)
            .ifPresent(builder::connectTimeout);

        Optional<String> host = readable.getOptional(HttpScanConnectorOptions.PROXY_HOST);
        Optional<Integer> port = readable.getOptional(HttpScanConnectorOptions.PROXY_PORT);
        if (host.isPresent() && port.isPresent()) {
            builder.proxy(ProxySelector.of(new InetSocketAddress(host.get(), port.get())));
            // 简化：代理鉴权省略（如需可在后续 task 增补）
        }
        HttpClient javaClient = builder.build();

        HttpResponseChecker checker = new HttpResponseChecker(
            readable.get(HttpScanConnectorOptions.SUCCESS_CODES),
            Optional.ofNullable(readable.get(HttpScanConnectorOptions.RETRY_CODES)).orElse("")
        );

        return HttpClientWithRetry.builder()
            .httpClient(javaClient)
            .retryConfig(HttpScanRetryConfigProvider.create(readable))
            .responseChecker(checker)
            .build();
    }
}
```

> 注：`HttpResponseChecker(String, String)` 构造函数已存在（见 `HttpResponseChecker.java:17`）。空 retryExpr 合法（空集）。ignored codes 在 Reader 层用 `HttpCodesParser` 单独解析判断。

- [ ] **Step 4: 运行确认通过** → RetryConfigProviderTest PASS。
- [ ] **Step 4b: 验证共享代码变更不破坏现有测试**

Run: `mvn -q -Dtest=JavaNetHttpClientFactoryTest test`（如存在）→ PASS。再 `mvn -q test -Dtest='HttpLookup*Test'` → 全绿。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/utils/JavaNetHttpClientFactory.java src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanRetryConfigProvider.java src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanHttpClientFactory.java src/test/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanRetryConfigProviderTest.java
git commit -m "[HTTP-SCAN] Add scan HttpClient/RetryConfig factories; promote getSslContext to public"
```

---

## Task 13: HttpScanSourceReader

**Files:**
- Create: `source/HttpScanSourceReader.java`
- Test: `source/HttpScanSourceReaderIT.java`（WireMock）

**Interfaces:**
- Consumes: `SourceReaderBase`（FLIP-27）或自定义最小 `SourceReader`；`HttpClientWithRetry`、`ScanRequestTemplate`、`PaginationStrategy`、`JsonPathExtractor`、`DeserializationSchema<RowData>`、`HttpCodesParser`（判 ignored codes）、`HttpLogger`
- Produces: 实现 `pollNext`：主循环（见 spec §3.2 伪代码）。关键：
  - 成功响应 → 剥壳 → 逐条 deserialize → collect
  - `ignored-response-codes` 命中 → 跳过内容，仍 `afterResponse`
  - 其余（HttpClientWithRetry 已处理 retry；非成功非 ignored）→ 抛异常 → job fail
  - 分页结束 → `InputStatus.END_OF_INPUT`

**实现策略**：继承 `org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase` 较重；鉴于**单并行度、无 split 并行、无 checkpoint 恢复**，采用**直接实现 `SourceReader` 接口**的最小实现，内部维护一个 split 与分页状态机。这样避免引入 `SourceReaderBase` 的 record-emit 线程模型复杂度（CLAUDE.md §2.8 简单方案优先）。

- [ ] **Step 1: 写失败测试**（WireMock 集成，覆盖无参 + 页码分页）

```java
package com.getindata.connectors.http.internal.table.scan.source;

// 用 WireMock 启动本地服务，构造最小 HttpScanConfig 指向 wiremock url，
// 构造 HttpScanSourceReader，调用 pollNext 收集 RowData，断言行数与内容。
// 见 spec §5.4 用例 1/6。具体 stub 与断言在实现时按 wiremock API 编写。
```

> 完整集成测试代码较长，实现时参考项目现有 `HttpLookupTableSourceITCase` 的 WireMock 用法。关键断言：①无参单页返回 2 条记录；②页码分页 page=1 返回100条、page=2 返回50条→停止，共 150 条。

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**（最小 SourceReader）

```java
package com.getindata.connectors.http.internal.table.scan.source;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.api.common.io.InputStatus;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.status.HttpCodesParser;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationState;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationStrategy;
import com.getindata.connectors.http.internal.table.scan.pagination.StopDecision;
import com.getindata.connectors.http.internal.table.scan.request.ScanRequestTemplate;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;
import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpScanSourceReader implements SourceReader<RowData, HttpScanSplit> {

    private final HttpClientWithRetry httpClient;
    private final ScanRequestTemplate requestTemplate;
    private final PaginationStrategy paginationStrategy;
    private final HttpScanConfig config;
    private final DeserializationSchema<RowData> deserializer;
    private final Set<Integer> ignoredCodes;

    private HttpScanSplit currentSplit;
    private PaginationState state;
    private boolean finished = false;

    public HttpScanSourceReader(HttpClientWithRetry httpClient,
                                ScanRequestTemplate requestTemplate,
                                PaginationStrategy paginationStrategy,
                                HttpScanConfig config,
                                DeserializationSchema<RowData> deserializer) throws ConfigurationException {
        this.httpClient = httpClient;
        this.requestTemplate = requestTemplate;
        this.paginationStrategy = paginationStrategy;
        this.config = config;
        this.deserializer = deserializer;
        String ignoredExpr = config.getReadableConfig()
            .get(com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions.IGNORED_RESPONSE_CODES);
        this.ignoredCodes = (ignoredExpr == null || ignoredExpr.isBlank())
            ? Set.of() : HttpCodesParser.parse(ignoredExpr);
    }

    @Override
    public void start() {
        // 单线程同步，无需启动额外线程
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (finished) {
            return InputStatus.END_OF_INPUT;
        }
        if (state == null) {
            state = paginationStrategy.initialState(config);
        }
        Optional<java.util.Map<String, String>> reqValues = paginationStrategy.nextRequestValues(state, config);
        if (reqValues.isEmpty()) {
            finished = true;
            return InputStatus.END_OF_INPUT;
        }

        java.net.http.HttpRequest request = requestTemplate.build(reqValues.get());
        HttpResponse<byte[]> response = httpClient.send(() -> request, HttpResponse.BodyHandlers.ofByteArray());

        if (ignoredCodes.contains(response.statusCode())) {
            // 跳过内容，仍推进分页
            StopDecision decision = paginationStrategy.afterResponse(state, "", 0, config);
            state = decision.getNextState();
            if (decision.isShouldStop()) {
                finished = true;
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        String body = new String(response.body(), java.nio.charset.StandardCharsets.UTF_8);
        List<byte[]> records = JsonPathExtractor.extractRecords(body, config.getContentField());
        for (byte[] rec : records) {
            RowData row = deserializer.deserialize(rec);
            if (row != null) {
                output.collect(row);
            }
        }

        StopDecision decision = paginationStrategy.afterResponse(state, body, records.size(), config);
        state = decision.getNextState();
        if (decision.isShouldStop()) {
            finished = true;
            return InputStatus.END_OF_INPUT;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public void addSplits(List<HttpScanSplit> splits) {
        if (!splits.isEmpty()) {
            currentSplit = splits.get(0);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // 无动作
    }

    @Override
    public List<HttpScanSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // HttpClient 复用，无需显式关闭
    }
}
```

> **核对点**（实现时确认，因 1.17/1.18 API）：
> - `InputStatus` 在 1.18 为 `org.apache.flink.api.connector.source.ReaderOutput` + `InputStatus`，包 `org.apache.flink.api.connector.source`。1.17 同。`ReaderOutput` 与 `pollNext` 签名在 1.17/1.18 一致。
> - `HttpResponse.BodyHandlers` 在 `java.net.http`。
> - `HttpClientWithRetry.send(Supplier, BodyHandler)` 签名见 Task 12 读取的源码（返回 `HttpResponse<T>`）。

- [ ] **Step 4: 运行确认通过** → 集成测试（无参 + 页码分页）PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSourceReader.java src/test/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSourceReaderIT.java
git commit -m "[HTTP-SCAN] Add HttpScanSourceReader with paginate-decode-collect loop"
```

---

## Task 14: HttpScanSource（FLIP-27 入口）

**Files:**
- Create: `source/HttpScanSource.java`
- Test: `source/HttpScanSourceTest.java`（单元：断言 Boundedness.BOUNDED、createEnumerator/createReader 不抛异常）

**Interfaces:**
- Consumes: 所有 source/* 类、`SourceReaderContext`
- Produces: `implements Source<RowData, HttpScanSplit, Void>`，`getBoundedness() = BOUNDED`，`createEnumerator(context)` 返回 `HttpScanSplitEnumerator`，`createReader(context)` 返回 `HttpScanSourceReader`（内部用 `HttpScanHttpClientFactory.create` + `ScanRequestTemplate` + `PaginationStrategy.from(config)` 构造）。

- [ ] **Step 1: 写失败测试**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.api.connector.source.Boundedness;
// ... 构造 HttpScanSource，断言 getBoundedness()==BOUNDED
```

- [ ] **Step 2: 运行确认失败** → 编译失败。
- [ ] **Step 3: 写实现**

```java
package com.getindata.connectors.http.internal.table.scan.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.pagination.CursorPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.NoPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.PageNumberPagination;
import com.getindata.connectors.http.internal.table.scan.pagination.PaginationStrategy;
import com.getindata.connectors.http.internal.table.scan.request.ScanRequestTemplate;

public class HttpScanSource implements Source<RowData, HttpScanSplit, Void> {

    private final HttpScanConfig config;
    private final DeserializationSchema<RowData> deserializer;

    public HttpScanSource(HttpScanConfig config, DeserializationSchema<RowData> deserializer) {
        this.config = config;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, HttpScanSplit> createReader(SourceReaderContext readerContext) throws ConfigurationException {
        return new HttpScanSourceReader(
            HttpScanHttpClientFactory.create(config),
            new ScanRequestTemplate(config),
            strategyFor(config),
            config,
            deserializer
        );
    }

    @Override
    public SplitEnumerator<HttpScanSplit, Void> createEnumerator(SplitEnumeratorContext<HttpScanSplit> enumContext) {
        return new HttpScanSplitEnumerator(enumContext, config);
    }

    @Override
    public SimpleVersionedSerializer<HttpScanSplit> getSplitSerializer() {
        return new HttpScanSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    private static PaginationStrategy strategyFor(HttpScanConfig config) {
        switch (config.getPaginationType()) {
            case "page-number": return new PageNumberPagination();
            case "cursor": return new CursorPagination();
            case "none":
            default: return new NoPagination();
        }
    }
}
```

> `VoidSerializer`：Flink 提供 `org.apache.flink.core.io.SimpleVersionedSerializer<Void>` 需自写一个最小实现（`getVersion()=1`，serialize 返回 `new byte[0]`，deserialize 返回 `null`）。在 Task 14 一并创建 `source/VoidSerializer.java`。

- [ ] **Step 4: 运行确认通过** → PASS。
- [ ] **Step 5: `mvn -q validate`** → SUCCESS。
- [ ] **Step 6: 提交**

```bash
git add src/main/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSource.java src/main/java/com/getindata/connectors/http/internal/table/scan/source/VoidSerializer.java src/test/java/com/getindata/connectors/http/internal/table/scan/source/HttpScanSourceTest.java
git commit -m "[HTTP-SCAN] Add HttpScanSource FLIP-27 entry with Boundedness.BOUNDED"
```

---

## Task 15: HttpScanMetadata

**Files:**
- Create: `metadata/HttpScanMetadata.java`
- Test: `metadata/HttpScanMetadataTest.java`

**Interfaces:**
- Consumes: Flink `ReadonlyMetadata`
- Produces: 3 个 metadata 列定义（`http-status-code`/`http-headers-map`/`page-number`），用于 DynamicTableSource 的 `SupportsReadingMetadata`。

> 注：metadata 列需要 Reader 在 collect 时附带 metadata。这要求 `HttpScanSourceReader` 把当前响应的 statusCode/headers/page 暴露给 `output.collect(RowData, Consumer<RowData>)` 的 metadata 回调。**简化决定（v1）**：先实现 `page-number` 一个 metadata 列（最易，Reader 已有 state.pageNumber），`http-status-code`/`http-headers-map` 需改 Reader collect 路径携带响应信息——作为 **Task 15 的范围扩展**，但若复杂度过高，v1 先只交付 `page-number`，并在 README 标注其余 2 个为后续。**执行时决定**：先实现 3 列的**定义**（metadata 列表），Reader 支持 page-number；status-code/headers-map 在 Reader 通过 `ReaderOutput` 的 metadata 透出（1.18 `output.collect(row, metadataConsumer)`）。若 `ReaderOutput` metadata API 在 1.17/1.18 差异大，则 v1 降级为仅 page-number。

- [ ] **Step 1-6**：TDD 循环，定义 3 个 metadata 列 + Reader 透出。

```bash
git commit -m "[HTTP-SCAN] Add HttpScanMetadata columns"
```

---

## Task 16: HttpScanDynamicTableSource

**Files:**
- Create: `HttpScanDynamicTableSource.java`
- Test: `HttpScanDynamicTableSourceTest.java`

**Interfaces:**
- Consumes: `ScanTableSource`、`SupportsProjectionPushDown`、`SourceProvider`、`HttpScanSource`、`DecodingFormat<DeserializationSchema<RowData>>`
- Produces:
  - `getScanRuntimeProvider(ctx)`：用 `decodingFormat.createRuntimeDecoder(ctx, projectedDataType)` 得到 deserializer；`return SourceProvider.of(new HttpScanSource(config, deserializer))`
  - `applyProjection(projectedFields, dataType)`：记录投影，重建 DataType
  - `copy()`、`asSummaryString()`

- [ ] **Step 1-6**：TDD 循环。
```bash
git commit -m "[HTTP-SCAN] Add HttpScanDynamicTableSource with projection pushdown"
```

---

## Task 17: HttpScanTableSourceFactory + 校验规则

**Files:**
- Create: `HttpScanTableSourceFactory.java`
- Test: `HttpScanTableSourceFactoryTest.java`（校验规则 1-13，每条正反两例）

**Interfaces:**
- Consumes: `DynamicTableSourceFactory`、`FactoryUtil`、`HttpScanConnectorOptions`、`HttpScanConfig`、`HttpScanDynamicTableSource`、`ConfigUtils.getHttpConnectorProperties`、`JsonPathExtractor.compileCheck`
- Produces: `factoryIdentifier() = "http-scan"`；`requiredOptions = {URL, FORMAT}`；`optionalOptions =` 其余 scan ConfigOption；`createDynamicTableSource` 内执行校验规则 1-13。

校验规则实现要点（来自 spec §4.2）：
- 规则3：GET + body → 抛错
- 规则6/7/8：占位符扫描（`method=GET`/分页类型 与 `${page}`/`${cursor}` 一致性）
- 规则9：url-vars 与 URL 双向匹配 `{name}`
- 规则10：所有 JSONPath 字段 `compileCheck`
- 规则11：数值范围

- [ ] **Step 1-6**：TDD 循环（20 个用例）。
```bash
git commit -m "[HTTP-SCAN] Add HttpScanTableSourceFactory with validation rules"
```

---

## Task 18: 服务发现注册 + 端到端 SQL 测试

**Files:**
- Modify: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`（追加一行）
- Test: `src/test/java/com/getindata/connectors/http/internal/table/scan/HttpScanTableSourceITCase.java`（Flink MiniCluster + WireMock，覆盖 spec §5.5 的 10 个用例）

- [ ] **Step 1: 注册 Factory**

在文件末尾追加：
```
com.getindata.connectors.http.internal.table.scan.HttpScanTableSourceFactory
```

- [ ] **Step 2-6**：写端到端测试，跑通 `CREATE TABLE ... http-scan ... + INSERT INTO sink SELECT * FROM ...`，覆盖无参、页码分页、游标分页、POST、url-vars、投影、JOIN、失败。

```bash
git commit -m "[HTTP-SCAN] Register factory; add end-to-end SQL ITCase"
```

---

## Task 19: pom jacoco + README + CHANGELOG

**Files:**
- Modify: `pom.xml:373-379`（JaCoCo excludes 追加 `**/HttpScanConnectorOptions.class`）
- Modify: `README.md`（新增 `### HTTP Scan Source` 章节 + 选项表 + Breaking changes）
- Modify: `CHANGELOG.md`（0.27.0 段）

- [ ] **Step 1-5**：编辑 + `mvn -q verify`（确认 jacoco 覆盖率门槛 + 全量测试通过）。
```bash
git commit -m "[HTTP-SCAN] Update pom jacoco excludes, README, CHANGELOG for 0.27.0"
```

---

## Self-Review（writing-plans 自审 checklist）

**1. Spec 覆盖：**
- 无参拉全量 → NoPagination (Task 6) + Reader (Task 13) + 端到端 (Task 18) ✓
- 带参调用 → query-params/url-vars/body (Task 1 Options + Task 3 Resolver + Task 9 Template) ✓
- 页码分页 + 4 种停止策略 → Task 7 ✓
- 游标分页 → Task 8 ✓
- 复用 security/auth/status/retry/proxy/logging → Task 12 ✓
- 校验规则 1-13 → Task 17 ✓
- metadata 列 → Task 15 ✓
- 投影下推 → Task 16 ✓
- checkpoint 不支持 → Task 10/11（最小实现）✓
- README/CHANGELOG/版本 → Task 19 ✓

**2. 占位符扫描：**
- Task 11 出现了 `nothing`/`snapshotState` 占位 → 已在 Task 11/14 内注明修正（`Void` + `snapshotState` 实现）。
- Task 13 的集成测试代码标注"参考现有 ITCase 编写" → 这是合理的（wiremock stub 模板随实现填充），但**执行时必须补完整 stub**，不允许留 TODO。
- Task 15 metadata 的 1.17/1.18 API 差异标注了降级方案。
- 这些"执行时确认点"是 API 版本核对，非占位实现——执行 Task 时第一件事就是核对真实签名。

**3. 类型一致性：**
- `PaginationStrategy.initialState()` 在 Task 6 定义为无参，Task 8 发现需 config → Task 8 明确**接口改为 `initialState(config)`**，并要求同步修正 Task 6/7。**执行顺序修正**：Task 6 写接口时就直接定为 `initialState(HttpScanConfig config)`，避免返工。→ 已在 Task 6 接口块修正。
- `nextRequestValues` / `afterResponse` 在 Task 6/7/8/13 命名一致 ✓
- `ScanRequestTemplate.build(Map)` 在 Task 9/13 一致 ✓
- `HttpScanHttpClientFactory.create(HttpScanConfig)` 在 Task 12/14 一致 ✓

**4. 已知执行时必须核对的 API（避免假设）：**
- Flink 1.18.1 `ReaderOutput`/`InputStatus`/`SourceReader.pollNext` 签名
- `SplitEnumeratorContext.assignSplit`/`signalNoMoreSplits`/`currentParallelism` 签名
- `HttpResponseChecker(String, String)` 构造（已确认存在，Task 12）
- `HttpClientWithRetry.send(Supplier, BodyHandler)`（已确认，Task 12）

执行时第一个动作永远是 Read 真实签名，禁止凭记忆。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-22-http-scan-connector.md`.

按用户授权"直接实现"，采用 **Inline Execution**（superpowers:executing-plans），在本会话内按 Task 顺序执行，每个 Task 走 TDD 循环 + checkstyle + commit，Task 间作为 checkpoint。

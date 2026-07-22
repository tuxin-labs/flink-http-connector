# http-scan Flink SQL 连接器设计文档

- 日期：2026-07-21
- 作者：`tuxin`（与 Claude 通过 superpowers:brainstorming 协作产出）
- 状态：已通过分节评审，等待用户对完整 spec 复审
- 目标交付物：一个新的 Flink SQL 连接器 `http-scan`，与现有 `rest-lookup`、`http-sink` 并列共存于本项目 `com.getindata:flink-http-connector` 之内。

---

## 0. 背景与目标

### 0.1 问题

现有 `getindata/flink-http-connector` 只实现 `LookupTableSource`（`HttpLookupTableSource.java:47`），无法作为 Flink SQL 的独立扫描源。用户不能写：

```sql
INSERT INTO sink SELECT * FROM http_table;
```

必须依赖"驱动表 + Lookup Join"绕一层，且**明确不支持分页**（`README.md:262-267`）：

> `array` - REST API returns array of objects. **Pagination is not supported yet.**

调研结论：Apache 官方 `apache/flink-connector-http` 仓库尚为早期骨架、无发布；社区无成熟的、能通过 Flink SQL `CREATE TABLE` 直接读取 HTTP 接口且支持分页的开源连接器。

### 0.2 目标（v1）

新增连接器 `http-scan`，覆盖以下 Flink SQL 使用场景：

1. **无参一次性拉全量**
2. **有参调用**（URL query 参数 / URL 路径变量 / POST body 模板）
3. **数字页码分页**（多种停止策略）
4. **游标分页**（下一页 token 来自响应体）

使用方式：

```sql
CREATE TABLE http_orders ( ... ) WITH ('connector' = 'http-scan', ...);
INSERT INTO my_sink SELECT * FROM http_orders;
```

### 0.3 非目标（v1 明确不做）

- 多并行度扫描（v1 单并行度）
- checkpoint 断点续传
- `continue-on-error`（Scan 场景语义模糊，易掩盖数据丢失）
- 单独发布为独立 Maven artifact（继续与 lookup / sink 同一 fat jar）
- Docker 镜像 / SQL Gateway 集成教程
- 压测与性能测试
- OpenAPI 自动生成客户端

---

## 1. 总体架构与模块边界

### 1.1 定位

在 `com.getindata.connectors.http.internal.table.scan.*` 下新增连接器，**factory identifier = `http-scan`**，与 `rest-lookup`、`http-sink` **并列共存**，同一 JAR，同一 `META-INF/services/org.apache.flink.table.factories.Factory` 注册。

### 1.2 分层设计

```
┌──────────────────────────────────────────────────────────────────┐
│ Flink SQL 层                                                     │
│   CREATE TABLE ... WITH ('connector'='http-scan', ...)           │
│   INSERT INTO sink SELECT * FROM http_scan_table                 │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ① Factory 层     HttpScanTableSourceFactory                      │
│                  · factoryIdentifier() = "http-scan"             │
│                  · requiredOptions / optionalOptions             │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ② DynamicTableSource 层  HttpScanDynamicTableSource              │
│                  implements ScanTableSource,                     │
│                             SupportsProjectionPushDown           │
│                  · getScanRuntimeProvider() 返回 SourceProvider  │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ③ FLIP-27 Source 层  HttpScanSource implements Source<...>       │
│      · createEnumerator() → HttpScanSplitEnumerator（单 split）  │
│      · createReader()    → HttpScanSourceReader                  │
│      · Boundedness.BOUNDED                                       │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ④ 分页驱动层  PaginationStrategy (interface)                    │
│      ├─ NoPagination                                             │
│      ├─ PageNumberPagination                                     │
│      └─ CursorPagination                                         │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ⑤ HTTP 客户端层  HttpScanHttpClient                              │
│      · 复用 java.net.http.HttpClient                             │
│      · 复用 internal/security · TLS/mTLS                         │
│      · 复用 internal/auth · Basic / OIDC                         │
│      · 复用 internal/retry · resilience4j                        │
│      · 复用 HttpLogger                                           │
└───────────────────────────────┬──────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│ ⑥ 反序列化层                                                    │
│      · JsonPathExtractor  · 按 content-field 剥壳                │
│      · DeserializationSchema (由 Format 提供)                    │
│           → RowData                                              │
└──────────────────────────────────────────────────────────────────┘
```

### 1.3 新增文件清单

```
src/main/java/com/getindata/connectors/http/internal/table/scan/
├─ HttpScanTableSourceFactory.java
├─ HttpScanDynamicTableSource.java
├─ HttpScanConnectorOptions.java       // 集中定义所有 ConfigOption
├─ HttpScanConfig.java                 // 从 ReadableConfig → 运行期配置对象
├─ source/
│   ├─ HttpScanSource.java             // implements Source<RowData, HttpScanSplit, ...>
│   ├─ HttpScanSplit.java              // 单一 split，携带初始 pagination state
│   ├─ HttpScanSplitState.java
│   ├─ HttpScanSplitEnumerator.java    // 只发一次 split
│   ├─ HttpScanSourceReader.java       // 拉取 → 反序列化 → collect
│   └─ HttpScanSplitSerializer.java    // 最小实现（无 checkpoint 恢复）
├─ pagination/
│   ├─ PaginationStrategy.java
│   ├─ PaginationState.java
│   ├─ NoPagination.java
│   ├─ PageNumberPagination.java
│   └─ CursorPagination.java
├─ request/
│   ├─ ScanRequestTemplate.java        // URL/path 占位符 + body 模板 + query
│   └─ PlaceholderResolver.java        // ${page} / ${cursor} 替换
└─ response/
    └─ JsonPathExtractor.java          // 剥壳 + 从响应体推导停止条件

src/main/java/com/getindata/connectors/http/internal/table/scan/metadata/
└─ HttpScanMetadataConverter.java
```

对应测试镜像目录：

```
src/test/java/com/getindata/connectors/http/internal/table/scan/
```

### 1.4 复用现有代码（禁止重复造轮子）

| 复用 | 已有位置 |
|---|---|
| Java Net HttpClient 构造与 TLS | `internal/security` |
| Basic / OIDC 认证 | `internal/auth`、`OIDCAuthHeaderValuePreprocessor` |
| 重试策略 | `internal/retry`（resilience4j） |
| HTTP 请求/响应日志 | `internal/HttpLogger` |
| 响应状态码分类 | `internal/status` |
| Header 预处理 | `internal/ComposeHeaderPreprocessor` |

### 1.5 与现有连接器的隔离

`http-scan` **独立目录、独立 Factory、独立 Options**，不修改 lookup 或 sink 的任何现有类。lookup 与 sink 行为**零变更**。

---

## 2. 配置项与 DDL 示例

### 2.1 命名约定

沿用项目现有 `gid.connector.http.*` 前缀。共性表选项（url / method / format / headers）用短名；扩展/分页/请求类选项用长前缀 `gid.connector.http.scan.*`。

### 2.2 完整配置项

#### A. 基础请求

| 选项 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `connector` | 是 | — | 固定 `http-scan` |
| `url` | 是 | — | 请求 URL，可含 `{path_var}` 占位符 |
| `format` | 是 | — | Flink Format 名，如 `json` |
| `method` | 否 | `GET` | `GET` / `POST` / `PUT` |
| `gid.connector.http.scan.url-vars` | 否 | — | URL 路径变量静态取值，格式 `key1:v1,key2:v2` |
| `gid.connector.http.scan.query-params` | 否 | — | Query 参数，格式 `k1=v1&k2=v2`，`&` 与 `=` 视为分隔符；值支持 `${page}` `${cursor}`；若值本身需要包含 `&` 或 `=` 字符，用户须提前 URL 编码，或改用 `body` 传参 |
| `gid.connector.http.scan.body` | 否 | — | POST/PUT 请求体模板，支持 `${page}` `${cursor}` |
| `gid.connector.http.scan.body-content-type` | 否 | `application/json` | 请求体 Content-Type |
| `gid.connector.http.scan.header.<HEADER_NAME>` | 否 | — | 请求头，多行 |
| `gid.connector.http.scan.content-field` | 否 | — | JSONPath，剥壳出记录数组或对象 |

#### B. 分页

| 选项 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `gid.connector.http.scan.pagination.type` | 否 | `none` | `none` / `page-number` / `cursor` |
| `gid.connector.http.scan.pagination.page-field` | page-number 必填 | `page` | 页码占位符名 |
| `gid.connector.http.scan.pagination.start-page` | 否 | `1` | 起始页码 |
| `gid.connector.http.scan.pagination.batch-size` | 否 | — | 单页预期条数；本页行数 < batch-size 即停 |
| `gid.connector.http.scan.pagination.total-pages` | 否 | — | 硬上限总页数，**最先生效** |
| `gid.connector.http.scan.pagination.total-count-jsonpath` | 否 | — | 响应总数字段 JSONPath |
| `gid.connector.http.scan.pagination.has-more-jsonpath` | 否 | — | 响应 has-more 字段 JSONPath |
| `gid.connector.http.scan.pagination.cursor-field` | cursor 必填 | `cursor` | 请求侧游标占位符名 |
| `gid.connector.http.scan.pagination.cursor-response-jsonpath` | cursor 必填 | — | 从响应抽下一页 cursor 的 JSONPath |
| `gid.connector.http.scan.pagination.initial-cursor` | 否 | 空 | 首次请求游标初始值 |

**停止策略优先级**（任一命中即停）：

1. `total-pages`
2. `total-count-jsonpath`：`rowsSeenTotal + rowsInPage >= total`
3. `has-more-jsonpath`：值为 `false` / `null`
4. `batch-size`：本页行数 < batch-size
5. 游标为空（游标分页兜底）

#### C. HTTP 传输（选项名与 lookup 保持一致或对齐）

| 选项 | 说明 |
|---|---|
| `gid.connector.http.scan.request.timeout` | 请求超时秒数（默认 30） |
| `gid.connector.http.scan.http-version` | `HTTP_1_1` / `HTTP_2` |
| `gid.connector.http.scan.retry-strategy.type` | `fixed-delay` / `exponential-delay` |
| `gid.connector.http.scan.retry-strategy.fixed-delay.delay` | fixed 延迟 |
| `gid.connector.http.scan.retry-strategy.exponential-delay.initial-backoff` / `max-backoff` / `backoff-multiplier` | 指数退避 |
| `gid.connector.http.scan.max-retries` | 每个请求最大重试次数（默认 3） |
| `gid.connector.http.scan.success-codes` | `2XX,404,!203` 语法 |
| `gid.connector.http.scan.retry-codes` | 同上 |
| `gid.connector.http.scan.ignored-response-codes` | 同上 |
| `gid.connector.http.scan.proxy.host` / `.port` / `.username` / `.password` | 代理 |
| `gid.connector.http.security.cert.server` / `.cert.client` / `.key.client` / `.cert.server.allowSelfSigned` | TLS/mTLS（沿用 key） |
| `gid.connector.http.security.oidc.token.request` / `.endpoint.url` / `.expiry.reduction` | OIDC（沿用 key） |
| `gid.connector.http.scan.use-raw-authorization-header` | Basic Auth 原样透传 |
| `gid.connector.http.logging.level` | `MIN` / `REQRESPONSE` / `MAX`（沿用） |

#### D. Format 透传

所有 `<format>.*` 选项（如 `json.fail-on-missing-field`、`json.ignore-parse-errors`）由 Flink `FactoryUtil` 自动透传给 Format 层，本连接器不解析。

### 2.3 四种场景 DDL 示例

#### 场景 ① 无参一次性拉全量

```sql
CREATE TABLE http_all (
  id BIGINT, name STRING, amount DECIMAL(18, 2), create_time TIMESTAMP(3)
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders/all',
  'format'    = 'json',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'json.ignore-parse-errors' = 'true'
);

INSERT INTO my_sink SELECT * FROM http_all;
```

#### 场景 ② 带参调用

```sql
CREATE TABLE http_by_category (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/items',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'category=book&region=CN',
  'gid.connector.http.scan.content-field' = '$.data.*'
);
```

#### 场景 ③A 页码分页 + batch-size 自动停止

```sql
CREATE TABLE http_paged (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.page-field' = 'page',
  'gid.connector.http.scan.pagination.start-page' = '1',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);
```

#### 场景 ③B 页码分页 + total-count-jsonpath 停止

```sql
CREATE TABLE http_paged_total (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.list.*',
  'gid.connector.http.scan.pagination.type'                  = 'page-number',
  'gid.connector.http.scan.pagination.start-page'            = '1',
  'gid.connector.http.scan.pagination.total-count-jsonpath'  = '$.data.total'
);
```

#### 场景 ④ 游标分页

```sql
CREATE TABLE http_cursor (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'cursor=${cursor}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'                      = 'cursor',
  'gid.connector.http.scan.pagination.cursor-field'              = 'cursor',
  'gid.connector.http.scan.pagination.cursor-response-jsonpath'  = '$.paging.next_cursor',
  'gid.connector.http.scan.pagination.initial-cursor'            = ''
);
```

#### 场景补充 POST body 分页

```sql
CREATE TABLE http_post_paged (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders/search',
  'method'    = 'POST',
  'format'    = 'json',
  'gid.connector.http.scan.body' = '{"page":${page},"size":100}',
  'gid.connector.http.scan.body-content-type' = 'application/json',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);
```

#### 场景补充 URL 路径变量

```sql
CREATE TABLE http_path_var (id BIGINT, name STRING) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/customers/{cid}/orders?page=${page}&size=100',
  'format'    = 'json',
  'gid.connector.http.scan.url-vars' = 'cid:C1001',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);
```

---

## 3. Source 运行时数据流

### 3.1 数据流概览（单并行度、BATCH 语义）

```
Factory → DynamicTableSource → SourceProvider → Source
                                                  ├─ SplitEnumerator（1 个 split）
                                                  └─ SourceReader（循环 pull-paginate-decode-collect）
```

### 3.2 Reader 主循环伪代码

```java
// HttpScanSourceReader.pollNext
while (!paginationEnded) {
    Optional<HttpRequest> reqOpt = paginationStrategy.nextRequest(state);
    if (reqOpt.isEmpty()) break;

    HttpResponse<String> resp = httpClient.send(reqOpt.get());   // 含 retry

    switch (statusClassifier.classify(resp.statusCode())) {
        case SUCCESS: {
            List<byte[]> records = jsonPathExtractor.extract(resp.body(), config.contentField);
            for (byte[] rec : records) {
                RowData row = deserializer.deserialize(rec);
                if (row != null) readerOutput.collect(row);
            }
            state = paginationStrategy.updateState(state, resp, records.size());
            paginationEnded = paginationStrategy.shouldStop(state, resp, records.size());
            break;
        }
        case IGNORED:
            // 不产生行，仍推进分页 state
            state = paginationStrategy.updateState(state, resp, 0);
            paginationEnded = paginationStrategy.shouldStop(state, resp, 0);
            break;
        case RETRY:
            // resilience4j 已耗尽重试
            throw new HttpScanException("Retry exhausted, status=" + resp.statusCode());
        default:
            throw new HttpScanException("Unclassified status=" + resp.statusCode());
    }
}
return InputStatus.END_OF_INPUT;
```

### 3.3 组件职责边界

| 组件 | 输入 | 输出 | 只做一件事 |
|---|---|---|---|
| `HttpScanConfig` | `ReadableConfig` | 不可变 POJO | 承载运行期配置 |
| `HttpScanSource` | Config + Deserializer | Enumerator / Reader 工厂 | FLIP-27 入口 |
| `HttpScanSplitEnumerator` | 无 | 一次性发一个 split | 只发一次 |
| `HttpScanSplit` | 无 | 初始 pagination state | 携带初始状态 |
| `HttpScanSourceReader` | Split | RowData 流 | 循环拉取 + 反序列化 + collect |
| `PaginationStrategy` | 当前 state + 上次 response | `Optional<Request>` + 新 state | 判断"下一次请求什么，是否结束" |
| `ScanRequestTemplate` | url/method/headers/body/query-params + state | `HttpRequest` | 只做占位符替换与请求装配 |
| `HttpScanHttpClient` | `HttpRequest` | `HttpResponse<String>` | 只做 IO + retry + auth |
| `JsonPathExtractor` | 响应体 + `content-field` | `List<byte[]>` + 抽 total/has-more/cursor | 剥壳与响应字段抽取 |
| `DeserializationSchema` | 单条 JSON bytes | `RowData` | 完全复用 flink-json |

### 3.4 分页状态

不可变 `PaginationState`：

```
pageNumber      // page-number 使用
cursor          // cursor 使用
rowsSeenTotal   // 供 total-count 停止判定
requestCount    // 供 total-pages 停止判定
```

`nextRequest(state)` 生成请求；`updateState(state, response, rowsInPage)` 生成新 state；`shouldStop(...)` 按 2.2.B 的优先级顺序判定。

### 3.5 请求构造

**占位符替换顺序**（防止相互污染）：

1. `url-vars` 静态值先替换 URL 路径中的 `{name}` —— 与运行状态无关，一次性完成
2. 每次迭代：`${page}` / `${cursor}` 替换到 URL / `query-params` / `body`
3. Header 只在构造 client 时装配一次，不参与占位符替换

**URL 编码**：
- `query-params` 选项值本身由用户负责："`&` 与 `=` 是保留分隔符"；对占位符替换后的最终 query 值执行 `URLEncoder.encode(value, UTF_8)`。
- URL 路径变量（`{name}`）替换时**原样替换**，用户提供的 `url-vars` 值必须已经 URL 编码。

### 3.6 错误处理

| 情形 | 行为 |
|---|---|
| `success-codes` 命中 | 正常处理 |
| `retry-codes` 命中 | resilience4j 自动重试，耗尽后 → job fail |
| `ignored-response-codes` 命中 | 跳过本页内容，**仍推进分页 state** |
| 其他 code | 抛 `HttpScanException` → job fail |
| `IOException` | 交给 retry；重试耗尽 → job fail |
| 反序列化失败 | 依赖 `json.ignore-parse-errors` 等 Format 自身选项 |

**明确不支持** `continue-on-error`。

### 3.7 投影下推

- 只做**列名/顺序投影**，不做过滤下推。
- 收到 `projectedFields` 后重建 `DataType`，把新 `DataType` 传给 Format 生成 `DeserializationSchema`。
- **不对 JSON 原始字节做字段裁剪**，由 flink-json 内部处理。

### 3.8 checkpoint 行为

- **不支持断点续传**。
- `HttpScanSplitSerializer` 提供最小可用实现（序列化空对象即可）以满足 FLIP-27 API 要求。
- README 明确写出该限制。

### 3.9 与 lookup 的隔离

新增类**不 import** 任何 `internal.table.lookup.*` 下的类；共享代码只走 `internal.security.*` / `internal.auth.*` / `internal.retry.*` / `internal.status.*` / `internal.HttpLogger`。

---

## 4. 数据类型与 schema 校验规则

### 4.1 支持的 SQL 数据类型

**结论**：完全依赖 `flink-json` 的类型能力，不自造类型系统。

支持 flink-json 覆盖的全部类型：
- 数值：`TINYINT` `SMALLINT` `INT` `BIGINT` `FLOAT` `DOUBLE` `DECIMAL(p,s)`
- 字符/二进制：`CHAR(n)` `VARCHAR(n)` `STRING` `BINARY(n)` `VARBINARY(n)` `BYTES`
- 布尔：`BOOLEAN`
- 时间：`DATE` `TIME` `TIMESTAMP(p)` `TIMESTAMP_LTZ(p)`
- 复合：`ARRAY<T>` `MAP<K,V>` `ROW<...>`（任意嵌套）
- 所有类型可 `NULL`

不支持的类型由 flink-json 自身抛 `ValidationException`，连接器不加白名单。

### 4.2 强校验规则（`ValidationException`）

| # | 规则 |
|---|---|
| 1 | `connector` `url` `format` 必填 |
| 2 | `method` ∈ `{GET, POST, PUT}`，大小写不敏感 |
| 3 | `method = GET` 时，禁止配置 `body` |
| 4 | `pagination.type` ∈ `{none, page-number, cursor}` |
| 5 | `pagination.type = cursor` 时，`cursor-response-jsonpath` 必填 |
| 6 | `pagination.type = page-number` 时，url / query-params / body 至少一处必须出现 `${page}` |
| 7 | `pagination.type = cursor` 时，同上要求出现 `${cursor}` |
| 8 | `pagination.type = none` 时，字符串中不得出现 `${page}` `${cursor}` |
| 9 | `url-vars` 的每个 `{name}` 与 URL 中的 `{name}` 双向匹配 |
| 10 | JSONPath 表达式必须能被 Jackson JsonPath 编译通过（编译期校验） |
| 11 | `start-page` ≥ 0；`batch-size` > 0；`total-pages` > 0；`max-retries` ≥ 0；超时 > 0 |
| 12 | TLS/mTLS 证书路径不做客户端存在性校验（保持与 lookup 一致） |
| 13 | 未知选项 → `ValidationException`（`FactoryUtil.validateExcept` 默认，Format 前缀与 connector 前缀自动放行） |

### 4.3 软校验规则（DEBUG 日志提示，不 fail）

| # | 提示 |
|---|---|
| S1 | `pagination.type != none` 但没配任何停止策略 → 提示"可能永久循环" |
| S2 | `content-field` 未配 → 提示"未剥壳，请确保 API 顶层返回记录数组或对象" |
| S3 | `format=json` 未加严格/宽松开关 → 提示默认严格模式 |

### 4.4 Metadata 列

沿用 lookup metadata 语义，v1 提供 3 个：

| Metadata Key | Data Type | 说明 |
|---|---|---|
| `http-status-code` | `INT` | 该行所在 HTTP 响应的状态码 |
| `http-headers-map` | `MAP<STRING, ARRAY<STRING>>` | 该行所在 HTTP 响应的响应头 |
| `page-number` | `INT` | 该行所在页码；游标分页为 request 序号（1-based） |

`http-status-code` / `http-headers-map` 复用 lookup 的 `MetadataConverter` 思路但独立实现 `HttpScanMetadataConverter`，避免与 lookup 耦合。`page-number` 为 scan 专属。

### 4.5 空响应 / 空数组 / 单对象处理

| 响应形态 | 行为 |
|---|---|
| `content-field` 未配 + 顶层是数组 | 逐条产出 |
| `content-field` 未配 + 顶层是单对象 | 产出 1 行 |
| `content-field` 已配 + 剥壳后是数组 | 逐条产出 |
| `content-field` 已配 + 剥壳后是单对象 | 产出 1 行 |
| `content-field` 已配 + 剥壳未命中 / null | 产出 0 行，不视为错误，分页仍推进 |
| 剥壳后是非对象数组（如字符串数组） | 抛 `HttpScanException` |

### 4.6 与 Format 的边界

- 连接器把**每条记录的原始 JSON 字节**交给 `DeserializationSchema.deserialize(byte[])`。
- Format 负责类型转换、字段缺失/多余处理、`json.ignore-parse-errors` 等。
- 连接器**不**读取、不解析 Format 内部选项（`json.*` 由 FactoryUtil 直接透传）。

---

## 5. 测试策略

### 5.1 测试栈

沿用项目现状：JUnit 5 + AssertJ + Mockito + WireMock 3.13.2；JaCoCo `line ≥ 90% / branch ≥ 90% / method ≥ 80%`。

测试镜像目录：`src/test/java/com/getindata/connectors/http/internal/table/scan/`

### 5.2 测试金字塔

- ① 单元测试 ≈ 80 例
- ② 集成测试（WireMock，不启动 Flink）≈ 30 例
- ③ 端到端 SQL 测试（MiniCluster + WireMock）≈ 10 例

### 5.3 单元测试（分类列表）

| 被测类 | 用例数 |
|---|---|
| `HttpScanConnectorOptions` | 5 |
| `HttpScanConfig` | 5 |
| `HttpScanTableSourceFactory`（4.2 每条规则正反两例） | 20 |
| `PlaceholderResolver` | 8 |
| `ScanRequestTemplate` | 6 |
| `JsonPathExtractor` | 9 |
| `NoPagination` | 2 |
| `PageNumberPagination` | 12 |
| `CursorPagination` | 6 |
| `HttpScanSplit` / `HttpScanSplitSerializer` | 3 |
| `HttpScanSplitEnumerator` | 3 |
| `HttpScanMetadataConverter` | 3 |

单元测试**不启动网络**、**不启动 Flink**；`HttpScanHttpClient` 全部 mock。

### 5.4 集成测试（`HttpScanSourceReaderIT`，25 个场景）

覆盖：无参、带参、`url-vars`、页码 batch-size 停、页码 total-pages 停、页码 total-count-jsonpath 停、页码 has-more-jsonpath 停、停止策略优先级、游标正常流程、游标 3 种空值变体停、POST body 分页、空剥壳仍推进、`retry-codes` 成功、`retry-codes` 耗尽、`ignored-response-codes`、未分类错误、`json.ignore-parse-errors` 生效、Basic Auth、自定义 header、大响应、慢响应超时、重定向。

### 5.5 端到端 SQL 测试（`HttpScanTableSourceITCase`，10 个用例）

覆盖：无参一次性 → PRINT sink、页码分页 batch-size、页码分页 total-count、游标分页、POST body 分页、URL 路径变量、投影下推、3 个 metadata 列同时使用、与 datagen 表 JOIN、HTTP 失败 → Job 失败。

### 5.6 手工验证清单

- [ ] `mvn clean verify` 通过（含 checkstyle + jacoco）
- [ ] `META-INF/services/...factories.Factory` 追加一行
- [ ] Flink 1.17.2 SQL Client 跑一次场景 ①、③A、④
- [ ] Flink 1.18.1 跑一次同样三个用例
- [ ] Fat jar 大小无异常增长

### 5.7 明确不做的测试

多并行度、checkpoint 恢复、`continue-on-error`、Format 内部类型转换、TLS/OIDC/Retry 内部逻辑（已由现有 lookup 测试覆盖）、压测。

---

## 6. 发布、文档与版本管理

### 6.1 版本号

**建议**：v1 发布为 `0.27.0`（新增 connector，非破坏性变更），走 pom 已有的 `mvn -DbumpMinor` profile。

### 6.2 运行时兼容性

- Flink **1.17.x** 与 **1.18.x**（编译期锁 1.18.1，实现只使用两版本共有 API）。
- Java 11。
- 不破坏 lookup / sink 的任何现有配置项、行为。

### 6.3 服务发现文件变更

`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory` **追加一行**：

```
com.getindata.connectors.http.internal.table.scan.HttpScanTableSourceFactory
```

现有条目不删不改。

### 6.4 pom.xml 变更（最小）

- **不引入新依赖**。
- JaCoCo excludes 追加 `**/HttpScanConnectorOptions.class`（与 `HttpLookupConnectorOptions.class` 一致处理）。

### 6.5 README.md 变更

在 `### HTTP Sink` 之前**新增章节** `### HTTP Scan Source`，包含：
- 简介与定位
- 四种典型 DDL 示例
- 分页停止策略优先级
- 状态码分类语义
- Available Metadata 表
- Limitations（单并行度、无 checkpoint、无 continue-on-error、兼容 Flink 1.17/1.18）

在 `### Table API Connector Options` 后追加 HTTP Scan Source 选项表。

`Breaking changes` 章节追加：

```
- Version 0.27
  - Added new connector `http-scan` for bounded HTTP source in Flink SQL.
    Not a breaking change; existing `rest-lookup` and `http-sink` are untouched.
```

### 6.6 CHANGELOG.md 变更

```
## [0.27.0] - YYYY-MM-DD
### Added
- New connector `http-scan`: scan HTTP API as Flink SQL source with pagination
  (page-number, cursor) and content-field JSONPath extraction. Supports GET/POST/PUT,
  URL path variables, query params, body templates, TLS/mTLS, Basic/OIDC auth, retries,
  proxy. Runtime compatible with Flink 1.17.x and 1.18.x.
```

### 6.7 打包与发布

沿用现有 maven-shade fat jar 与 Sonatype central publishing。新连接器与 lookup、sink 打入同一 fat jar，用户仍是一份 `flink-http-connector-0.27.0.jar`。

### 6.8 PR 拆分建议（供 writing-plans 参考）

| PR | 内容 | 依赖 |
|---|---|---|
| PR-1 | 设计文档 + CHANGELOG + Options 类骨架 + Factory 骨架 | 无 |
| PR-2 | 分页策略层 + PlaceholderResolver + JsonPathExtractor + 单元测试 | PR-1 |
| PR-3 | FLIP-27 Source 层 + HttpClient 装配 + 集成测试 | PR-2 |
| PR-4 | DynamicTableSource 接入 + META-INF/services 注册 + 端到端 SQL 测试 + README | PR-3 |

---

## 附录 A：澄清阶段决策纪要

| # | 问题 | 决策 |
|---|---|---|
| 1 | Source API 选型 | FLIP-27 新版 Source（BOUNDED），兼容 Flink 1.17.x / 1.18.x |
| 2 | 并行度模型 | 单并行度（v1） |
| 3 | 分页停止策略 | 四种全支持：`total-pages`、`total-count-jsonpath`、`has-more-jsonpath`、`batch-size` |
| 4 | 数据解析方式 | 复用 Flink Format，同时提供可选 `content-field` JSONPath 剥壳 |
| 5 | 参数传递位置 | URL query、URL 路径占位符、POST body 模板 |
| 6 | 复用能力 | 认证与 TLS/mTLS、重试策略、HTTP 代理、请求/响应日志 |
| 7 | checkpoint 与失败恢复 | 失败即作业失败，不支持断点续传 |
| 8 | 代码定位 | 本项目内新增 connector，与 rest-lookup 并列 |

---

## 附录 B：参考资料

- getindata/flink-http-connector README 中"分页未支持"声明：`README.md:262-267`
- Flink FLIP-27 Source API：`org.apache.flink.api.connector.source.Source`
- Apache SeaTunnel HTTP Source（作为分页策略与停止条件的设计参考）：<https://seatunnel.apache.org/docs/connectors/source/Http/>
- Ververica 博客 ING Bank Custom HTTP Connector（未开源）：<https://www.ververica.com/blog/performing-api-calls-via-a-custom-http-connector-using-flink-sql>

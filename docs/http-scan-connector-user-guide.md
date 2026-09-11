# HTTP Scan Connector 使用说明文档

## 目录

- [1. 概述](#1-概述)
- [2. 快速开始](#2-快速开始)
- [3. JAR 依赖与安装](#3-jar-依赖与安装)
- [4. 连接器配置参数](#4-连接器配置参数)
  - [4.1 基础请求参数](#41-基础请求参数)
  - [4.2 分页参数](#42-分页参数)
  - [4.3 HTTP 传输参数](#43-http-传输参数)
  - [4.4 连接与代理参数](#44-连接与代理参数)
  - [4.5 安全与认证参数](#45-安全与认证参数)
  - [4.6 日志参数](#46-日志参数)
  - [4.7 Format 透传参数](#47-format-透传参数)
- [5. 使用场景详解](#5-使用场景详解)
  - [5.1 无参一次性拉全量](#51-无参一次性拉全量)
  - [5.2 带参调用](#52-带参调用)
  - [5.3 页码分页](#53-页码分页)
  - [5.4 游标分页](#54-游标分页)
  - [5.5 POST Body 分页](#55-post-body-分页)
  - [5.6 URL 路径变量](#56-url-路径变量)
- [6. 分页停止策略](#6-分页停止策略)
- [7. HTTP 状态码处理](#7-http-状态码处理)
- [8. 内容提取 (Content Field)](#8-内容提取-content-field)
- [9. 认证与安全](#9-认证与安全)
  - [9.1 Basic Authentication](#91-basic-authentication)
  - [9.2 OIDC Bearer Authentication](#92-oidc-bearer-authentication)
  - [9.3 TLS/mTLS](#93-tlsmtls)
- [10. 重试策略](#10-重试策略)
- [11. 错误处理](#11-错误处理)
- [12. 当前限制 (v1)](#12-当前限制-v1)
- [13. 完整配置参数表](#13-完整配置参数表)

---

## 1. 概述

`http-scan` 是 `getindata/flink-http-connector` 项目新增的 Flink SQL 连接器，用于将 HTTP/REST API 作为**有界扫描源**（Bounded Source）直接在 Flink SQL 中使用。

### 核心特性

- **直接 SQL 访问**：通过 `CREATE TABLE ... WITH ('connector'='http-scan')` + `INSERT INTO sink SELECT * FROM http_table` 直接读取 HTTP API
- **有界源**：基于 Flink FLIP-27 Source API（`Boundedness.BOUNDED`），适合作为批处理任务的数据源
- **分页支持**：支持页码分页（page-number）和游标分页（cursor）两种模式
- **灵活参数传递**：支持 URL Query 参数、URL 路径变量、POST/PUT Body 模板
- **内容提取**：支持 JSONPath 从响应体中提取记录数组
- **企业级特性**：复用项目现有的 TLS/mTLS、Basic/OIDC 认证、重试策略、HTTP 代理等能力

### 与 rest-lookup 的区别

| 特性 | http-scan | rest-lookup |
|------|-----------|-------------|
| 使用方式 | 独立扫描源 | Lookup Join（需要驱动表） |
| 分页支持 | ✅ 支持页码/游标分页 | ❌ 不支持分页 |
| 有界性 | 有界（Bounded） | 无界（Unbounded） |
| 典型场景 | 批量拉取 API 数据 | 实时维度表关联 |

---

## 2. 快速开始

### 最小化示例

```sql
-- 创建 http-scan 表
CREATE TABLE http_orders (
  id BIGINT,
  name STRING,
  amount DECIMAL(18, 2)
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json'
);

-- 使用 INSERT INTO 读取数据
INSERT INTO my_sink SELECT * FROM http_orders;
```

### 带分页的示例

```sql
CREATE TABLE http_orders_paged (
  id BIGINT,
  name STRING,
  amount DECIMAL(18, 2)
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);

INSERT INTO my_sink SELECT * FROM http_orders_paged;
```

---

## 3. JAR 依赖与安装

### 获取 JAR

从 GitHub [Releases](https://github.com/tuxin-labs/flink-http-connector/releases) 页面下载
`flink-http-connector-<version>.jar`（本连接器为本仓库 fork 版本，不发布到 Maven Central）。

如需从源码构建：

```bash
git clone https://github.com/tuxin-labs/flink-http-connector.git
cd flink-http-connector
mvn verify
# 构建产物位于 target/flink-http-connector-<version>.jar
```

### Flink SQL Client 使用

启动 SQL Client 时通过 `-j` 指定连接器 JAR：

```bash
./bin/sql-client.sh -j flink-http-connector-0.27.0.jar
```

或将其复制到 Flink 的 `lib` 目录后重启集群：

```bash
cp flink-http-connector-0.27.0.jar $FLINK_HOME/lib/
```

### 运行时依赖

此连接器依赖以下 Flink 运行时组件（通常由 Flink 集群提供）：

- `org.apache.flink:flink-java`
- `org.apache.flink:flink-clients`
- `org.apache.flink:flink-connector-base`
- `org.apache.flink:flink-json`（用于 JSON 格式解析）

### 环境要求

- **Java**: 11+
- **Flink**: 1.17.x 或 1.18.x（推荐 1.18.x）
- **Maven**: 3.x（仅构建时）

---

## 4. 连接器配置参数

### 4.1 基础请求参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `connector` | ✅ 是 | — | 固定值 `http-scan` |
| `url` | ✅ 是 | — | HTTP 端点 URL，可含 `{path_var}` 占位符 |
| `format` | ✅ 是 | — | Flink Format 名，如 `json`、`csv` 等 |
| `method` | ❌ 否 | `GET` | HTTP 方法：`GET` / `POST` / `PUT` |
| `gid.connector.http.scan.url-vars` | ❌ 否 | — | URL 路径变量静态取值，格式 `key1:v1,key2:v2` |
| `gid.connector.http.scan.query-params` | ❌ 否 | — | URL Query 参数，格式 `k1=v1&k2=v2`，支持 `${page}` / `${cursor}` 占位符 |
| `gid.connector.http.scan.body` | ❌ 否 | — | POST/PUT 请求体模板，支持 `${page}` / `${cursor}` 占位符 |
| `gid.connector.http.scan.body-content-type` | ❌ 否 | `application/json` | 请求体 Content-Type |
| `gid.connector.http.scan.content-field` | ❌ 否 | — | JSONPath，从响应体剥出记录数组或对象 |

#### url 参数详解

`url` 是请求的基础地址，支持以下特性：

- **静态 URL**：`https://api.example.com/orders`
- **带路径变量的 URL**：`https://api.example.com/customers/{cid}/orders`
  - 路径变量 `{cid}` 的值通过 `gid.connector.http.scan.url-vars` 配置
- **带查询参数占位符的 URL**：`https://api.example.com/orders?page=${page}&size=100`
  - 占位符 `${page}` / `${cursor}` 在每次分页请求时自动替换

#### method 参数详解

| 值 | 说明 | 限制 |
|----|------|------|
| `GET` | HTTP GET 请求 | 不允许配置 `body` |
| `POST` | HTTP POST 请求 | 可配置 `body` |
| `PUT` | HTTP PUT 请求 | 可配置 `body` |

#### query-params 参数详解

格式：`k1=v1&k2=v2`

- `&` 和 `=` 是分隔符，不能作为值的一部分
- 如果值本身需要包含 `&` 或 `=` 字符，用户须提前 URL 编码
- 支持 `${page}` 和 `${cursor}` 占位符，运行时自动替换

示例：
```sql
'gid.connector.http.scan.query-params' = 'category=book&region=CN&page=${page}&size=100'
```

#### body 参数详解

用于 POST/PUT 请求的请求体模板：

- 支持 `${page}` 和 `${cursor}` 占位符
- 默认 Content-Type 为 `application/json`
- 可通过 `body-content-type` 修改

示例：
```sql
'gid.connector.http.scan.body' = '{"page":${page},"size":100,"filter":"active"}'
'gid.connector.http.scan.body-content-type' = 'application/json'
```

#### content-field 参数详解

使用 JSONPath 语法从响应体中提取记录：

| JSONPath 示例 | 说明 |
|---------------|------|
| `$.data` | 提取 `data` 字段（可以是数组或对象） |
| `$.data.*` | 提取 `data` 下的所有元素（通配符） |
| `$.result.items` | 提取嵌套字段 |
| `$.records` | 提取 `records` 数组 |

**注意**：
- 路径未命中或为 `null` 时返回空列表（不视为错误）
- 如果未配置，响应顶层必须是数组或对象
- JSONPath 必须以 `$` 开头

---

### 4.2 分页参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gid.connector.http.scan.pagination.type` | ❌ 否 | `none` | 分页类型：`none` / `page-number` / `cursor` |
| `gid.connector.http.scan.pagination.page-field` | page-number 模式必填 | `page` | 页码占位符名（用于 `${page}` 替换） |
| `gid.connector.http.scan.pagination.start-page` | ❌ 否 | `1` | 起始页码（≥ 0） |
| `gid.connector.http.scan.pagination.batch-size` | ❌ 否 | — | 单页预期条数；本页行数 < batch-size 即停（> 0） |
| `gid.connector.http.scan.pagination.total-pages` | ❌ 否 | — | 硬上限总页数；命中即最先生效（> 0） |
| `gid.connector.http.scan.pagination.max-requests` | ❌ 否 | `10000` | 分页请求硬上限安全阀（> 0），达到即停，防止无限扫描 |
| `gid.connector.http.scan.pagination.total-count-jsonpath` | ❌ 否 | — | 响应总数字段 JSONPath，如 `$.total` |
| `gid.connector.http.scan.pagination.has-more-jsonpath` | ❌ 否 | — | 响应 has-more 字段 JSONPath；为 `false` / `null` 即停 |
| `gid.connector.http.scan.pagination.cursor-field` | cursor 模式必填 | `cursor` | 游标占位符名（用于 `${cursor}` 替换） |
| `gid.connector.http.scan.pagination.cursor-response-jsonpath` | cursor 模式必填 | — | 从响应抽下一页 cursor 的 JSONPath |
| `gid.connector.http.scan.pagination.initial-cursor` | ❌ 否 | 空字符串 | 首次请求游标初始值 |

#### 分页类型说明

**none（默认）**：不分页，一次性拉取所有数据。

**page-number**：页码分页
- 每次请求自动递增页码
- `${page}` 占位符被替换为当前页码
- 至少需要在 url、query-params 或 body 中使用 `${page}` 占位符

**cursor**：游标分页
- 从响应中提取下一页的游标值
- `${cursor}` 占位符被替换为当前游标值
- 必须配置 `cursor-response-jsonpath`
- 游标为空或 `null` 时停止分页

#### 占位符使用规则

| 分页类型 | 允许的占位符 | 必须出现的位置 |
|----------|-------------|----------------|
| `none` | 不允许 `${page}` 或 `${cursor}` | — |
| `page-number` | `${page}` | url、query-params 或 body 至少一处 |
| `cursor` | `${cursor}` | url、query-params 或 body 至少一处 |

---

### 4.3 HTTP 传输参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gid.connector.http.scan.request.timeout` | ❌ 否 | `30` | 请求超时秒数 |
| `gid.connector.http.scan.http-version` | ❌ 否 | — | HTTP 版本：`HTTP_1_1` / `HTTP_2` |
| `gid.connector.http.scan.max-retries` | ❌ 否 | `3` | 每个请求最大重试次数（≥ 0，设为 0 禁用重试） |

#### request.timeout 参数详解

设置单个 HTTP 请求的超时时间（秒）。如果请求在超时时间内未完成，将抛出超时异常。

示例：
```sql
'gid.connector.http.scan.request.timeout' = '60'  -- 60秒超时
```

#### http-version 参数详解

指定使用的 HTTP 协议版本：

| 值 | 说明 |
|----|------|
| `HTTP_1_1` | HTTP/1.1 |
| `HTTP_2` | HTTP/2 |

**注意**：某些 HTTP/1.1 端点可能拒绝 HTTP/2 请求，此时需要显式设置为 `HTTP_1_1`。

---

### 4.4 连接与代理参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gid.connector.http.scan.connection.timeout` | ❌ 否 | — | 连接超时（Duration 格式，如 `PT30S`） |
| `gid.connector.http.scan.proxy.host` | ❌ 否 | — | 代理主机名 |
| `gid.connector.http.scan.proxy.port` | ❌ 否 | — | 代理端口 |
| `gid.connector.http.scan.proxy.username` | ❌ 否 | — | 代理用户名 |
| `gid.connector.http.scan.proxy.password` | ❌ 否 | — | 代理密码 |

#### 代理配置示例

```sql
CREATE TABLE http_with_proxy (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.proxy.host'     = 'proxy.example.com',
  'gid.connector.http.scan.proxy.port'     = '8080',
  'gid.connector.http.scan.proxy.username' = 'user',
  'gid.connector.http.scan.proxy.password' = 'pass'
);
```

---

### 4.5 安全与认证参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gid.connector.http.security.cert.server` | ❌ 否 | — | 服务器证书路径（逗号分隔多个） |
| `gid.connector.http.security.cert.client` | ❌ 否 | — | 客户端证书路径（mTLS） |
| `gid.connector.http.security.key.client` | ❌ 否 | — | 客户端私钥路径（mTLS，PKCS8 格式） |
| `gid.connector.http.security.cert.server.allowSelfSigned` | ❌ 否 | — | 允许自签名证书（`true`/`false`） |
| `gid.connector.http.security.oidc.token.endpoint.url` | ❌ 否 | — | OIDC Token Endpoint URL |
| `gid.connector.http.security.oidc.token.request` | ❌ 否 | — | OIDC Token Request Body（URL 编码格式） |
| `gid.connector.http.security.oidc.token.expiry.reduction` | ❌ 否 | `PT1S` | Token 过期提前刷新时间 |
| `gid.connector.http.scan.use-raw-authorization-header` | ❌ 否 | `false` | 是否原样透传 Authorization header |

---

### 4.6 日志参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gid.connector.http.logging.level` | ❌ 否 | `MIN` | HTTP 日志级别 |

#### 日志级别说明

| 级别 | 记录内容 |
|------|----------|
| `MIN` | 请求方法、URI、响应状态码 |
| `REQRESPONSE` | 请求方法、URI、请求体、响应状态码、响应体 |
| `MAX` | 请求方法、URI、请求体、请求头、响应状态码、响应体、响应头 |

**注意**：`REQRESPONSE` 和 `MAX` 级别可能记录敏感信息（如认证 Token），请勿在生产环境使用。

示例：
```sql
'gid.connector.http.logging.level' = 'REQRESPONSE'
```

---

### 4.7 Format 透传参数

所有 `<format>.*` 格式选项由 Flink `FactoryUtil` 自动透传给 Format 层，连接器本身不解析。

常用 JSON 格式选项：

| 参数 | 说明 |
|------|------|
| `json.ignore-parse-errors` | 忽略 JSON 解析错误（`true`/`false`） |
| `json.fail-on-missing-field` | 缺失字段时报错（`true`/`false`） |
| `json.timestamp-format.standard` | 时间戳格式标准（`SQL`/`ISO-8601`） |

示例：
```sql
CREATE TABLE http_data (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'false'
);
```

---

## 5. 使用场景详解

### 5.1 无参一次性拉全量

**场景**：API 一次性返回所有数据，无需分页。

```sql
CREATE TABLE http_all_orders (
  id BIGINT,
  name STRING,
  amount DECIMAL(18, 2),
  create_time TIMESTAMP(3)
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders/all',
  'format'    = 'json',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'json.ignore-parse-errors' = 'true'
);

INSERT INTO my_sink SELECT * FROM http_all_orders;
```

**关键配置**：
- 不配置 `pagination.type`（默认为 `none`）
- 配置 `content-field` 提取记录数组
- 确保 API 返回的顶层是数组或对象

---

### 5.2 带参调用

**场景**：API 需要传递固定参数（非分页参数）。

#### 使用 Query 参数

```sql
CREATE TABLE http_by_category (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/items',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'category=book&region=CN',
  'gid.connector.http.scan.content-field' = '$.data.*'
);

INSERT INTO my_sink SELECT * FROM http_by_category;
```

#### 使用 URL 路径变量

```sql
CREATE TABLE http_customer_orders (
  id BIGINT,
  order_date STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/customers/{cid}/orders',
  'format'    = 'json',
  'gid.connector.http.scan.url-vars' = 'cid:C1001',
  'gid.connector.http.scan.content-field' = '$.data.*'
);

INSERT INTO my_sink SELECT * FROM http_customer_orders;
```

---

### 5.3 页码分页

**场景**：API 使用页码进行分页，每页返回固定数量的记录。

#### 使用 batch-size 自动停止

```sql
CREATE TABLE http_paged_orders (
  id BIGINT,
  name STRING
) WITH (
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

INSERT INTO my_sink SELECT * FROM http_paged_orders;
```

**停止条件**：当某页返回的记录数 < 100 时停止。

#### 使用 total-pages 硬上限

```sql
CREATE TABLE http_paged_limited (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=50',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'        = 'page-number',
  'gid.connector.http.scan.pagination.start-page'  = '1',
  'gid.connector.http.scan.pagination.total-pages' = '10'
);

INSERT INTO my_sink SELECT * FROM http_paged_limited;
```

**停止条件**：最多请求 10 页。

#### 使用 total-count-jsonpath 停止

```sql
CREATE TABLE http_paged_total (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.list.*',
  'gid.connector.http.scan.pagination.type'                  = 'page-number',
  'gid.connector.http.scan.pagination.start-page'            = '1',
  'gid.connector.http.scan.pagination.total-count-jsonpath'  = '$.data.total'
);

INSERT INTO my_sink SELECT * FROM http_paged_total;
```

**停止条件**：当累计拉取行数 >= `$.data.total` 的值时停止。

#### 使用 has-more-jsonpath 停止

```sql
CREATE TABLE http_paged_has_more (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'                = 'page-number',
  'gid.connector.http.scan.pagination.start-page'          = '1',
  'gid.connector.http.scan.pagination.has-more-jsonpath'   = '$.has_more'
);

INSERT INTO my_sink SELECT * FROM http_paged_has_more;
```

**停止条件**：当 `$.has_more` 的值为 `false` 或 `null` 时停止。

---

### 5.4 游标分页

**场景**：API 使用游标（cursor/token）进行分页，每页返回下一页的游标值。

```sql
CREATE TABLE http_cursor_orders (
  id BIGINT,
  name STRING
) WITH (
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

INSERT INTO my_sink SELECT * FROM http_cursor_orders;
```

**关键配置**：
- `pagination.type = cursor`
- `cursor-field`：请求参数中游标的字段名
- `cursor-response-jsonpath`：从响应中提取下一页游标的 JSONPath
- `initial-cursor`：首次请求的游标值（可为空）

**停止条件**：当响应中提取的游标值为 `null` 或空字符串时停止。此外，`pagination.max-requests`
安全阀（默认 10000）保证即使 API 异常持续返回非空游标，扫描也必然终止。

---

### 5.5 POST Body 分页

**场景**：API 使用 POST 请求，分页参数在请求体中。

```sql
CREATE TABLE http_post_paged (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders/search',
  'method'    = 'POST',
  'format'    = 'json',
  'gid.connector.http.scan.body' = '{"page":${page},"size":100,"filter":"active"}',
  'gid.connector.http.scan.body-content-type' = 'application/json',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);

INSERT INTO my_sink SELECT * FROM http_post_paged;
```

**关键配置**：
- `method = POST`
- `body` 中使用 `${page}` 占位符
- `body-content-type` 指定请求体类型

---

### 5.6 URL 路径变量

**场景**：API URL 中包含动态路径参数。

```sql
CREATE TABLE http_path_var (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/customers/{cid}/orders?page=${page}&size=100',
  'format'    = 'json',
  'gid.connector.http.scan.url-vars' = 'cid:C1001',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'       = 'page-number',
  'gid.connector.http.scan.pagination.batch-size' = '100'
);

INSERT INTO my_sink SELECT * FROM http_path_var;
```

**关键配置**：
- URL 中使用 `{cid}` 占位符
- `url-vars` 提供静态值 `cid:C1001`
- 路径变量与 URL 中的占位符必须双向匹配

**注意**：
- URL 中的 `{name}` 占位符会在构造请求时被替换
- `url-vars` 中的值必须已经 URL 编码（如果需要的话）

---

## 6. 分页停止策略

分页停止策略决定了何时停止拉取数据。对于 `page-number` 和 `cursor` 分页类型，支持以下停止策略（按优先级从高到低）：

### 优先级顺序

1. **max-requests**（最高优先级，安全阀）
   - 已发请求数达到 `max-requests` 配置值（默认 10000）即停，并输出 WARN 日志
   - 这是硬上限安全阀：即使所有停止条件都未配置（或 API 异常持续返回非空 cursor），
     扫描也必然终止。如需扫描更多数据请调大该值

2. **total-pages**
   - 已发请求数达到 `total-pages` 配置值即停
   - 这是硬上限，最先生效

3. **total-count-jsonpath**
   - 从响应中提取总数字段
   - 当 `累计拉取行数 >= 总数` 时停止
   - JSONPath 示例：`$.total`、`$.data.count`

4. **has-more-jsonpath**
   - 从响应中提取 has-more 字段
   - 当值为 `false` 或 `null` 时停止
   - JSONPath 示例：`$.has_more`、`$.pagination.hasNext`

5. **batch-size**
   - 当某页返回的记录数 < `batch-size` 配置值时停止
   - 适用于最后一页不满的情况

6. **游标为空**（仅 cursor 分页）
   - 当从响应中提取的游标值为 `null` 或空字符串时停止
   - 这是游标分页的兜底停止条件

### 停止策略组合示例

```sql
-- 使用多个停止策略（任一命中即停）
CREATE TABLE http_multi_stop (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/orders',
  'format'    = 'json',
  'gid.connector.http.scan.query-params' = 'page=${page}&size=100',
  'gid.connector.http.scan.content-field' = '$.data.*',
  'gid.connector.http.scan.pagination.type'                  = 'page-number',
  'gid.connector.http.scan.pagination.start-page'            = '1',
  'gid.connector.http.scan.pagination.batch-size'            = '100',
  'gid.connector.http.scan.pagination.total-pages'           = '50',
  'gid.connector.http.scan.pagination.total-count-jsonpath'  = '$.total',
  'gid.connector.http.scan.pagination.has-more-jsonpath'     = '$.has_more'
);
```

**停止条件**（按优先级）：
1. 达到 `max-requests` 硬上限（未配置时默认 10000）
2. 请求满 50 页
3. 累计行数 >= `$.total`
4. `$.has_more` 为 `false` 或 `null`
5. 某页行数 < 100

---

## 7. HTTP 状态码处理

连接器将 HTTP 响应状态码分为三类，使用语法 `2XX,404,!203` 进行配置。

### 状态码分类

| 类别 | 配置参数 | 说明 |
|------|----------|------|
| 成功 | `success-codes` | 正常处理响应内容 |
| 可重试 | `retry-codes` | 临时错误，自动重试 |
| 忽略 | `ignored-response-codes` | 跳过内容，但仍推进分页 |
| 错误 | 未分类的状态码 | 抛异常，作业失败 |

### 状态码语法

- **单个状态码**：`404`、`500`
- **状态码组**：`2XX`（200-299）、`4XX`（400-499）、`5XX`（500-599）
- **排除**：`!203`、`!501`
- **组合**：`2XX,404,!203`

### 默认配置

```sql
'gid.connector.http.scan.success-codes' = '2XX'
```

### 配置示例

```sql
CREATE TABLE http_custom_codes (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.success-codes'          = '2XX',
  'gid.connector.http.scan.retry-codes'            = '5XX,!501,!505',
  'gid.connector.http.scan.ignored-response-codes' = '404'
);
```

**说明**：
- 200-299：成功，正常处理
- 500-599（除 501、505）：可重试错误，自动重试
- 404：忽略，跳过内容但继续分页
- 其他状态码（如 400、403）：错误，作业失败

---

## 8. 内容提取 (Content Field)

`content-field` 参数使用 JSONPath 语法从响应体中提取记录。

### JSONPath 语法

| JSONPath | 说明 | 示例响应 | 提取结果 |
|----------|------|----------|----------|
| `$.data` | 提取 `data` 字段 | `{"data": [{"id":1}]}` | `[{"id":1}]` |
| `$.data.*` | 提取 `data` 下所有元素 | `{"data": [{"id":1}, {"id":2}]}` | `{"id":1}`, `{"id":2}` |
| `$.result.items` | 提取嵌套字段 | `{"result":{"items":[{"id":1}]}}` | `[{"id":1}]` |
| `$.records` | 提取 `records` 数组 | `{"records":[{"id":1}]}` | `[{"id":1}]` |

### 响应处理规则

| 响应形态 | 行为 |
|----------|------|
| `content-field` 未配 + 顶层是数组 | 逐条产出 |
| `content-field` 未配 + 顶层是单对象 | 产出 1 行 |
| `content-field` 已配 + 剥壳后是数组 | 逐条产出 |
| `content-field` 已配 + 剥壳后是单对象 | 产出 1 行 |
| `content-field` 已配 + 剥壳未命中 / null | 产出 0 行，不视为错误，分页仍推进 |
| 剥壳后是非对象数组（如字符串数组） | 抛异常 |

### 示例

#### API 返回标准格式

```json
{
  "code": 200,
  "data": {
    "list": [
      {"id": 1, "name": "Alice"},
      {"id": 2, "name": "Bob"}
    ],
    "total": 100
  }
}
```

配置：
```sql
'gid.connector.http.scan.content-field' = '$.data.list'
```

#### API 返回数组

```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

配置：
```sql
-- 不配置 content-field，或配置为 $
'gid.connector.http.scan.content-field' = '$'
```

---

## 9. 认证与安全

### 9.1 Basic Authentication

#### 自动编码模式（默认）

连接器会自动将用户名密码编码为 Base64 并添加 `Basic ` 前缀。

```sql
CREATE TABLE http_basic_auth (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.header.Authorization' = 'username:password'
);
```

**说明**：
- 配置值 `username:password` 会被自动编码为 `Basic dXNlcm5hbWU6cGFzc3dvcmQ=`
- 如果值已经以 `Basic ` 开头，则原样使用

#### 原样透传模式

如果需要原样透传 Authorization header（如已编码的 Token）：

```sql
CREATE TABLE http_raw_auth (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.use-raw-authorization-header' = 'true',
  'gid.connector.http.scan.header.Authorization' = 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
);
```

---

### 9.2 OIDC Bearer Authentication

使用 OpenID Connect (OIDC) 获取 Bearer Token 进行认证。

```sql
CREATE TABLE http_oidc_auth (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.security.oidc.token.endpoint.url' = 'https://auth.example.com/oauth/token',
  'gid.connector.http.security.oidc.token.request' = 'grant_type=client_credentials&client_id=myclient&client_secret=mysecret'
);
```

**参数说明**：

| 参数 | 说明 |
|------|------|
| `oidc.token.endpoint.url` | OIDC Token Endpoint URL |
| `oidc.token.request` | Token Request Body（`application/x-www-form-urlencoded` 格式） |
| `oidc.token.expiry.reduction` | Token 过期提前刷新时间（默认 `PT1S`） |

**注意**：
- Token 会被自动缓存和刷新
- 获取的 Token 会自动添加到 `Authorization: Bearer <token>` header

---

### 9.3 TLS/mTLS

#### HTTPS（使用默认证书）

对于使用公共 CA 签发的 HTTPS 端点，无需额外配置：

```sql
CREATE TABLE http_https (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',  -- 使用 https 协议
  'format'    = 'json'
);
```

#### 自定义服务器证书

对于使用自签名证书或私有 CA 的端点：

```sql
CREATE TABLE http_custom_cert (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://internal.example.com/data',
  'format'    = 'json',
  'gid.connector.http.security.cert.server' = '/path/to/server-cert.pem'
);
```

**说明**：
- 支持 PEM 和 DER 格式
- 多个证书路径用逗号分隔

#### mTLS（双向认证）

```sql
CREATE TABLE http_mtls (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://secure.example.com/data',
  'format'    = 'json',
  'gid.connector.http.security.cert.server' = '/path/to/ca-cert.pem',
  'gid.connector.http.security.cert.client' = '/path/to/client-cert.pem',
  'gid.connector.http.security.key.client'  = '/path/to/client-key.pem'
);
```

**注意**：
- 客户端私钥必须是 PKCS8 格式
- 支持 PEM 和 DER 格式

#### 允许自签名证书（仅开发/测试环境）

```sql
CREATE TABLE http_self_signed (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://self-signed.example.com/data',
  'format'    = 'json',
  'gid.connector.http.security.cert.server.allowSelfSigned' = 'true'
);
```

**警告**：此选项禁用证书验证，仅用于开发/测试环境，生产环境请勿使用。

---

## 10. 重试策略

连接器支持两种重试策略，用于处理临时性错误（如网络抖动、服务暂时不可用）。

### 重试策略类型

| 类型 | 说明 | 默认值 |
|------|------|--------|
| `fixed-delay` | 固定延迟重试 | ✅ 默认 |
| `exponential-delay` | 指数退避重试 | — |

### fixed-delay（固定延迟）

每次重试之间等待固定时间。

```sql
CREATE TABLE http_fixed_retry (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.retry-strategy.type' = 'fixed-delay',
  'gid.connector.http.scan.retry-strategy.fixed-delay.delay' = 'PT2S',
  'gid.connector.http.scan.max-retries' = '5'
);
```

**参数说明**：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `retry-strategy.type` | 重试策略类型 | `fixed-delay` |
| `retry-strategy.fixed-delay.delay` | 重试间隔 | `PT1S`（1秒） |
| `max-retries` | 最大重试次数 | `3` |

### exponential-delay（指数退避）

每次重试的延迟时间按指数增长。

```sql
CREATE TABLE http_exp_retry (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  'gid.connector.http.scan.retry-strategy.type' = 'exponential-delay',
  'gid.connector.http.scan.retry-strategy.exponential-delay.initial-backoff' = 'PT1S',
  'gid.connector.http.scan.retry-strategy.exponential-delay.max-backoff' = 'PT1M',
  'gid.connector.http.scan.retry-strategy.exponential-delay.backoff-multiplier' = '2.0',
  'gid.connector.http.scan.max-retries' = '5'
);
```

**参数说明**：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `retry-strategy.type` | 重试策略类型 | `exponential-delay` |
| `retry-strategy.exponential-delay.initial-backoff` | 初始延迟 | `PT1S`（1秒） |
| `retry-strategy.exponential-delay.max-backoff` | 最大延迟 | `PT1M`（1分钟） |
| `retry-strategy.exponential-delay.backoff-multiplier` | 退避乘数 | `1.5` |
| `max-retries` | 最大重试次数 | `3` |

**延迟计算公式**：
```
delay = initial-backoff * (backoff-multiplier ^ retry_count)
```
但不超过 `max-backoff`。

### 禁用重试

```sql
'gid.connector.http.scan.max-retries' = '0'
```

---

## 11. 错误处理

### 错误类型与处理方式

| 错误类型 | 处理方式 |
|----------|----------|
| `success-codes` 命中 | 正常处理 |
| `retry-codes` 命中 | 自动重试，耗尽后作业失败 |
| `ignored-response-codes` 命中 | 跳过内容，仍推进分页 |
| 未分类的状态码 | 抛异常，作业失败 |
| `IOException`（网络错误） | 自动重试，耗尽后作业失败 |
| 反序列化失败 | 依赖 `json.ignore-parse-errors` 等 Format 选项 |

### 错误处理示例

```sql
CREATE TABLE http_error_handling (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'http-scan',
  'url'       = 'https://api.example.com/data',
  'format'    = 'json',
  -- 成功状态码
  'gid.connector.http.scan.success-codes' = '2XX',
  -- 可重试状态码
  'gid.connector.http.scan.retry-codes' = '5XX,!501',
  -- 忽略的状态码（跳过内容但继续分页）
  'gid.connector.http.scan.ignored-response-codes' = '404',
  -- 忽略 JSON 解析错误
  'json.ignore-parse-errors' = 'true'
);
```

**注意**：`http-scan` 不支持 `continue-on-error` 选项（与 `rest-lookup` 不同）。

---

## 12. 当前限制 (v1)

### 功能限制

1. **单并行度**：扫描由单个 Source Task 串行执行。并行度 > 1 时多余子任务会直接退出（不会重复拉取数据），但也不会提升吞吐，请将并行度设为 1
2. **不支持 Checkpoint 断点续传**：失败的作业从第一页重新开始（开启 checkpoint 的作业 failover 后同样从头重扫）
3. **不支持 `continue-on-error`**：未分类的 HTTP 错误会导致作业失败
4. **不支持 Metadata 列**：计划在后续版本支持

### 兼容性

- **Flink 版本**：1.17.x 和 1.18.x
- **Java 版本**：11+
- **运行模式**：推荐 BATCH 模式

### 性能考虑

- 单并行度可能成为大数据量场景的瓶颈
- 大量分页请求可能对 API 服务造成压力

---

## 13. 完整配置参数表

### 基础请求参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `connector` | STRING | ✅ | — | 固定值 `http-scan` |
| `url` | STRING | ✅ | — | HTTP 端点 URL |
| `format` | STRING | ✅ | — | Flink Format 名 |
| `method` | STRING | ❌ | `GET` | HTTP 方法 |
| `gid.connector.http.scan.url-vars` | STRING | ❌ | — | URL 路径变量 |
| `gid.connector.http.scan.query-params` | STRING | ❌ | — | Query 参数模板 |
| `gid.connector.http.scan.body` | STRING | ❌ | — | 请求体模板 |
| `gid.connector.http.scan.body-content-type` | STRING | ❌ | `application/json` | 请求体 Content-Type |
| `gid.connector.http.scan.content-field` | STRING | ❌ | — | JSONPath 内容提取 |

### 分页参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.scan.pagination.type` | STRING | ❌ | `none` | 分页类型 |
| `gid.connector.http.scan.pagination.page-field` | STRING | ❌ | `page` | 页码字段名 |
| `gid.connector.http.scan.pagination.start-page` | INT | ❌ | `1` | 起始页码 |
| `gid.connector.http.scan.pagination.batch-size` | INT | ❌ | — | 单页预期条数 |
| `gid.connector.http.scan.pagination.total-pages` | INT | ❌ | — | 硬上限总页数 |
| `gid.connector.http.scan.pagination.max-requests` | INT | ❌ | `10000` | 分页请求硬上限安全阀 |
| `gid.connector.http.scan.pagination.total-count-jsonpath` | STRING | ❌ | — | 总数字段 JSONPath |
| `gid.connector.http.scan.pagination.has-more-jsonpath` | STRING | ❌ | — | has-more 字段 JSONPath |
| `gid.connector.http.scan.pagination.cursor-field` | STRING | ❌ | `cursor` | 游标字段名 |
| `gid.connector.http.scan.pagination.cursor-response-jsonpath` | STRING | ❌ | — | 游标响应 JSONPath |
| `gid.connector.http.scan.pagination.initial-cursor` | STRING | ❌ | 空 | 初始游标值 |

### HTTP 传输参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.scan.request.timeout` | INT | ❌ | `30` | 请求超时（秒） |
| `gid.connector.http.scan.http-version` | STRING | ❌ | — | HTTP 版本 |
| `gid.connector.http.scan.max-retries` | INT | ❌ | `3` | 最大重试次数 |
| `gid.connector.http.scan.retry-strategy.type` | STRING | ❌ | `fixed-delay` | 重试策略类型 |
| `gid.connector.http.scan.retry-strategy.fixed-delay.delay` | DURATION | ❌ | `PT1S` | 固定延迟间隔 |
| `gid.connector.http.scan.retry-strategy.exponential-delay.initial-backoff` | DURATION | ❌ | `PT1S` | 指数退避初始延迟 |
| `gid.connector.http.scan.retry-strategy.exponential-delay.max-backoff` | DURATION | ❌ | `PT1M` | 指数退避最大延迟 |
| `gid.connector.http.scan.retry-strategy.exponential-delay.backoff-multiplier` | DOUBLE | ❌ | `1.5` | 退避乘数 |

### 状态码参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.scan.success-codes` | STRING | ❌ | `2XX` | 成功状态码 |
| `gid.connector.http.scan.retry-codes` | STRING | ❌ | — | 可重试状态码 |
| `gid.connector.http.scan.ignored-response-codes` | STRING | ❌ | — | 忽略响应状态码 |

### 连接与代理参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.scan.connection.timeout` | DURATION | ❌ | — | 连接超时 |
| `gid.connector.http.scan.proxy.host` | STRING | ❌ | — | 代理主机 |
| `gid.connector.http.scan.proxy.port` | INT | ❌ | — | 代理端口 |
| `gid.connector.http.scan.proxy.username` | STRING | ❌ | — | 代理用户名 |
| `gid.connector.http.scan.proxy.password` | STRING | ❌ | — | 代理密码 |

### 安全与认证参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.security.cert.server` | STRING | ❌ | — | 服务器证书路径 |
| `gid.connector.http.security.cert.client` | STRING | ❌ | — | 客户端证书路径 |
| `gid.connector.http.security.key.client` | STRING | ❌ | — | 客户端私钥路径 |
| `gid.connector.http.security.cert.server.allowSelfSigned` | STRING | ❌ | — | 允许自签名证书 |
| `gid.connector.http.security.oidc.token.endpoint.url` | STRING | ❌ | — | OIDC Token Endpoint |
| `gid.connector.http.security.oidc.token.request` | STRING | ❌ | — | OIDC Token Request |
| `gid.connector.http.security.oidc.token.expiry.reduction` | DURATION | ❌ | `PT1S` | Token 过期提前时间 |
| `gid.connector.http.scan.use-raw-authorization-header` | BOOLEAN | ❌ | `false` | 原样透传 Authorization |
| `gid.connector.http.scan.header.<NAME>` | STRING | ❌ | — | 自定义请求头 |

### 日志参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `gid.connector.http.logging.level` | STRING | ❌ | `MIN` | 日志级别 |

### Format 透传参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `json.ignore-parse-errors` | BOOLEAN | ❌ | `false` | 忽略 JSON 解析错误 |
| `json.fail-on-missing-field` | BOOLEAN | ❌ | `true` | 缺失字段时报错 |
| 其他 `<format>.*` 参数 | — | ❌ | — | 由 Format 层定义 |

---

## 附录

### 参考资料

- [上游项目 getindata/flink-http-connector](https://github.com/getindata/flink-http-connector)（`rest-lookup` 与 `http-sink` 的原始实现，本仓库在其基础上新增 `http-scan`）
- [Flink FLIP-27 Source API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Source+Split+API)
- [Flink JSON Format](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/)
- [设计文档](design/http-scan-connector-design.md)

### 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| 0.27.0 | Unreleased | 新增 `http-scan` 连接器 |

---

**文档维护者**：tuxin  
**最后更新**：2026-09-04

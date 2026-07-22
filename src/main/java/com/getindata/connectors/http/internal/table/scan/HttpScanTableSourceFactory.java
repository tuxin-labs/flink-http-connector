package com.getindata.connectors.http.internal.table.scan;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.types.utils.DataTypeUtils.removeTimeAttribute;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.scan.request.PlaceholderResolver;
import com.getindata.connectors.http.internal.table.scan.response.JsonPathExtractor;
import com.getindata.connectors.http.internal.utils.ConfigUtils;

/**
 * http-scan 连接器的 {@link DynamicTableSourceFactory}，factoryIdentifier = "http-scan"。
 *
 * <p>负责发现并校验 Format、校验连接器配置（规则见设计文档 §4.2）、构造 {@link HttpScanConfig}
 * 与 {@link HttpScanDynamicTableSource}。
 */
@Slf4j
public class HttpScanTableSourceFactory implements DynamicTableSourceFactory {

    private static final Pattern PAGE_PLACEHOLDER = Pattern.compile("\\$\\{page}");
    private static final Pattern CURSOR_PLACEHOLDER = Pattern.compile("\\$\\{cursor}");
    private static final Pattern PATH_VAR_PATTERN = Pattern.compile("\\{(\\w+)}");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig readable = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validateExcept(
            "table.",
            HttpConnectorConfigConstants.GID_CONNECTOR_HTTP,
            FactoryUtil.FORMAT.key());

        validateHttpScanOptions(readable);

        Properties properties =
            ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());
        HttpScanConfig config = HttpScanConfig.from(readable, properties);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType physicalRowDataType = toRowDataType(resolvedSchema.getColumns());

        return new HttpScanDynamicTableSource(config, decodingFormat, physicalRowDataType);
    }

    @Override
    public String factoryIdentifier() {
        return "http-scan";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(HttpScanConnectorOptions.URL, FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
            HttpScanConnectorOptions.METHOD,
            HttpScanConnectorOptions.URL_VARS,
            HttpScanConnectorOptions.QUERY_PARAMS,
            HttpScanConnectorOptions.BODY,
            HttpScanConnectorOptions.BODY_CONTENT_TYPE,
            HttpScanConnectorOptions.CONTENT_FIELD,
            HttpScanConnectorOptions.PAGINATION_TYPE,
            HttpScanConnectorOptions.PAGINATION_PAGE_FIELD,
            HttpScanConnectorOptions.PAGINATION_START_PAGE,
            HttpScanConnectorOptions.PAGINATION_BATCH_SIZE,
            HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES,
            HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH,
            HttpScanConnectorOptions.PAGINATION_HAS_MORE_JSONPATH,
            HttpScanConnectorOptions.PAGINATION_CURSOR_FIELD,
            HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH,
            HttpScanConnectorOptions.PAGINATION_INITIAL_CURSOR,
            HttpScanConnectorOptions.REQUEST_TIMEOUT,
            HttpScanConnectorOptions.HTTP_VERSION,
            HttpScanConnectorOptions.RETRY_STRATEGY_TYPE,
            HttpScanConnectorOptions.RETRY_FIXED_DELAY,
            HttpScanConnectorOptions.RETRY_EXP_INITIAL_BACKOFF,
            HttpScanConnectorOptions.RETRY_EXP_MAX_BACKOFF,
            HttpScanConnectorOptions.RETRY_EXP_MULTIPLIER,
            HttpScanConnectorOptions.MAX_RETRIES,
            HttpScanConnectorOptions.SUCCESS_CODES,
            HttpScanConnectorOptions.RETRY_CODES,
            HttpScanConnectorOptions.IGNORED_RESPONSE_CODES,
            HttpScanConnectorOptions.CONNECTION_TIMEOUT,
            HttpScanConnectorOptions.PROXY_HOST,
            HttpScanConnectorOptions.PROXY_PORT,
            HttpScanConnectorOptions.PROXY_USERNAME,
            HttpScanConnectorOptions.PROXY_PASSWORD,
            HttpScanConnectorOptions.USE_RAW_AUTH_HEADER
        );
    }

    /**
     * 校验 http-scan 选项（设计文档 §4.2 规则 1-13）。url/format 必填由 requiredOptions 保证。
     */
    void validateHttpScanOptions(ReadableConfig options) {
        // 规则2: method ∈ GET/POST/PUT
        String method = options.get(HttpScanConnectorOptions.METHOD);
        if (!"GET".equalsIgnoreCase(method)
            && !"POST".equalsIgnoreCase(method)
            && !"PUT".equalsIgnoreCase(method)) {
            throw new IllegalArgumentException(
                "Unsupported HTTP method: " + method + ". Supported: GET, POST, PUT.");
        }

        // 规则3: GET 时禁止 body
        String body = options.get(HttpScanConnectorOptions.BODY);
        if (body != null && "GET".equalsIgnoreCase(method)) {
            throw new IllegalArgumentException(
                "HTTP GET method cannot have a body. Remove 'gid.connector.http.scan.body' "
                    + "or use POST/PUT.");
        }

        // 规则4: pagination.type ∈ none/page-number/cursor
        String paginationType = options.get(HttpScanConnectorOptions.PAGINATION_TYPE);
        if (!"none".equals(paginationType)
            && !"page-number".equals(paginationType)
            && !"cursor".equals(paginationType)) {
            throw new IllegalArgumentException(
                "Unsupported pagination.type: " + paginationType
                    + ". Supported: none, page-number, cursor.");
        }

        // 规则5: cursor 模式 cursor-response-jsonpath 必填
        if ("cursor".equals(paginationType)) {
            String cursorRespPath =
                options.get(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH);
            if (cursorRespPath == null || cursorRespPath.isBlank()) {
                throw new IllegalArgumentException(
                    "gid.connector.http.scan.pagination.cursor-response-jsonpath is required "
                        + "when pagination.type = cursor.");
            }
        }

        // 规则6/7/8: 占位符与分页类型一致性
        String combinedTemplate = joinNullable(
            options.get(HttpScanConnectorOptions.URL),
            options.get(HttpScanConnectorOptions.QUERY_PARAMS),
            options.get(HttpScanConnectorOptions.BODY));
        boolean hasPage = PAGE_PLACEHOLDER.matcher(combinedTemplate).find();
        boolean hasCursor = CURSOR_PLACEHOLDER.matcher(combinedTemplate).find();
        if ("page-number".equals(paginationType) && !hasPage) {
            throw new IllegalArgumentException(
                "pagination.type = page-number requires at least one ${page} placeholder "
                    + "in url/query-params/body.");
        }
        if ("cursor".equals(paginationType) && !hasCursor) {
            throw new IllegalArgumentException(
                "pagination.type = cursor requires at least one ${cursor} placeholder "
                    + "in url/query-params/body.");
        }
        if ("none".equals(paginationType) && (hasPage || hasCursor)) {
            throw new IllegalArgumentException(
                "pagination.type = none does not allow ${page} or ${cursor} placeholders "
                    + "in url/query-params/body.");
        }

        // 规则9: url-vars 与 URL 双向匹配
        validateUrlVars(
            options.get(HttpScanConnectorOptions.URL),
            options.get(HttpScanConnectorOptions.URL_VARS));

        // 规则10: JSONPath 编译期校验
        compileIfPresent(options.get(HttpScanConnectorOptions.CONTENT_FIELD), "content-field");
        compileIfPresent(options.get(HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH),
            "total-count-jsonpath");
        compileIfPresent(options.get(HttpScanConnectorOptions.PAGINATION_HAS_MORE_JSONPATH),
            "has-more-jsonpath");
        compileIfPresent(options.get(HttpScanConnectorOptions.PAGINATION_CURSOR_RESPONSE_JSONPATH),
            "cursor-response-jsonpath");

        // 规则11: 数值范围
        int startPage = options.get(HttpScanConnectorOptions.PAGINATION_START_PAGE);
        if (startPage < 0) {
            throw new IllegalArgumentException("pagination.start-page must be >= 0.");
        }
        validatePositiveIfPresent(options, HttpScanConnectorOptions.PAGINATION_BATCH_SIZE,
            "pagination.batch-size");
        validatePositiveIfPresent(options, HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES,
            "pagination.total-pages");
        int maxRetries = options.get(HttpScanConnectorOptions.MAX_RETRIES);
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max-retries must be >= 0.");
        }

        // 软校验 S1/S2
        if (!"none".equals(paginationType)) {
            boolean noStopStrategy =
                options.getOptional(HttpScanConnectorOptions.PAGINATION_BATCH_SIZE).isEmpty()
                    && options.getOptional(HttpScanConnectorOptions.PAGINATION_TOTAL_PAGES).isEmpty()
                    && options.get(HttpScanConnectorOptions.PAGINATION_TOTAL_COUNT_JSONPATH) == null
                    && options.get(HttpScanConnectorOptions.PAGINATION_HAS_MORE_JSONPATH) == null;
            if (noStopStrategy && !"cursor".equals(paginationType)) {
                log.warn("pagination.type={} but no stop strategy configured; "
                    + "may loop indefinitely.", paginationType);
            }
        }
        if (options.get(HttpScanConnectorOptions.CONTENT_FIELD) == null) {
            log.warn("content-field is not configured; ensure the API returns a top-level "
                + "array or object.");
        }
    }

    private void validateUrlVars(String url, String urlVarsRaw) {
        if (url == null) {
            return;
        }
        Set<String> urlPathVars = new HashSet<>();
        Matcher m = PATH_VAR_PATTERN.matcher(url);
        while (m.find()) {
            urlPathVars.add(m.group(1));
        }
        Map<String, String> urlVars = PlaceholderResolver.parseUrlVars(urlVarsRaw);
        for (String var : urlPathVars) {
            if (!urlVars.containsKey(var)) {
                throw new IllegalArgumentException(
                    "URL path variable {" + var + "} has no value in url-vars.");
            }
        }
        for (String var : urlVars.keySet()) {
            if (!urlPathVars.contains(var)) {
                throw new IllegalArgumentException(
                    "url-vars contains {" + var + "} but it is not used in url.");
            }
        }
    }

    private void compileIfPresent(String jsonPath, String fieldName) {
        if (jsonPath == null || jsonPath.isBlank()) {
            return;
        }
        try {
            JsonPathExtractor.compileCheck(jsonPath);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid JSONPath for " + fieldName + ": " + jsonPath, e);
        }
    }

    private void validatePositiveIfPresent(ReadableConfig options, ConfigOption<Integer> option,
                                           String fieldName) {
        options.getOptional(option).ifPresent(value -> {
            if (value <= 0) {
                throw new IllegalArgumentException(fieldName + " must be > 0.");
            }
        });
    }

    private static String joinNullable(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (p != null) {
                sb.append(p);
            }
        }
        return sb.toString();
    }

    private DataType toRowDataType(List<Column> columns) {
        return columns.stream()
            .filter(Column::isPhysical)
            .map(this::columnToField)
            .collect(Collectors.collectingAndThen(
                Collectors.toList(),
                list -> DataTypes.ROW(list.toArray(new Field[0]))))
            .notNull();
    }

    private Field columnToField(Column column) {
        return FIELD(column.getName(), removeTimeAttribute(column.getDataType()));
    }
}

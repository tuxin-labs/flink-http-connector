package com.getindata.connectors.http.internal.table.scan.source;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.status.HttpCodesParser;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ProxyConfig;

/**
 * 构造 http-scan 的 {@link HttpClientWithRetry}。
 *
 * <p>复用 {@link JavaNetHttpClientFactory#getSslContext} 处理 TLS/mTLS、
 * {@link ProxyConfig} 处理代理认证（与 lookup 一致）；复用 {@link HttpResponseChecker}
 * 做状态码分类。ignored-response-codes 折进 success 集合（与 lookup 一致），
 * 由 Reader 单独判断是否跳过内容。
 */
public final class HttpScanHttpClientFactory {

    private HttpScanHttpClientFactory() {
    }

    public static HttpClientWithRetry create(HttpScanConfig config) throws ConfigurationException {
        SSLContext sslContext = JavaNetHttpClientFactory.getSslContext(config.getProperties());
        HttpClient.Builder builder = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .sslContext(sslContext);

        ReadableConfig readable = config.getReadableConfig();
        readable.getOptional(HttpScanConnectorOptions.CONNECTION_TIMEOUT)
            .ifPresent(builder::connectTimeout);
        readable.getOptional(HttpScanConnectorOptions.HTTP_VERSION)
            .ifPresent(version -> builder.version(HttpClient.Version.valueOf(version)));

        Optional<String> host = readable.getOptional(HttpScanConnectorOptions.PROXY_HOST);
        Optional<Integer> port = readable.getOptional(HttpScanConnectorOptions.PROXY_PORT);
        if (host.isPresent() && port.isPresent()) {
            ProxyConfig proxyConfig = new ProxyConfig(
                host.get(),
                port.get(),
                readable.getOptional(HttpScanConnectorOptions.PROXY_USERNAME),
                readable.getOptional(HttpScanConnectorOptions.PROXY_PASSWORD));
            builder.proxy(ProxySelector.of(new InetSocketAddress(proxyConfig.getHost(), proxyConfig.getPort())));
            proxyConfig.getAuthenticator().ifPresent(builder::authenticator);
        }

        HttpClient javaClient = builder.build();

        Set<Integer> ignoredCodes = parseCodesSafe(readable.get(HttpScanConnectorOptions.IGNORED_RESPONSE_CODES));
        Set<Integer> retryCodes = parseCodesSafe(readable.get(HttpScanConnectorOptions.RETRY_CODES));
        Set<Integer> successCodes = new HashSet<>(parseCodesSafe(readable.get(HttpScanConnectorOptions.SUCCESS_CODES)));
        // ignored 折进 success，使 HttpClientWithRetry 不对 ignored 码抛异常
        successCodes.addAll(ignoredCodes);

        HttpResponseChecker checker = new HttpResponseChecker(successCodes, retryCodes);

        return HttpClientWithRetry.builder()
            .httpClient(javaClient)
            .retryConfig(HttpScanRetryConfigProvider.create(readable))
            .responseChecker(checker)
            .build();
    }

    private static Set<Integer> parseCodesSafe(String expr) throws ConfigurationException {
        if (expr == null || expr.isBlank()) {
            return Set.of();
        }
        return HttpCodesParser.parse(expr);
    }
}

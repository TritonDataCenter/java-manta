/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import com.joyent.http.signature.apache.httpclient.HttpSignatureConfigurator;
import com.joyent.http.signature.apache.httpclient.HttpSignatureRequestInterceptor;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.ConfigurationException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultBackoffStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.impl.io.DefaultHttpResponseParserFactory;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Factory class that creates instances of
 * {@link org.apache.http.client.HttpClient} configured for use with
 * HTTP signature based authentication.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaConnectionFactory implements Closeable {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaConnectionFactory.class);

    /**
     * Default DNS resolver for all connections to the Manta.
     */
    private static final DnsResolver DNS_RESOLVER = new ShufflingDnsResolver();

    /**
     * Default HTTP headers to send to all requests to Manta.
     */
    private static final Collection<? extends Header> HEADERS = Arrays.asList(
            new BasicHeader(MantaHttpHeaders.ACCEPT_VERSION, "~1.0"),
            new BasicHeader(HttpHeaders.ACCEPT, "application/json, */*")
    );

    /**
     * Configuration context that provides connection details.
     */
    private final ConfigContext config;

    /**
     * HTTP Signatures authentication configuration helper.
     */
    private final HttpSignatureConfigurator signatureConfigurator;

    /**
     * Apache HTTP Client connection builder helper.
     */
    private final HttpClientBuilder httpClientBuilder;

    /**
     * Connection manager instance that is associated with a single Manta client.
     */
    private final HttpClientConnectionManager connectionManager;

    /**
     * Create new instance using the passed configuration.
     * @param config configuration of the connection parameters
     * @param keyPair cryptographic signing key pair used for HTTP signatures
     */
    public MantaConnectionFactory(final ConfigContext config,
                                  final KeyPair keyPair) {
        Validate.notNull(config, "Configuration context must not be null");

        this.config = config;

        // Setup configurator helper

        final boolean useNativeCodeToSign;

        if (config.disableNativeSignatures() == null) {
            useNativeCodeToSign = true;
        } else {
            useNativeCodeToSign = !config.disableNativeSignatures();
        }

        if (config.noAuth()) {
            this.signatureConfigurator = null;
        } else {
            this.signatureConfigurator = new HttpSignatureConfigurator(
                    keyPair,
                    createCredentials(),
                    useNativeCodeToSign);
        }

        this.connectionManager = buildConnectionManager();
        this.httpClientBuilder = createBuilder();
    }

    /**
     * Builds and configures a default connection factory instance.
     *
     * @return configured connection factory
     */
    protected HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection>
            buildHttpConnectionFactory() {
        return new ManagedHttpClientConnectionFactory(
                new DefaultHttpRequestWriterFactory(),
                new DefaultHttpResponseParserFactory());
    }

    /**
     * Configures a connection manager with all of the setting needed to connect
     * to Manta.
     *
     * @return fully configured connection manager
     */
    protected HttpClientConnectionManager buildConnectionManager() {
        final int maxConns = ObjectUtils.firstNonNull(
                config.getMaximumConnections(),
                DefaultsConfigContext.DEFAULT_MAX_CONNS);

        final ConnectionSocketFactory sslConnectionSocketFactory =
                new MantaSSLConnectionSocketFactory(this.config);

        final RegistryBuilder<ConnectionSocketFactory> registryBuilder =
                RegistryBuilder.create();

        final Registry<ConnectionSocketFactory> socketFactoryRegistry = registryBuilder
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslConnectionSocketFactory)
                .build();

        HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory =
                buildHttpConnectionFactory();

        final PoolingHttpClientConnectionManager poolingConnectionManager =
                new PoolingHttpClientConnectionManager(socketFactoryRegistry,
                        connFactory,
                        DNS_RESOLVER);
        poolingConnectionManager.setDefaultMaxPerRoute(maxConns);

        return poolingConnectionManager;
    }

    /**
     * Configures the builder class with all of the settings needed to connect to
     * Manta.
     *
     * @return configured instance
     */
    protected HttpClientBuilder createBuilder() {
        final boolean noAuth = ObjectUtils.firstNonNull(config.noAuth(), false);

        final int maxConns = ObjectUtils.firstNonNull(
                config.getMaximumConnections(),
                DefaultsConfigContext.DEFAULT_MAX_CONNS);

        final int timeout = ObjectUtils.firstNonNull(
                config.getTimeout(),
                DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);

        final long requestTimeout = Duration.ofSeconds(1L).toMillis();

        final RequestConfig requestConfig = RequestConfig.custom()
                .setAuthenticationEnabled(false)
                .setSocketTimeout(timeout)
                .setConnectionRequestTimeout((int)requestTimeout)
                .setContentCompressionEnabled(true)
                .build();

        final HttpClientBuilder builder = HttpClients.custom()
                .disableAuthCaching()
                .disableCookieManagement()
                .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
                .setMaxConnTotal(maxConns)
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                .setDefaultHeaders(HEADERS)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManagerShared(false)
                .setConnectionBackoffStrategy(new DefaultBackoffStrategy());

        if (config.getRetries() > 0) {
            builder.setRetryHandler(new MantaHttpRequestRetryHandler(config))
                   .setServiceUnavailableRetryStrategy(new MantaServiceUnavailableRetryStrategy(config));
        } else {
            LOGGER.info("Retry of failed requests is disabled");
        }

        final HttpHost proxyHost = findProxyServer();

        if (proxyHost != null) {
            builder.setProxy(proxyHost);
        }

        builder.addInterceptorFirst(new RequestIdInterceptor());
        builder.setConnectionManager(this.connectionManager);

        if (!noAuth) {
            @SuppressWarnings("unchecked")
            HttpSignatureAuthScheme authScheme =
                    (HttpSignatureAuthScheme)this.signatureConfigurator.getAuthScheme();

            builder.addInterceptorLast(new HttpSignatureRequestInterceptor(
                    authScheme,
                    this.createCredentials(),
                    !this.config.noAuth()));

        }

        return builder;
    }

    /**
     * Creates a {@link Credentials} instance based on the stored
     * {@link ConfigContext}.
     *
     * @return credentials for Manta
     */
    protected Credentials createCredentials() {
        final String user = config.getMantaUser();
        Validate.notNull(user, "User must not be null");

        final String keyId = config.getMantaKeyId();
        Validate.notNull(keyId, "Key id must not be null");

        return new UsernamePasswordCredentials(user, keyId);
    }

    /**
     * Derives Manta URI for a given path.
     *
     * @param path full path
     * @return full URI as string of resource
     */
    protected String uriForPath(final String path) {
        Validate.notNull(path, "Path must not be null");

        if (path.startsWith("/")) {
            return String.format("%s%s", config.getMantaURL(), path);
        } else {
            return String.format("%s/%s", config.getMantaURL(), path);
        }
    }

    /**
     * Derives Manta URI for a given path with the passed query
     * parameters.
     *
     * @param path full path
     * @param params query parameters to add to the URI
     * @return full URI as string of resource
     */
    protected String uriForPath(final String path, final List<NameValuePair> params) {
        Validate.notNull(path, "Path must not be null");
        Validate.notNull(params, "Params must not be null");

        try {
            final URIBuilder uriBuilder = new URIBuilder(uriForPath(path));
            uriBuilder.addParameters(params);
            return uriBuilder.build().toString();
        } catch (URISyntaxException e) {
            throw new ConfigurationException(String.format("Invalid path in URI: %s", path));
        }
    }

    /**
     * Finds the host of the proxy server that was configured as part of the
     * JVM settings.
     *
     * @return proxy server as {@link HttpHost}, if no proxy then null
     */
    protected HttpHost findProxyServer() {
        final ProxySelector proxySelector = ProxySelector.getDefault();
        List<Proxy> proxies = proxySelector.select(URI.create(config.getMantaURL()));

        if (!proxies.isEmpty()) {
            /* The Apache HTTP Client doesn't understand the concept of multiple
             * proxies, so we use only the first one returned. */
            final Proxy proxy = proxies.get(0);

            switch (proxy.type()) {
                case DIRECT:
                    return null;
                case SOCKS:
                    throw new ConfigurationException("SOCKS proxies are unsupported");
                default:
                    // do nothing and fall through
            }

            if (proxy.address() instanceof InetSocketAddress) {
                InetSocketAddress sa = (InetSocketAddress) proxy.address();

                return new HttpHost(sa.getHostName(), sa.getPort());
            } else {
                String msg = String.format(
                        "Expecting proxy to be instance of InetSocketAddress. "
                        + " Actually: %s", proxy.address());
                throw new ConfigurationException(msg);
            }
        } else {
            return null;
        }
    }

    /**
     * Creates a new configured instance of {@link CloseableHttpClient} based
     * on the factory's configuration.
     *
     * @return new connection object instance
     */
    public CloseableHttpClient createConnection() {
        return httpClientBuilder.build();
    }

    /**
     * Convenience method used for building DELETE operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpDelete delete(final String path) {
        return new HttpDelete(uriForPath(path));
    }

    /**
     * Convenience method used for building DELETE operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpDelete delete(final String path, final List<NameValuePair> params) {
        return new HttpDelete(uriForPath(path, params));
    }

    /**
     * Convenience method used for building GET operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpGet get(final String path) {
        return new HttpGet(uriForPath(path));
    }

    /**
     * Convenience method used for building GET operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpGet get(final String path, final List<NameValuePair> params) {
        return new HttpGet(uriForPath(path, params));
    }

    /**
     * Convenience method used for building HEAD operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpHead head(final String path) {
        return new HttpHead(uriForPath(path));
    }

    /**
     * Convenience method used for building HEAD operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpHead head(final String path, final List<NameValuePair> params) {
        return new HttpHead(uriForPath(path, params));
    }

    /**
     * Convenience method used for building POST operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPost post(final String path) {
        return new HttpPost(uriForPath(path));
    }

    /**
     * Convenience method used for building POST operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPost post(final String path, final List<NameValuePair> params) {
        return new HttpPost(uriForPath(path, params));
    }

    /**
     * Convenience method used for building PUT operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPut put(final String path) {
        return new HttpPut(uriForPath(path));
    }

    /**
     * Convenience method used for building PUT operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPut put(final String path, final List<NameValuePair> params) {
        return new HttpPut(uriForPath(path, params));
    }

    @Override
    public void close() throws IOException {
        if (this.connectionManager != null) {
            connectionManager.shutdown();
        }
    }
}

/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import com.joyent.http.signature.apache.httpclient.HttpSignatureConfigurator;
import com.joyent.http.signature.apache.httpclient.HttpSignatureRequestInterceptor;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.ConfigContextMBean;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.ConfigurationException;
import com.joyent.manta.util.MantaVersion;
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
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.DnsResolver;
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

import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
     * A running count of the times we have created new {@link MantaConnectionFactory}
     * instances.
     */
    private static final AtomicInteger CONNECTION_FACTORY_COUNT = new AtomicInteger(0);

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
    private final PoolingHttpClientConnectionManager connectionManager;

    /**
     * Weak reference (because we don't want this object to own it) to signer
     * thread local container.
     */
    private final WeakReference<ThreadLocalSigner> signerThreadLocalRef;

    /**
     * List of all MBeans to be added to JMX.
     */
    private final Map<ObjectName, DynamicMBean> jmxDynamicBeans;

    /**
     * Create new instance using the passed configuration.
     * @param config configuration of the connection parameters
     * @param keyPair cryptographic signing key pair used for HTTP signatures
     * @param signer Signer configured to work with the the given keyPair
     */
    public MantaConnectionFactory(final ConfigContext config,
                                  final KeyPair keyPair,
                                  final ThreadLocalSigner signer) {
        Validate.notNull(config, "Configuration context must not be null");

        CONNECTION_FACTORY_COUNT.incrementAndGet();

        this.config = config;

        // Setup configurator helper

        final boolean useNativeCodeToSign;

        useNativeCodeToSign = config.disableNativeSignatures() == null
                || !config.disableNativeSignatures();

        final HttpSignatureAuthScheme authScheme;

        // If we have auth disabled, then we don't assign any signer classes
        if (config.noAuth()) {
            this.signatureConfigurator = null;
            authScheme = null;
            this.signerThreadLocalRef = new WeakReference<>(null);
        // When auth is enabled we assign a configurator that sets up signing
        } else {
            this.signatureConfigurator = new HttpSignatureConfigurator(
                    keyPair,
                    createCredentials(),
                    signer);
            this.signerThreadLocalRef = new WeakReference<>(signer);
            authScheme = (HttpSignatureAuthScheme) this.signatureConfigurator.getAuthScheme();
        }

        this.connectionManager = buildConnectionManager();

        this.httpClientBuilder = createBuilder(authScheme);
        this.jmxDynamicBeans = buildMBeans();

        registerMBeans();
    }

    /**
     * Registers the beans stored in <code>this.jmxDynamicBeans</code> so
     * that they can be exposed via JMX.
     */
    protected void registerMBeans() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        Set<Map.Entry<ObjectName, DynamicMBean>> beans = this.jmxDynamicBeans.entrySet();

        for (Map.Entry<ObjectName, DynamicMBean> bean : beans) {
            try {
                server.registerMBean(bean.getValue(), bean.getKey());
            } catch (JMException e) {
                String msg = String.format("Error registering [%s] MBean in JMX",
                        bean.getKey());
                LOGGER.warn(msg, e);
            }
        }
    }

    /**
     * Unregisters the beans stored in <code>this.jmxDynamicBeans</code> so
     * that they are no longer visible via JMX.
     */
    protected void unregisterMBeans() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        Set<Map.Entry<ObjectName, DynamicMBean>> beans = this.jmxDynamicBeans.entrySet();

        for (Map.Entry<ObjectName, DynamicMBean> bean : beans) {
            try {
                server.unregisterMBean(bean.getKey());
            } catch (JMException e) {
                String msg = String.format("Error registering [%s] MBean in JMX",
                        bean.getKey());
                LOGGER.warn(msg, e);
            }
        }
    }

    /**
     * Builds the MBeans used to expose data to JMX.
     * @return populated Map of beans
     */
    protected Map<ObjectName, DynamicMBean> buildMBeans() {
        Map<ObjectName, DynamicMBean> beans = new HashMap<>();

        try {
            String poolStatsObjectName = String.format(
                    "com.joyent.manta.client:type=PoolStatsMBean[%d]",
                    CONNECTION_FACTORY_COUNT.get());
            ObjectName poolStatsName = new ObjectName(poolStatsObjectName);
            beans.put(poolStatsName, new PoolStatsMBean(this.connectionManager));
        } catch (JMException e) {
            LOGGER.warn("Error creating PoolStatsMBean", e);
        }

        try {
            String configObjectName = String.format(
                    "com.joyent.manta.client:type=ConfigMBean[%d]",
                    CONNECTION_FACTORY_COUNT.get());
            ObjectName configName = new ObjectName(configObjectName);
            beans.put(configName, new ConfigContextMBean(this.config));
        } catch (JMException e) {
            LOGGER.warn("Error creating ConfigMBean", e);
        }

        // If we had any errors, we just return no mbeans
        return Collections.unmodifiableMap(beans);
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
     * Builds a socket configuration customized for Manta.
     *
     * @return fully configured instance
     */
    protected SocketConfig buildSocketConfig() {
        final int socketTimeout = ObjectUtils.firstNonNull(
                config.getTcpSocketTimeout(),
                DefaultsConfigContext.DEFAULT_TCP_SOCKET_TIMEOUT);

        return SocketConfig.custom()
                /* Disable Nagle's algorithm for this connection.  Written data
                 * to the network is not buffered pending acknowledgement of
                 * previously written data.
                 */
                .setTcpNoDelay(true)
                /* Set a timeout on blocking Socket operations. */
                .setSoTimeout(socketTimeout)
                .setSoKeepAlive(true)
                .build();
    }

    /**
     * Builds and configures a {@link ConnectionConfig} instance.
     * @return fully configured instance
     */
    protected ConnectionConfig buildConnectionConfig() {
        final int bufferSize = ObjectUtils.firstNonNull(
                config.getHttpBufferSize(),
                DefaultsConfigContext.DEFAULT_HTTP_BUFFER_SIZE);

        return ConnectionConfig.custom()
                .setBufferSize(bufferSize)
                .build();
    }

    /**
     * Configures a connection manager with all of the setting needed to connect
     * to Manta.
     *
     * @return fully configured connection manager
     */
    protected PoolingHttpClientConnectionManager buildConnectionManager() {
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
        poolingConnectionManager.setMaxTotal(maxConns);
        poolingConnectionManager.setDefaultSocketConfig(buildSocketConfig());
        poolingConnectionManager.setDefaultConnectionConfig(buildConnectionConfig());

        return poolingConnectionManager;
    }

    /**
     * Configures the builder class with all of the settings needed to connect to
     * Manta.
     *
     * @param authScheme authentication scheme to use (null if noAuth is enabled)
     * @return configured instance
     */
    protected HttpClientBuilder createBuilder(final HttpSignatureAuthScheme authScheme) {
        final boolean noAuth = ObjectUtils.firstNonNull(config.noAuth(), false);

        final int maxConns = ObjectUtils.firstNonNull(
                config.getMaximumConnections(),
                DefaultsConfigContext.DEFAULT_MAX_CONNS);

        final int timeout = ObjectUtils.firstNonNull(
                config.getTimeout(),
                DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);

        final long requestTimeout = Duration.ofSeconds(1L).toMillis();

        final String userAgent = String.format(
                "Java-Manta-SDK/%s (Java/%s/%s)",
                MantaVersion.VERSION,
                System.getProperty("java.version"),
                System.getProperty("java.vendor"));

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
                .setConnectionBackoffStrategy(new DefaultBackoffStrategy())
                .setUserAgent(userAgent);

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

        if (!noAuth && authScheme != null) {
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

        return new UsernamePasswordCredentials(user, null);
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

        /* We clear all thread local instances of the signer class so that
         * there are no dangling thread-local variables when the connection
         * factory is closed (typically when MantaClient is closed).
         */
        ThreadLocalSigner signerThreadLocal = this.signerThreadLocalRef.get();
        if (signerThreadLocal != null) {
            signerThreadLocal.clearAll();
        }

        unregisterMBeans();
    }
}

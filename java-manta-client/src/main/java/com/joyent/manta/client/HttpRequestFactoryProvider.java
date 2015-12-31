/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.joyent.http.signature.google.httpclient.HttpSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.ProxySelectorRoutePlanner;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.net.ProxySelector;

import static com.joyent.http.signature.HttpSignerUtils.X_REQUEST_ID_HEADER;

/**
 * Provider class that provides a configured implementation of
 * {@link HttpRequestFactory} and the underlying {@link HttpClient}
 * that backs it.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class HttpRequestFactoryProvider implements AutoCloseable {
    /**
     * The static logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestFactoryProvider.class);

    /**
     * The size of the internal socket buffer used to buffer data
     * while receiving / transmitting HTTP messages.
     */
    private static final int SOCKET_BUFFER_SIZE = 8192;

    /**
     * Default port to connect to for HTTP connections outbound.
     */
    private static final int HTTP_PORT = 80;

    /**
     * Default port to connect to for HTTPS connections outbound.
     */
    private static final int HTTPS_PORT = 443;

    /**
     * The JSON factory instance used by the http library for handling JSON.
     */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /**
     * Apache HTTP Client 4.1 method of configuration HTTP Clients.
     */
    private static final HttpParams HTTP_PARAMS = buildHttpParams();

    /**
     * Google HTTP Client request factory.
     */
    private final HttpRequestFactory requestFactory;

    /**
     * Library configuration context reference.
     */
    private final ConfigContext config;

    /**
     * Creates a new instance of class configured using the passed
     * {@link HttpSigner}.
     *
     * @param httpSigner HTTP Signer used to sign Google HTTP requests
     * @param config library configuration context reference
     * @throws IOException thrown when the instance can't be setup properly
     */
    public HttpRequestFactoryProvider(final HttpSigner httpSigner,
                                      final ConfigContext config)
            throws IOException {
        this.config = config;
        this.requestFactory = buildRequestFactory(httpSigner);
    }

    /**
     * Creates the parameters used to configure the Apache HTTP Client.
     *
     * @return Configuration parameters object
     */
    private static HttpParams buildHttpParams() {
        final HttpParams params = new BasicHttpParams();
        // Turn off stale checking. Our connections break all the time anyway,
        // and it's not worth it to pay the penalty of checking every time.
        HttpConnectionParams.setStaleCheckingEnabled(params, false);
        HttpConnectionParams.setSocketBufferSize(params, SOCKET_BUFFER_SIZE);
        HttpConnectionParams.setTcpNoDelay(params, true);

        return params;
    }

    /**
     * Creates and configures an Apache HTTP Client {@link HttpClient}.
     *
     * @return a configured instance of {@link HttpClient}
     */
    private HttpClient buildHttpClient() {
        final HttpParams params = HTTP_PARAMS;
        final SSLSocketFactory socketFactory = SSLSocketFactory.getSystemSocketFactory();
        final PlainSocketFactory plainSocketFactory = PlainSocketFactory.getSocketFactory();
        final ProxySelector proxySelector = ProxySelector.getDefault();



        // See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html
        final SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", HTTP_PORT, plainSocketFactory));
        registry.register(new Scheme("https", HTTPS_PORT, socketFactory));

        final ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager();

        final int maxConns;
        if (config.getMaximumConnections() == null) {
            maxConns = DefaultsConfigContext.DEFAULT_MAX_CONNS;
        } else {
            maxConns = config.getMaximumConnections();
        }

        connectionManager.setMaxTotal(maxConns);
        connectionManager.setDefaultMaxPerRoute(maxConns);

        final DefaultHttpClient defaultHttpClient = new DefaultHttpClient(connectionManager, params);

        final int httpRetries;

        if (config.getRetries() == null) {
            httpRetries = DefaultsConfigContext.DEFAULT_HTTP_RETRIES;
        } else {
            httpRetries = config.getRetries();
        }

        defaultHttpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(httpRetries, false));

        if (proxySelector != null) {
            defaultHttpClient.setRoutePlanner(new ProxySelectorRoutePlanner(registry, proxySelector));
        }

        return defaultHttpClient;
    }

    /**
     * Builds a configured instance of {@link HttpRequestFactory}.
     *
     * @param httpSigner HTTP Signer used to sign Google HTTP requests
     * @return configured instance of {@link HttpRequestFactory}
     * @throws IOException thrown when the instance can't be setup properly
     */
    private HttpRequestFactory buildRequestFactory(final HttpSigner httpSigner)
            throws IOException {
        final HttpTransport transport;

        /* We only allow three choices for HttpTransport because we shade the
         * Google HTTP Client libraries, so even if you stick in another
         * library, you will have to make it comply with a munged classpath.
         */
        switch (config.getHttpTransport()) {
            case "MockHttpTransport":
                transport = new MockHttpTransport();
                break;
            case "NetHttpTransport":
                transport = new NetHttpTransport();
                break;
            case "ApacheHttpTransport":
                transport = new ApacheHttpTransport(buildHttpClient());
                break;
            default:
                transport = new ApacheHttpTransport(buildHttpClient());
        }

        LOG.debug("Using HttpTransport implementation: {}", transport.getClass());

        final HttpExecuteInterceptor signingInterceptor = request -> {
            // Set timeouts
            final int httpTimeout;

            if (config.getTimeout() == null) {
                httpTimeout = DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT;
            } else {
                httpTimeout = config.getTimeout();
            }

            request.setReadTimeout(httpTimeout);
            request.setConnectTimeout(httpTimeout);
            request.setLoggingEnabled(LOG.isDebugEnabled());

            // Sign request
            httpSigner.signRequest(request);
            // Load request ID into MDC so that it can be logged
            final Object requestId = request.getHeaders().get(X_REQUEST_ID_HEADER);
            if (requestId != null) {
                MDC.put("mantaRequestId", requestId.toString());
            }
        };

        final HttpResponseInterceptor responseInterceptor = response -> MDC.remove("mantaRequestId");

        final HttpRequestInitializer initializer = request -> {
            request.setInterceptor(signingInterceptor);
            request.setResponseInterceptor(responseInterceptor);
            request.setParser(new JsonObjectParser(JSON_FACTORY));
        };

        return transport.createRequestFactory(initializer);
    }


    /**
     * @return configured instance of {@link HttpRequestFactory}
     */
    public HttpRequestFactory getRequestFactory() {
        return requestFactory;
    }

    @Override
    public void close() throws Exception {
        final HttpTransport transport = requestFactory.getTransport();

        if (config.getHttpTransport().equals("ApacheHttpTransport")) {
            // We know this will cast fine because it is configured as such
            @SuppressWarnings("unchecked")
            ApacheHttpTransport apacheTransport = (ApacheHttpTransport)transport;
            HttpClient httpClient = apacheTransport.getHttpClient();

            if (httpClient.getConnectionManager() != null) {
                httpClient.getConnectionManager().shutdown();
            }
        }
    }
}

package com.joyent.manta.client;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.joyent.http.signature.google.httpclient.HttpSigner;
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

import java.io.IOException;
import java.net.ProxySelector;

/**
 * @author Elijah Zupancic
 * @since 1.0.0
 */
public class HttpRequestFactoryProvider implements AutoCloseable {
    /**
     * The JSON factory instance used by the http library for handling JSON.
     */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final HttpParams HTTP_PARAMS = buildHttpParams();

    private final HttpClient httpClient;
    private final HttpRequestFactory requestFactory;

    public HttpRequestFactoryProvider(final HttpSigner httpSigner)
            throws IOException {
        this.httpClient = buildHttpClient();
        this.requestFactory = buildRequestFactory(httpSigner, httpClient);
    }

    private static HttpParams buildHttpParams() {
        final HttpParams params = new BasicHttpParams();
        // Turn off stale checking. Our connections break all the time anyway,
        // and it's not worth it to pay the penalty of checking every time.
        HttpConnectionParams.setStaleCheckingEnabled(params, false);
        HttpConnectionParams.setSocketBufferSize(params, 8192);
        HttpConnectionParams.setTcpNoDelay(params, true);

        return params;
    }

    private static HttpClient buildHttpClient() {
        final HttpParams params = HTTP_PARAMS;
        final SSLSocketFactory socketFactory = SSLSocketFactory.getSocketFactory();
        final ProxySelector proxySelector = ProxySelector.getDefault();

        // See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html
        final SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        registry.register(new Scheme("https", 443, socketFactory));

        final ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager();
        connectionManager.setMaxTotal(20);
        connectionManager.setDefaultMaxPerRoute(200);

        final DefaultHttpClient defaultHttpClient = new DefaultHttpClient(connectionManager, params);
        defaultHttpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0, false));

        if (proxySelector != null) {
            defaultHttpClient.setRoutePlanner(new ProxySelectorRoutePlanner(registry, proxySelector));
        }

        return defaultHttpClient;
    }

    private static HttpRequestFactory buildRequestFactory(final HttpSigner httpSigner,
                                                         final HttpClient httpClient)
            throws IOException {
        final HttpTransport transport = new ApacheHttpTransport(httpClient);
        final HttpRequestInitializer initializer = new HttpRequestInitializer() {
            @Override
            public void initialize(final HttpRequest request) throws IOException {
                request.setInterceptor(new HttpExecuteInterceptor() {
                    @Override
                    public void intercept(HttpRequest request) throws IOException {
                        httpSigner.signRequest(request);
                    }
                });
                request.setParser(new JsonObjectParser(JSON_FACTORY));
            }
        };

        // TODO: Call shutdown method
        return transport.createRequestFactory(initializer);
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public HttpRequestFactory getRequestFactory() {
        return requestFactory;
    }

    @Override
    public void close() throws Exception {
        if (httpClient.getConnectionManager() != null) {
            httpClient.getConnectionManager().shutdown();
        }
    }
}

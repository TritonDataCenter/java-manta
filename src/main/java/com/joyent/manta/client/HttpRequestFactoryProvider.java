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
public class HttpRequestFactoryProvider {
    /**
     * The JSON factory instance used by the http library for handling JSON.
     */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    public static HttpRequestFactory newDefaultHttpClient(final HttpSigner httpSigner)
            throws IOException {
        SSLSocketFactory socketFactory = SSLSocketFactory.getSocketFactory();
        ProxySelector proxySelector = ProxySelector.getDefault();
        HttpParams params = new BasicHttpParams();
        // Turn off stale checking. Our connections break all the time anyway,
        // and it's not worth it to pay the penalty of checking every time.
        HttpConnectionParams.setStaleCheckingEnabled(params, false);
        HttpConnectionParams.setSocketBufferSize(params, 8192);

        // See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html
        SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        registry.register(new Scheme("https", 443, socketFactory));
        ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager(registry);
        connectionManager.setMaxTotal(20);
        connectionManager.setDefaultMaxPerRoute(200);
        DefaultHttpClient defaultHttpClient = new DefaultHttpClient(connectionManager, params);
        defaultHttpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0, false));

        if (proxySelector != null) {
            defaultHttpClient.setRoutePlanner(new ProxySelectorRoutePlanner(registry, proxySelector));
        }

        final HttpTransport transport = new ApacheHttpTransport(defaultHttpClient);
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
}

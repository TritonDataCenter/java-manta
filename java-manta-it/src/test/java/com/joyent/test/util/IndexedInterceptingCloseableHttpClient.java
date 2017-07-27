package com.joyent.test.util;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tomascelaya on 7/26/17.
 */
public class IndexedInterceptingCloseableHttpClient extends CloseableHttpClient {

    private final CloseableHttpClient wrapped;

    private final AtomicInteger requestCount = new AtomicInteger(0);

    private final HashMap<Integer, CloseableHttpResponse> cannedResponses = new HashMap<>();

    public IndexedInterceptingCloseableHttpClient(final CloseableHttpClient wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    protected CloseableHttpResponse doExecute(HttpHost httpHost, HttpRequest httpRequest, HttpContext httpContext)
            throws IOException, ClientProtocolException {

        final int requestIdx = requestCount.getAndIncrement();

        if (cannedResponses.containsKey(requestIdx)) {
            return cannedResponses.get(requestIdx);
        }

        return wrapped.execute(httpHost, httpRequest, httpContext);
    }

    public synchronized void setResponse(int requestIdx, CloseableHttpResponse response) {
        cannedResponses.put(requestIdx, response);
    }

    public synchronized void clearResponses() {
        cannedResponses.clear();
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    @Deprecated
    @Override
    public HttpParams getParams() {
        return wrapped.getParams();
    }

    @Deprecated
    @Override
    public ClientConnectionManager getConnectionManager() {
        return wrapped.getConnectionManager();
    }
}

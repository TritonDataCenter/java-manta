package com.joyent.manta.http;

import org.apache.http.HttpClientConnection;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FakeHttpClientConnectionManager implements HttpClientConnectionManager {

    private final ConnectionRequest connReq;

    public FakeHttpClientConnectionManager(final HttpClientConnection conn) {
        this.connReq = new PassthroughConnectionRequest(conn);
    }

    @Override
    public ConnectionRequest requestConnection(final HttpRoute route,
                                               final Object state) {
        return connReq;
    }

    @Override
    public void releaseConnection(final HttpClientConnection conn,
                                  final Object newState,
                                  final long validDuration,
                                  final TimeUnit timeUnit) {
    }

    @Override
    public void connect(final HttpClientConnection conn,
                        final HttpRoute route,
                        final int connectTimeout,
                        final HttpContext context) throws IOException {
    }

    @Override
    public void upgrade(final HttpClientConnection conn,
                        final HttpRoute route,
                        final HttpContext context) throws IOException {
    }

    @Override
    public void routeComplete(final HttpClientConnection conn,
                              final HttpRoute route,
                              final HttpContext context) throws IOException {
    }

    @Override
    public void closeIdleConnections(final long idletime,
                                     final TimeUnit tunit) {
    }

    @Override
    public void closeExpiredConnections() {
    }

    @Override
    public void shutdown() {
    }

    private static class PassthroughConnectionRequest implements ConnectionRequest {

        private final HttpClientConnection conn;

        PassthroughConnectionRequest(final HttpClientConnection conn) {
            this.conn = conn;
        }

        @Override
        public HttpClientConnection get(final long timeout,
                                        final TimeUnit tunit) {
            return this.conn;
        }

        @Override
        public boolean cancel() {
            return false;
        }
    }
}


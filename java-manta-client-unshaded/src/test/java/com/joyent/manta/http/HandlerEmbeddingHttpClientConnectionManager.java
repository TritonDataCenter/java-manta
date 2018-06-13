/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpClientConnection;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.Validate.notNull;

public class HandlerEmbeddingHttpClientConnectionManager implements HttpClientConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(HandlerEmbeddingHttpClientConnectionManager.class);

    private final EntityPopulatingHttpRequestHandler requestHandler;

    public HandlerEmbeddingHttpClientConnectionManager(final EntityPopulatingHttpRequestHandler requestHandler) {
        this.requestHandler = notNull(requestHandler);
    }

    @Override
    public ConnectionRequest requestConnection(final HttpRoute route,
                                               final Object state) {
        LOG.trace("Connection requested");
        return new BasicEmbeddedHandlerConnectionRequest(this.requestHandler);
    }

    @Override
    public void releaseConnection(final HttpClientConnection conn,
                                  final Object newState,
                                  final long validDuration,
                                  final TimeUnit timeUnit) {
        if (!(conn instanceof EmbeddedRequestHandlerClientConnection)) {
            LOG.warn("Unexpected connection type, attempting to close.");
            IOUtils.closeQuietly(conn);
            return;
        }

        LOG.trace("Releasing connection: " + ((EmbeddedRequestHandlerClientConnection) conn).getId());
    }

    @Override
    public void connect(final HttpClientConnection conn,
                        final HttpRoute route,
                        final int connectTimeout,
                        final HttpContext context) throws IOException {
        LOG.trace("Connecting connection: " + conn);
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

    /**
     * "Basic" because it returns a new connection every time, e.g. it is ignorant of Keep-Alive.
     */
    private static class BasicEmbeddedHandlerConnectionRequest implements ConnectionRequest {

        private final EntityPopulatingHttpRequestHandler requestHandler;

        BasicEmbeddedHandlerConnectionRequest(final EntityPopulatingHttpRequestHandler requestHandler) {
            this.requestHandler = requestHandler;
        }

        @Override
        public HttpClientConnection get(final long timeout,
                                        final TimeUnit tunit)
                throws InterruptedException, ExecutionException, ConnectionPoolTimeoutException {
            return new EmbeddedRequestHandlerClientConnection(this.requestHandler);
        }

        @Override
        public boolean cancel() {
            return false;
        }
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


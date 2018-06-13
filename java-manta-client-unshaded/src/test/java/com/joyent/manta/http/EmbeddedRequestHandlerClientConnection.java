/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.Header;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.message.BasicHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.CLOSED;
import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.READY;
import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.RECV_RES_BODY;
import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.RECV_RES_HEADER;
import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.SEND_REQ_BODY;
import static com.joyent.manta.http.EmbeddedRequestHandlerClientConnection.ConnectionState.SEND_REQ_HEADER;
import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;
import static org.mockito.Mockito.mock;

class EmbeddedRequestHandlerClientConnection implements HttpClientConnection {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedRequestHandlerClientConnection.class);

    private static final AtomicLong COUNTER = new AtomicLong(0);

    private final String id;

    private final EntityPopulatingHttpRequestHandler requestHandler;

    private final HttpConnectionMetrics metrics;

    private final AtomicReference<ConnectionState> state;

    private boolean handled;

    private HttpResponse currentResponse;

    private final Object lock = new Object();

    enum ConnectionState {
        READY,
        SEND_REQ_HEADER,
        SEND_REQ_BODY,
        RECV_RES_HEADER,
        RECV_RES_BODY,
        CLOSED,
    }

    EmbeddedRequestHandlerClientConnection(final EntityPopulatingHttpRequestHandler requestHandler) {
        this.id = Long.toString(COUNTER.getAndIncrement());
        this.requestHandler = requestHandler;
        this.state = new AtomicReference<>(READY);
        this.handled = false;

        // don't care about this
        this.metrics = mock(HttpConnectionMetrics.class);
    }

    /**
     * Checks if response data is available from the connection.
     */
    @Override
    public boolean isResponseAvailable(final int timeout) {
        synchronized (this.lock) {
            return this.handled;
        }
    }

    /**
     * Sends the request line and all headers over the connection.
     */
    @Override
    public void sendRequestHeader(final HttpRequest request)
            throws IOException, HttpException {
        synchronized (this.lock) {
            this.validTransition(SEND_REQ_HEADER, READY);
            this.currentResponse = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_NOT_IMPLEMENTED, "Sorry");

            if (request instanceof HttpEntityEnclosingRequest) {
                // we'll call handle when the request entity is "sent" in #sendRequestEntity(HttpEntityEnclosingRequest)
                this.handled = false;
                return;
            }

            onRequestSubmitted(request);

            this.requestHandler.handle(request, this.currentResponse, null);
            this.handled = true;
        }
    }

    /**
     * Sends the request entity over the connection.
     */
    @Override
    public void sendRequestEntity(final HttpEntityEnclosingRequest request)
            throws IOException, HttpException {
        LOG.info(" >>> entity >>> " + request.getRequestLine());

        synchronized (this.lock) {
            this.validTransition(SEND_REQ_BODY, SEND_REQ_HEADER);
            // we don't actually send a request body (since we dont need that yet)

            if (this.handled) {
                throw new IOException("sendRequestEntity called but request was already handled");
            }

            this.requestHandler.handle(request, this.currentResponse, null);
            this.handled = true;
        }
    }

    /**
     * Receives the request line and headers of the next response available from
     * this connection. The caller should examine the HttpResponse object to
     * find out if it should try to receive a response entity as well.
     *
     * @return a new HttpResponse object with status line and headers
     * initialized.
     */
    @Override
    public HttpResponse receiveResponseHeader()
            throws IOException, HttpException {
        synchronized (this.lock) {
            if (this.currentResponse == null || !this.handled) {
                throw new IOException("Request not yet handled");
            }

            this.validTransition(RECV_RES_HEADER, SEND_REQ_HEADER, SEND_REQ_BODY);

            onResponseReceived(this.currentResponse);

            return this.currentResponse;
        }
    }

    /**
     * Receives the next response entity available from this connection and
     * attaches it to an existing HttpResponse object.
     */
    @Override
    public void receiveResponseEntity(final HttpResponse response)
            throws IOException, HttpException {
        synchronized (this.lock) {
            if (this.currentResponse == null || !this.handled) {
                throw new IOException("Request not yet handled");
            }

            this.validTransition(RECV_RES_BODY, RECV_RES_HEADER);

            this.requestHandler.populateEntity(response);
        }
    }

    private void validTransition(final ConnectionState target, final ConnectionState... expected) throws IOException {
        notNull(target);
        notEmpty(expected);

        final ConnectionState current = this.state.get();

        for (int i = 0; i < expected.length; i++) {
            if (current.equals(expected[i])) {
                this.state.compareAndSet(current, target);
                return;
            }
        }

        throw new IOException(
                String.format(
                        "Invalid connection state transition: expected [%s], was [%s]",
                        ArrayUtils.toString(expected, "null"),
                        current));
    }

    /**
     * Writes out all pending buffered data over the open connection.
     */
    @Override
    public void flush() {
    }

    /**
     * Closes this connection gracefully.
     * This method will attempt to flush the internal output
     * buffer prior to closing the underlying socket.
     */
    @Override
    public void close() throws IOException {
        synchronized (this.lock) {
            if (this.state.get() == CLOSED) {
                return;
            }

            try {
                this.validTransition(CLOSED, RECV_RES_BODY);
            } catch (final IOException e) {
                this.state.set(CLOSED);
                throw e;
            }
            this.currentResponse = null;
        }
    }

    @Override
    public boolean isOpen() {
        synchronized (this.lock) {
            return this.state.get() != CLOSED;
        }
    }

    @Override
    public boolean isStale() {
        return false;
    }

    @Override
    public void setSocketTimeout(final int timeout) {
    }

    /**
     * Returns the socket timeout value.
     *
     * @return 0 if timeout is disabled
     */
    @Override
    public int getSocketTimeout() {
        return 0;
    }

    /**
     * Force-closes this connection. May be called from any thread.
     */
    @Override
    public void shutdown() throws IOException {
        this.close();
    }

    @Override
    public HttpConnectionMetrics getMetrics() {
        return this.metrics;
    }

    String getId() {
        return this.id;
    }

    private void onResponseReceived(final HttpResponse response) {
        if (response != null) {
            LOG.trace(getId() + " << " + response.getStatusLine().toString());
            final Header[] headers = response.getAllHeaders();
            for (final Header header : headers) {
                LOG.trace(getId() + " << " + header.toString());
            }
        }
    }

    private void onRequestSubmitted(final HttpRequest request) {
        if (request != null) {
            LOG.trace(getId() + " >> " + request.getRequestLine().toString());
            final Header[] headers = request.getAllHeaders();
            for (final Header header : headers) {
                LOG.trace(getId() + " >> " + header.toString());
            }
        }
    }

}


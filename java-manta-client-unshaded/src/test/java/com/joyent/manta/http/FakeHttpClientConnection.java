package com.joyent.manta.http;


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
import org.apache.http.protocol.HttpRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class FakeHttpClientConnection implements HttpClientConnection {
    
    private static final Logger LOG = LoggerFactory.getLogger(FakeHttpClientConnection.class);

    private static final AtomicLong COUNTER = new AtomicLong(0);

    private final String id;

    private final HttpRequestHandler requestHandler;

    private final HttpConnectionMetrics metrics;

    private final AtomicReference<ConnectionState> state;

    private boolean handled;

    private HttpResponse currentResponse;

    private enum ConnectionState {
        READY,
        REQ_SENT_HEADER,
        REQ_SENT_BODY,
        RES_RECV_HEADER,
        RES_RECV_BODY,
    }

    FakeHttpClientConnection(final HttpRequestHandler requestHandler) {
        this.id = Long.toString(COUNTER.getAndIncrement());
        this.requestHandler = requestHandler;
        this.state = new AtomicReference<>(ConnectionState.READY);
        this.handled = false;

        // don't care about this
        this.metrics = mock(HttpConnectionMetrics.class);
    }

    /**
     * Checks if response data is available from the connection.
     */
    @Override
    public boolean isResponseAvailable(final int timeout) {
        return true;
    }

    /**
     * Sends the request line and all headers over the connection.
     */
    @Override
    public void sendRequestHeader(final HttpRequest request)
            throws IOException, HttpException {
        this.state.compareAndSet(ConnectionState.READY, ConnectionState.REQ_SENT_HEADER);
        this.currentResponse = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_NOT_IMPLEMENTED, "Sorry");

        if (request instanceof HttpEntityEnclosingRequest) {
            this.handled = false;
            return;
        }

        onRequestSubmitted(request);

        this.requestHandler.handle(request, this.currentResponse, null);
        this.handled = true;
    }

    /**
     * Sends the request entity over the connection.
     */
    @Override
    public void sendRequestEntity(final HttpEntityEnclosingRequest request)
            throws IOException, HttpException {
        LOG.info(" >>> entity >>> " + request.getRequestLine());

        this.state.compareAndSet(ConnectionState.REQ_SENT_HEADER, ConnectionState.REQ_SENT_BODY);
        // we don't actually send a request body (since we dont need that yet)

        if (this.handled) {
            throw new IllegalStateException("sendRequestEntity called but request was already handled");
        }

        this.requestHandler.handle(request, this.currentResponse, null);
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
    public HttpResponse receiveResponseHeader() {
        final ConnectionState currentState = this.state.get();

        if (!currentState.equals(ConnectionState.REQ_SENT_HEADER)
                && !currentState.equals(ConnectionState.REQ_SENT_BODY)) {
            throw new AssertionError(
                    "Invalid state transition during receiveResponseHeader, was: " + currentState);
        }

        onResponseReceived(this.currentResponse);

        this.state.compareAndSet(ConnectionState.REQ_SENT_HEADER, ConnectionState.REQ_SENT_BODY);

        return this.currentResponse;
    }

    /**
     * Receives the next response entity available from this connection and
     * attaches it to an existing HttpResponse object.
     */
    @Override
    public void receiveResponseEntity(final HttpResponse response) {
        LOG.debug(getId() + " << entity received " + this.currentResponse.getStatusLine());
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
    public void close() {

    }

    @Override
    public boolean isOpen() {
        return true;
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
    }

    @Override
    public HttpConnectionMetrics getMetrics() {
        return this.metrics;
    }

    private String getId() {
        return this.id;
    }

    private void onResponseReceived(final HttpResponse response) {
        if (response != null) {
            LOG.debug(getId() + " << " + response.getStatusLine().toString());
            final Header[] headers = response.getAllHeaders();
            for (final Header header : headers) {
                LOG.debug(getId() + " << " + header.toString());
            }
        }
    }

    private void onRequestSubmitted(final HttpRequest request) {
        if (request != null) {
            LOG.debug(getId() + " >> " + request.getRequestLine().toString());
            final Header[] headers = request.getAllHeaders();
            for (final Header header : headers) {
                LOG.debug(getId() + " >> " + header.toString());
            }
        }
    }

}


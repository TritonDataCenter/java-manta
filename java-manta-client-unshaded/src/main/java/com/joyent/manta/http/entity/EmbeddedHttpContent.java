/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that provides visibility into the {@link org.apache.http.HttpEntity}
 * object being put to the server. This allows us to proxy all of the
 * {@link OutputStream} API calls.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EmbeddedHttpContent implements HttpEntity {
    /**
     * Number of milliseconds to wait between checks to see if the stream has been closed.
     */
    private static final long CLOSED_CHECK_INTERVAL = 50L;

    /**
     * The actual output stream that is used by Apache HTTP Client.
     */
    private volatile OutputStream writer;

    /**
     * Content type of entity.
     */
    private final String contentType;

    /**
     * Flag indicating that writing object has been closed.
     */
    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     *
     * @param contentType content type of entity to be sent
     * @param closed reference to boolean indicating if the {@link OutputStream} has been closed
     */
    public EmbeddedHttpContent(final String contentType, final AtomicBoolean closed) {
        this.contentType = contentType;
        this.closed = closed;
    }

    @Override
    public synchronized void writeTo(final OutputStream out) throws IOException {
        writer = out;

            /* Loop while the parent OutputStream is still open. This allows us to write
             * to the stream from the parent class while keeping the stream open with
             * another thread. */
        while (!closed.get()) {
            try {
                this.wait(CLOSED_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                return; // exit loop and assume closed if interrupted
            }
        }
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return -1L;
    }

    @Override
    public Header getContentType() {
        return new BasicHeader(HttpHeaders.CONTENT_TYPE, contentType);
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException("getContent is not supported");
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    @Deprecated
    public void consumeContent() throws IOException {
        throw new UnsupportedOperationException("consumeContent is not supported");
    }

    public OutputStream getWriter() {
        return writer;
    }
}

/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * {@link InputStream} implementation that allows for the attaching and detaching of delegated (source) streams. This
 * allows classes which embed us to continue supplying data to their callers transparently regardless of whether the
 * data is coming from the original source {@link InputStream} or a continuation thereof.
 * <p>
 * Inpsired by {@link com.joyent.manta.client.multipart.MultipartOutputStream}.
 *
 * <strong>This class is not thread-safe.</strong>
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.3
 */
public class ContinuableInputStream extends InputStream {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuableInputStream.class);

    /**
     * EOF marker.
     */
    static final int EOF = -1;

    /**
     * We don't want to keep trying to read from the backing stream (or accept new continuations) if we've actually
     * seen EOF.
     */
    private boolean eofSeen;

    /**
     * Whether the stream has been closed yet. We don't want to continue delivering reads or accept new continuations
     * if this stream has been closed.
     */
    private boolean closed;

    /**
     * Total number of bytes read so far.
     */
    private long bytesRead;

    /**
     * The delegate stream. We replace this
     */
    private InputStream wrapped;

    /**
     * Construct a new stream that reads from {@code initial} and can handle continuations and the lifecycle of both.
     *
     * @param initial the stream from which to start reading
     */
    ContinuableInputStream(final InputStream initial) {
        this.eofSeen = false;
        this.closed = false;
        this.bytesRead = 0;
        this.wrapped = initial;
    }

    /**
     * Attaches the next stream.
     *
     * @param next stream to switch to as a backing stream
     */
    public void continueWith(final InputStream next) {
        notNull(next, "InputStream must not be null");

        if (this.eofSeen) {
            throw new IllegalStateException("Already reached end of source stream, refusing to set new source");
        }

        IOUtils.closeQuietly(this.wrapped);

        this.wrapped = next;
    }

    /**
     * Retrieve the total number of bytes read through this stream.
     *
     * @return total bytes read
     */
    public long getBytesRead() {
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();

        if (this.closed || this.eofSeen) {
            return EOF;
        }

        final int b = this.wrapped.read();
        if (b != EOF) {
            this.bytesRead += b;
        } else {
            this.eofSeen = true;
        }

        return b;
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        ensureOpen();

        if (this.closed || this.eofSeen) {
            return EOF;
        }

        final int n = this.wrapped.read(buffer);
        if (n != EOF) {
            this.bytesRead += n;
        } else {
            this.eofSeen = true;
        }

        return n;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        ensureOpen();

        if (this.closed || this.eofSeen) {
            return EOF;
        }

        final int n = this.wrapped.read(buffer, offset, length);
        if (n != EOF) {
            this.bytesRead += n;
        } else {
            this.eofSeen = true;
        }

        return n;
    }

    /**
     * Closes this stream and releases any system resources associated with it. If the stream is already closed then
     * invoking this method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised to relinquish the underlying resources and to
     * internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        if (this.wrapped != null) {
            wrapped.close();
        }

        this.closed = true;
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private void ensureOpen() {
        if (this.closed) {
            throw new IllegalStateException("Attempted to read from a closed InputStream");
        }
    }

}

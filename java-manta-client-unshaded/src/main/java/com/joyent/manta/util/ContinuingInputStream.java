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
import java.util.Objects;

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
public class ContinuingInputStream extends InputStream {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuingInputStream.class);

    /**
     * EOF marker.
     */
    private static final int EOF = -1;

    /**
     * We don't want to keep trying to read from the backing stream (or accept new continuations) if we've actually seen
     * EOF.
     */
    private boolean eofSeen;

    /**
     * Whether the stream has been closed yet. We don't want to continue delivering reads or accept new continuations if
     * this stream has been closed.
     */
    private boolean closed;

    /**
     * Total number of bytes read so far.
     */
    private long bytesRead;

    /**
     * The delegate stream. We replace this when the user calls {@link #continueWith(InputStream)}. When this field is
     * null we are in a "dirty" state and will not handle read calls.
     */
    private InputStream wrapped;

    /**
     * Construct a new stream that reads from {@code initial} and can handle continuations and the lifecycle of both.
     *
     * @param initial the stream from which to start reading
     */
    public ContinuingInputStream(final InputStream initial) {
        this.eofSeen = false;
        this.closed = false;
        this.bytesRead = 0;
        this.wrapped = Objects.requireNonNull(initial);
    }

    protected InputStream getWrapped() {
        return this.wrapped;
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

        if (this.closed) {
            throw new IllegalStateException("Stream is closed, refusing to set new source");
        }

        // QUESTION: is the following useful? it signals that the caller has probably done something silly
        // if (this.wrapped == next) {
        //     throw new IllegalArgumentException("Current and next stream are the same");
        // }

        this.wrapped = next;
    }

    /**
     * Retrieve the total number of bytes read through this stream.
     *
     * @return total bytes read
     */
    public long getBytesRead() {
        return this.bytesRead;
    }

    @Override
    public int read() throws IOException {
        ensureReady();

        if (this.eofSeen) {
            return EOF;
        }

        try {
            final int b = this.wrapped.read();

            if (b != EOF) {
                this.bytesRead += b;
            } else {
                this.eofSeen = true;
            }

            return b;
        } catch (final IOException ioe) {
            discardWrapped();
            throw ioe;
        }
    }

    @Override
    public int read(final byte[] b) throws IOException {
        ensureReady();

        if (this.eofSeen) {
            return EOF;
        }

        try {
            final int n = this.wrapped.read(b);

            if (n != EOF) {
                this.bytesRead += n;
            } else {
                this.eofSeen = true;
            }

            return n;
        } catch (final IOException ioe) {
            discardWrapped();
            throw ioe;
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        ensureReady();

        if (this.eofSeen) {
            return EOF;
        }

        try {
            final int n = this.wrapped.read(b, off, len);

            if (n != EOF) {
                this.bytesRead += n;
            } else {
                this.eofSeen = true;
            }

            return n;
        } catch (final IOException ioe) {
            discardWrapped();
            throw ioe;
        }
    }

    @Override
    public long skip(final long n) throws IOException {
        ensureReady();

        try {
            final long s = this.wrapped.skip(n);

            if (0 < s) {
                this.bytesRead += s;
            }

            return s;
        } catch (final IOException ioe) {
            discardWrapped();
            throw ioe;
        }
    }

    @Override
    public int available() throws IOException {
        ensureReady();

        try {
            return this.wrapped.available();
        } catch (final IOException ioe) {
            discardWrapped();
            throw ioe;
        }
    }

    @Override
    public void mark(final int readlimit) {
        throw new UnsupportedOperationException("ContinuingInputStream does not support mark/reset");
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("ContinuingInputStream does not support mark/reset");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        this.closed = true;

        if (this.wrapped != null) {
            wrapped.close();
        }
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private void ensureReady() {
        if (this.closed) {
            throw new IllegalStateException("Attempted to read from a closed InputStream");
        }

        if (this.wrapped == null) {
            throw new IllegalStateException("Read called before setting a continuation stream");
        }
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private void discardWrapped() {
        IOUtils.closeQuietly(this.wrapped);
        this.wrapped = null;
    }

}

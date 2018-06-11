/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * {@link OutputStream} implementation that allows for the attaching and
 * detaching of delegated (source) streams. This is done so that we can
 * preserve the state of a {@link org.bouncycastle.jcajce.io.CipherOutputStream} instance
 * while switching the backing stream (in this case a stream that allows
 * writing to the HTTP socket).
 *
 * <p><strong>This class is not thread-safe.</strong></p>
 */
@NotThreadSafe
public class ResumableInputStream extends InputStream {

    /**
     * EOF marker.
     */
    static final int EOF = -1;

    /**
     * Default buffer size (same as manta.http_buffer_size).
     */
    private static final int BUFFER_SIZE_DEFAULT = 4 * 1024;

    /**
     * Backing stream.
     */
    private CountingInputStream source = null;

    /**
     * Boolean indicating if the stream is closed and
     * should stop accepting calls to {@link #setSource(InputStream)} and {@code read} methods.
     */
    private volatile boolean closed = false;

    /**
     * Buffer.
     */
    private final byte[] buffer;

    /**
     * Index of next byte to be read.
     */
    private int bufPos;

    /**
     * Number of valid bytes.
     */
    private int bufCount;

    /**
     * Total count of bytes read across all source streams.
     */
    private int count;

    /**
     * Creates a new instance with the default buffer size.
     */
    public ResumableInputStream() {
        this(BUFFER_SIZE_DEFAULT);
    }

    /**
     * Creates a new instance with the specified buffer size.
     *
     * @param bufferSize cipher block size
     */
    public ResumableInputStream(final int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("Buffer size must be greater than zero");
        }
        this.buffer = new byte[bufferSize];
        this.bufPos = 0;
        this.count = 0;
    }

    /**
     * Attaches the next stream.
     *
     * @param next stream to switch to as a backing stream
     */
    public ResumableInputStream setSource(final InputStream next) {
        if (this.closed) {
            throw new IllegalStateException("Attempted to set source on a closed ResumableInputStream");
        }
        notNull(next, "InputStream must not be null");

        if (this.bufCount == EOF) {
            throw new IllegalStateException("Already reached end of source stream, refusing to set new source");
        }

        if (this.source != null) {
            IOUtils.closeQuietly(this.source);
        }

        this.source = new CountingInputStream(next);
        return this;
    }

    @Override
    public int read() throws IOException {
        if (this.closed) {
            throw new IllegalStateException("Attempted to read from a closed ResumableInputStream");
        }

        attemptToReadAhead();

        if (this.bufCount == EOF) {
            return EOF;
        }

        final int byteReturned = this.buffer[this.bufPos];

        count += 1;
        bufPos += 1;

        return byteReturned;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (this.closed) {
            throw new IllegalStateException("Attempted to read from a closed ResumableInputStream");
        }

        notNull(b, "Provided byte array must not be null");

        if (off < 0 || len < 0 || len > b.length - off) {
            final String message = String.format(
                    "Invalid parameters to read(byte[], int, int): buffersize=%d, offset=%d, length=%d",
                    b.length,
                    off,
                    len);
            throw new IndexOutOfBoundsException(message);
        }

        attemptToReadAhead();

        if (this.bufCount == EOF) {
            return EOF;
        }

        int remaining = len;
        int pos = off;
        int read = 0;

        while (0 < remaining && this.bufPos < this.bufCount) {
            final int copied = Math.min(remaining, this.bufCount - this.bufPos);
            System.arraycopy(buffer, bufPos, b, pos, copied);
            pos += copied;
            bufPos += copied;
            read += copied;

            if (copied < remaining) {
                fillBuffer();
            }

            remaining -= copied;
        }

        this.count += read;
        return read;
    }

    @Override
    public int available() throws IOException {
        attemptToReadAhead();
        return this.bufCount - this.bufPos;
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

        if (this.source != null) {
            source.close();
        }

        this.closed = true;
    }

    public int getCount() {
        return this.count;
    }

    /**
     * Make sure we're ready to provide bytes.
     * TODO: It might make sense to provide as input the desired number of bytes?
     *
     * @throws IOException when {@link #fillBuffer()} throws
     */
    private void attemptToReadAhead() throws IOException {
        if (this.bufCount == EOF) {
            return;
        }

        if (this.count == 0 || this.bufPos == this.bufCount) {
            fillBuffer();
        }
    }

    /**
     * Populate our buffer with data from {@link #source}.
     *
     * @throws IOException when we fail to read from the source stream
     */
    private void fillBuffer() throws IOException {
        // we only reset bufPos if we successfully read
        this.bufCount = this.source.read(this.buffer);
        this.bufPos = 0;
    }
}

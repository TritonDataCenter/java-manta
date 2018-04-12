/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.io.input.CountingInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * {@link OutputStream} implementation that allows for the attaching and
 * detaching of delegated (wrapped) streams. This is done so that we can
 * preserve the state of a {@link org.bouncycastle.jcajce.io.CipherOutputStream} instance
 * while switching the backing stream (in this case a stream that allows
 * writing to the HTTP socket).
 *
 * <p><strong>This class is not thread-safe.</strong></p>
 */
@NotThreadSafe
public class MultipartInputStream extends InputStream {

    /**
     * EOF marker.
     */
    private static final int EOF = -1;

    /**
     * Backing stream.
     */
    private CountingInputStream wrapped = null;

    /**
     * Buffering stream.
     */
    private final byte[] buffer;

    /**
     * Number of valid bytes in {@link this.buffer}.
     */
    private int bufIndex;

    /**
     * Total count of bytes read across all wrapped streams.
     */
    private int count;

    /**
     * Creates a new instance with the specified block size.
     *
     * @param blockSize cipher block size
     */
    public MultipartInputStream(final int blockSize) {
        this.buffer = new byte[blockSize];
        this.bufIndex = 0;
        this.count = 0;
    }

    /**
     * Attaches the next stream.
     * @param next stream to switch to as a backing stream
     */
    public void setNext(final InputStream next) {
        this.wrapped = new CountingInputStream(next);
    }

    @Override
    public int read() throws IOException {
        if (bufIndex == buffer.length) {
            fill();
        }

        final int byteReturned = this.buffer[this.bufIndex];

        count += 1;
        bufIndex += 1;

        return byteReturned;
    }

    @Override
    public int read(final byte[] b) throws IOException {
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
    }

    @Override
    public long skip(final long n) throws IOException {
        return super.skip(n);
    }

    @Override
    public int available() throws IOException {
        return super.available();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}

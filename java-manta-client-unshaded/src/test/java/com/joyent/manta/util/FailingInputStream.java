/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import static com.joyent.manta.util.FailingInputStream.FailureOrder.ON_EOF;
import static com.joyent.manta.util.FailingInputStream.FailureOrder.POST_READ;
import static com.joyent.manta.util.FailingInputStream.FailureOrder.PRE_READ;
import static java.util.Objects.requireNonNull;

public class FailingInputStream extends InputStream {

    public enum FailureOrder {
        PRE_READ,
        POST_READ,
        ON_EOF,
    }

    private static final Logger LOG = LoggerFactory.getLogger(FailingInputStream.class);

    /**
     * End of file magic number.
     */
    private static final int EOF = -1;

    /**
     * The wrapped InputStream
     */
    private final InputStream wrapped;

    /**
     * Current read byte count.
     */
    private final AtomicLong count = new AtomicLong(0L);

    /**
     * Minimum number of bytes to read before failing.
     */
    private final long minimumBytes;

    private final FailureOrder failureOrder;

    /**
     * @param wrapped InputStream to wrap
     * @param failureOrder when to trigger the failure
     * @param minimumBytes number of bytes to read successfully before failing, ignored if failureOrder is ON_EOF
     */
    public FailingInputStream(final InputStream wrapped, final FailureOrder failureOrder, final long minimumBytes) {
        this.wrapped = requireNonNull(wrapped);
        this.failureOrder = requireNonNull(failureOrder);
        this.minimumBytes = minimumBytes;
    }

    @Override
    public int read() throws IOException {
        preReadFailure(1);

        // it's a byte, even though the return is int
        final int nextByte = this.wrapped.read();

        if (nextByte == EOF) {
            eofReadFailure();

            return EOF;
        }

        this.count.incrementAndGet();

        // byte is lost!
        postReadFailure();

        return nextByte;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        preReadFailure(len);

        final int bytesRead = wrapped.read(b, off, len);

        if (bytesRead == EOF) {
            eofReadFailure();

            return EOF;
        }

        count.addAndGet(bytesRead);

        // bytes were read into buffer but the user won't know how many
        postReadFailure();

        return bytesRead;
    }

    private void preReadFailure(final int willRead) throws IOException {
        if (PRE_READ != (this.failureOrder)) {
            return;
        }

        failIfEnoughBytesRead(willRead);
    }

    private void postReadFailure() throws IOException {
        if (POST_READ != (this.failureOrder)) {
            return;
        }

        failIfEnoughBytesRead(0);
    }

    private void eofReadFailure() throws IOException {
        if (ON_EOF != this.failureOrder) {
            return;
        }

        failIfEnoughBytesRead(EOF);
    }

    private void failIfEnoughBytesRead(final int next) throws IOException {
        if (next == EOF && ON_EOF.equals(this.failureOrder)) {
            throw new IOException("Read failure on EOF");
        }

        final boolean failureByteReached = this.minimumBytes <= this.count.get() + next;

        if (failureByteReached && this.failureOrder == PRE_READ) {
            throw new IOException("Read failure before byte " + this.minimumBytes);
        }

        if (failureByteReached && this.failureOrder == POST_READ) {
            throw new IOException("Read failure after byte " + this.minimumBytes);
        }
    }
}

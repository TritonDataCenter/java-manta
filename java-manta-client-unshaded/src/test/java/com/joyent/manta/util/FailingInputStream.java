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

public class FailingInputStream extends InputStream {

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

    private final boolean failAfterRead;

    /**
     * @param wrapped      InputStream to wrap
     * @param minimumBytes number of bytes to read successfully before failing
     */
    public FailingInputStream(InputStream wrapped, long minimumBytes, boolean failAfterRead) {
        this.wrapped = wrapped;
        this.minimumBytes = minimumBytes;
        this.failAfterRead = failAfterRead;
        LOG.debug("failing stream, bytes {} post? {}", minimumBytes, failAfterRead);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        preReadFailure(len);

        final int bytesRead = wrapped.read(b, off, len);

        if (bytesRead == EOF) {
            return EOF;
        }

        count.addAndGet(bytesRead);

        // bytes were read into buffer but the user won't know how many
        postReadFailure();

        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        preReadFailure(1);

        // it's a byte, even though the return is int
        int nextByte = this.wrapped.read();
        if (nextByte == EOF) {
            return EOF;
        }

        this.count.incrementAndGet();

        // byte is lost!
        postReadFailure();

        return nextByte;
    }

    private void preReadFailure(final int next) throws IOException {
        if (!this.failAfterRead) {
            failIfEnoughBytesRead(next, false);
        }
    }

    private void postReadFailure() throws IOException {
        if (this.failAfterRead) {
            failIfEnoughBytesRead(0, true);
        }
    }

    private void failIfEnoughBytesRead(final int next, final boolean isAfterRead) throws IOException {
        IOException e = null;
        if (count.get() + next >= minimumBytes) {
            final String relative = isAfterRead ? "after reading" : "attempting to read up to";
            e = new IOException("Read failure " + relative + " byte " + minimumBytes);
            LOG.debug("should fail {} because {} + {} >= {}", relative, count.get(), next, minimumBytes);
        }

        if (e != null) {
            throw e;
        }
    }
}

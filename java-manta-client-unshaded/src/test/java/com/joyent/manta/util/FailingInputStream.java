/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

public class FailingInputStream extends InputStream {
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
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        preReadFailure(len);

        int bytesRead = wrapped.read(b, off, len);

        count.addAndGet(bytesRead);

        // bytes were read, but the user won't know how many
        postReadFailure();

        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        preReadFailure(1);

        // it's a byte, even though the return is int
        int nextByte = this.wrapped.read();
        if (nextByte != EOF) {
            this.count.incrementAndGet();
        }

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
        if (count.get() + next >= minimumBytes) {
            final String relative = isAfterRead ? "after reading" : "attempting to read up to";
            throw new IOException("Read failure " + relative + " byte " + minimumBytes);
        }
    }
}

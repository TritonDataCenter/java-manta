/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

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

    /**
     *
     * @param wrapped InputStream to wrap
     * @param minimumBytes number of bytes to read successfully before failing
     */
    public FailingInputStream(InputStream wrapped, int minimumBytes) {
        this.wrapped = wrapped;
        this.minimumBytes = minimumBytes;
    }

    @Override
    public int read(byte[] b) throws IOException {
        failAfterMinimum(b.length);
        int bytesRead = wrapped.read(b);
        count.addAndGet(bytesRead);
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        failAfterMinimum(len);
        int bytesRead = wrapped.read(b, off, len);
        count.addAndGet(bytesRead);
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        failAfterMinimum(1);
        // it's a byte, even though the return is int
        int nextByte = wrapped.read();
        if (nextByte != EOF) {
            count.incrementAndGet();
        }
        return nextByte;
    }

    private void failAfterMinimum(final int next) throws SpuriousIOException {
        if (count.get() + next > minimumBytes) {
            throw new SpuriousIOException("Read failure after byte " + minimumBytes);
        }
    }
}

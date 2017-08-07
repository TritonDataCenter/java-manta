/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class FailingOutputStream extends OutputStream {

    public static final int NO_FAILURE = -1;

    /**
     * The wrapped InputStream
     */
    private final OutputStream wrapped;

    /**
     * Current read byte count.
     */
    private final AtomicLong count = new AtomicLong(0L);

    /**
     * Minimum number of bytes to read before failing.
     */
    private long minimumBytes;

    public FailingOutputStream(final OutputStream wrapped, int minimumBytes) {
        this.wrapped = wrapped;
        this.minimumBytes = minimumBytes;
    }

    @Override
    public void write(int b) throws IOException {
        failAfterMinimum(1);
        count.incrementAndGet();
        wrapped.write(b);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        failAfterMinimum(bytes.length);
        count.addAndGet(bytes.length);
        wrapped.write(bytes, off, len);
    }

    public void setMinimumBytes(final int minimumBytes) {
        this.minimumBytes = minimumBytes;
    }

    private void failAfterMinimum(final int next) throws IOException {
        if (minimumBytes == NO_FAILURE) {
            return;
        }

        if (count.get() + next > minimumBytes) {
            throw new IOException("Write failure after byte " + minimumBytes);
        }
    }
}

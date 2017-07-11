/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link java.io.InputStream} implementation that will always read one byte
 * less than the length specified in a {@link InputStream#read(byte[], int, int)}
 * method invocation.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.1
 */
public class IncompleteByteReadInputStream extends InputStream {
    private final InputStream wrapped;

    public IncompleteByteReadInputStream(final InputStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public int read() throws IOException {
        return wrapped.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len > 1 && len - off - 1 > 0) {
            return wrapped.read(b, off, len - 1);
        } else {
            return wrapped.read(b, off, len);
        }
    }
}

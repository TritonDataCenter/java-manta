/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.macs.HMac;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} implementation that progressively updates a
 * HMAC as data is written.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HmacOutputStream extends OutputStream {
    /**
     * HMAC instance used to generate HMAC for wrapped stream.
     */
    private HMac hmac;

    /**
     * Underlying stream being wrapped.
     */
    private OutputStream out;

    /**
     * Private constructor used for serialization.
     */
    private HmacOutputStream() {
    }

    /**
     * Creates a new instance using the specified HMAC instance and wrapping
     * the specified stream.
     *
     * @param hmac HMAC instance that has been initialized
     * @param chained stream being wrapped
     */
    public HmacOutputStream(final HMac hmac, final OutputStream chained) {
        Validate.notNull(hmac, "HMAC instance must not be null");
        Validate.notNull(chained, "OutputStream must not be null");

        this.hmac = hmac;
        this.out = chained;
    }

    public HMac getHmac() {
        return hmac;
    }

    @Override
    public void write(final int b) throws IOException {
        hmac.update((byte)b);
        out.write(b);
    }

    @Override
    public void write(final byte[] buffer) throws IOException {
        hmac.update(buffer, 0, buffer.length);
        out.write(buffer);
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length) throws IOException {
        hmac.update(buffer, offset, length);
        out.write(buffer, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}

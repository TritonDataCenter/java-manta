/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.util.NotThreadSafe;

import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

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
public class MultipartOutputStream extends OutputStream {
    /**
     * Backing stream.
     */
    private OutputStream wrapped = null;

    /**
     * Buffering stream.
     */
    private ByteArrayOutputStream buf;

    /**
     * Blocksize to segment to.
     */
    private int blockSize;

    /**
     * Private no-arg constructor for serialization.
     */
    private MultipartOutputStream() {
    }

    /**
     * Creates a new instance with the specified block size.
     *
     * @param blockSize cipher block size
     */
    public MultipartOutputStream(final int blockSize) {
        this.blockSize = blockSize;
        this.buf = new ByteArrayOutputStream(blockSize);
    }

    /**
     * Creates a new instance with the specified block size and output buffer.
     *
     * @param blockSize cipher block size
     * @param buf output buffer
     */
    public MultipartOutputStream(final int blockSize, final ByteArrayOutputStream buf) {
        this.blockSize = blockSize;
        this.buf = buf;
    }

    /**
     * Attaches the next stream.
     * @param next stream to switch to as a backing stream
     */
    public void setNext(final OutputStream next) {
        wrapped = next;
    }

    /**
     * Caller must check this before closing everything out.
     *
     * @return the bytes that didn't fit within the blocksize
     */
    public byte[] getRemainder() {
        return buf.toByteArray();
    }

    // DOES NOT CLOSE UNDERLYING STREAM
    // It might look reasonable to flush the buffer here.  But that
    // would complicate HMAC where we need to write the final "normal
    // bytes", flush the buffer, then write the HMAC, and finally
    // close the underlying buffers up.
    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException {
        wrapped.flush();
    }

    @Override
    public void write(final byte[] buffer) throws IOException {
        int outstanding = buf.size() + buffer.length;
        if (outstanding < blockSize) {
            buf.write(buffer);
        } else {
            int remainder = outstanding % blockSize;
            flushBuffer();
            wrapped.write(buffer, 0, buffer.length - remainder);
            buf.write(buffer, buffer.length - remainder, remainder);
        }
    }

    /**
     * Flushes the backing buffer.
     *
     * @throws IOException thrown when the flush operation failed
     */
    public void flushBuffer() throws IOException {
        wrapped.write(buf.toByteArray());
        buf.reset();
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length) throws IOException {
        write(Arrays.copyOfRange(buffer, offset, offset + length));
    }

    @Override
    public void write(final int value) throws IOException {
        wrapped.write(value);
    }

    public ByteArrayOutputStream getBuf() {
        return buf;
    }
}

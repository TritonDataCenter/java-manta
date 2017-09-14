/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import com.joyent.manta.util.MantaUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.io.DigestOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Class that wraps an {@link HttpEntity} instance and calculates a running
 * digest on the data written out.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class DigestedEntity implements HttpEntity {
    /**
     * Calculates a running MD5 as data is streamed out.
     */
    private final Digest digest;

    /**
     * Wrapped entity implementation in which the API is proxied through.
     */
    private final HttpEntity wrapped;

    /**
     * Creates a entity that wraps another entity and calculates a running
     * digest.
     *
     * @param wrapped entity to wrap
     * @param digest cryptographic digest implementation
     */
    public DigestedEntity(final HttpEntity wrapped,
                          final Digest digest) {
        this.wrapped = wrapped;
        this.digest = digest;
    }

    @Override
    public boolean isRepeatable() {
        return this.wrapped.isRepeatable();
    }

    @Override
    public boolean isChunked() {
        return this.wrapped.isChunked();
    }

    @Override
    public long getContentLength() {
        return this.wrapped.getContentLength();
    }

    @Override
    public Header getContentType() {
        return this.wrapped.getContentType();
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return this.wrapped.getContent();
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        digest.reset(); // reset the digest state in case we're in a retry

        // If our wrapped entity is backed by a buffer of some form
        // we can read easily read the whole buffer into our message digest.
        if (wrapped instanceof MemoryBackedEntity) {
            final MemoryBackedEntity entity = (MemoryBackedEntity)wrapped;
            final ByteBuffer backingBuffer = entity.getBackingBuffer();

            if (backingBuffer.hasArray()) {
                final byte[] bytes = backingBuffer.array();
                final int offset = backingBuffer.arrayOffset();
                final int position = backingBuffer.position();
                final int limit = backingBuffer.limit();

                digest.update(bytes, offset + position, limit - position);
                backingBuffer.position(limit);

                wrapped.writeTo(out);
            }
        } else {
            try (DigestOutputStream dout = new DigestOutputStream(digest);
                 TeeOutputStream teeOut = new TeeOutputStream(out, dout)) {
                wrapped.writeTo(teeOut);
                teeOut.flush();
            }
        }
    }

    @Override
    public boolean isStreaming() {
        return this.wrapped.isStreaming();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void consumeContent() throws IOException {
        this.wrapped.consumeContent();
    }

    /**
     * Digest hash of all data that has passed through the
     * {@link DigestedEntity#writeTo(OutputStream)} method's stream.
     *
     * @return a byte array containing the md5 value
     */
    public byte[] getDigest() {
        final byte[] res = new byte[digest.getDigestSize()];

        digest.doFinal(res, 0);

        return res;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("digest", MantaUtils.byteArrayAsHexString(getDigest()))
                .append("wrapped", wrapped)
                .toString();
    }
}

/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import com.joyent.manta.exception.MantaException;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.Header;
import org.apache.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
    private final MessageDigest messageDigest;

    /**
     * Wrapped entity implementation in which the API is proxied through.
     */
    private final HttpEntity wrapped;

    /**
     * Creates a entity that wraps another entity and calculates a running
     * digest.
     *
     * @param wrapped entity to wrap
     * @param algorithm lookup name of cryptographic digest
     */
    public DigestedEntity(final HttpEntity wrapped,
                   final String algorithm) {
        this.wrapped = wrapped;

        try {
            this.messageDigest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            String msg = String.format("No digest algorithm by the name of "
                    + "[%s] available in the JVM", algorithm);
            throw new MantaException(msg);
        }
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
        // If our wrapped entity is backed by a buffer of some form
        // we can read easily read the whole buffer into our message digest.
        if (wrapped instanceof MemoryBackedEntity) {
            MemoryBackedEntity entity = (MemoryBackedEntity)wrapped;
            messageDigest.update(entity.getBackingBuffer());

            wrapped.writeTo(out);
        } else {
            try (DigestOutputStream dout = new DigestOutputStream(out, messageDigest)) {
                wrapped.writeTo(dout);
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
        return this.messageDigest.digest();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageDigest", MantaUtils.byteArrayAsHexString(getDigest()))
                .append("wrapped", wrapped)
                .toString();
    }
}

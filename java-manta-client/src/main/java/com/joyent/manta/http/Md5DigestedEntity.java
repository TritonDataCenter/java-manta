/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.manta.util.MantaUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.Header;
import org.apache.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

/**
 * Class that wraps an {@link HttpEntity} instance and calculates a running
 * MD5 digests on the data written out.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
class Md5DigestedEntity implements HttpEntity {
    /**
     * Calculates a running MD5 as data is streamed out.
     */
    private final MessageDigest md5Digest = DigestUtils.getMd5Digest();

    /**
     * Wrapped entity implementation in which the API is proxied through.
     */
    private final HttpEntity wrapped;

    public Md5DigestedEntity(final HttpEntity wrapped) {
        this.wrapped = wrapped;
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
        try (DigestOutputStream dout = new DigestOutputStream(out, md5Digest)) {
            wrapped.writeTo(dout);
        }
    }

    @Override
    public boolean isStreaming() {
        return this.wrapped.isStreaming();
    }

    @SuppressWarnings("deprecated")
    @Override
    public void consumeContent() throws IOException {
        this.wrapped.consumeContent();
    }

    /**
     * MD5 hash of all data that has passed through the
     * {@link Md5DigestedEntity#writeTo(OutputStream)} method's stream.
     *
     * @return a byte array containing the md5 value
     */
    public byte[] getMd5() {
        return md5Digest.digest();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("md5Digest", MantaUtils.byteArrayAsHexString(getMd5()))
                .append("wrapped", wrapped)
                .toString();
    }
}

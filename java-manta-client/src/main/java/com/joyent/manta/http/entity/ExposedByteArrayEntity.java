/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import org.apache.commons.lang3.Validate;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Provides a byte array backed entity like {@link org.apache.http.entity.ByteArrayEntity}
 * that provides a method to access the underlying byte array as a buffer.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ExposedByteArrayEntity extends AbstractHttpEntity
        implements MemoryBackedEntity, Cloneable {
    /**
     * Backing binary data as a byte array.
     */
    private final byte[] buffer;

    /**
     * Offset to read from byte array.
     */
    private final int offset;

    /**
     * Total length of the data to read from byte array.
     */
    private final int length;

    /**
     * Creates a new instance based on the passed byte array.
     * @param buffer byte array to back this entity.
     * @param contentType content type to be used
     */
    public ExposedByteArrayEntity(final byte[] buffer, final ContentType contentType) {
        this(buffer, 0, buffer.length, contentType);
    }

    /**
     * Creates a new instance based on the passed byte array at the specified
     * offset and length.
     *
     * @param buffer byte array to back this entity.
     * @param offset offset to start reading the byte array from
     * @param length total number of bytes to read
     * @param contentType content type to be used
     */
    public ExposedByteArrayEntity(final byte[] buffer, final int offset,
                                  final int length, final ContentType contentType) {
        Validate.notNull(buffer, "Byte array must not be null");

        this.buffer = buffer;
        this.offset = offset;
        this.length = length;

        if (contentType != null) {
            setContentType(contentType.toString());
        }
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return this.length;
    }

    @Override
    public InputStream getContent() {
        return new ByteArrayInputStream(this.buffer, this.offset, this.length);
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        Validate.notNull(out, "OutputStream should not be null");

        out.write(this.buffer, this.offset, this.length);
        out.flush();
    }

    /**
     * Since this class is backed by a byte array, it is by definition not
     * streaming.
     *
     * @return {@code false}
     */
    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public ByteBuffer getBackingBuffer() {
        return ByteBuffer.wrap(buffer, offset, length);
    }
}

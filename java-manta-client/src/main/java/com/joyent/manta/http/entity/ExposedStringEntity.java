/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http.entity;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ExposedStringEntity extends StringEntity implements MemoryBackedEntity {
    /**
     * Creates a StringEntity with the specified content and content type.
     *
     * @param string      content to be used. Not {@code null}.
     * @param contentType content type to be used. May be {@code null}, in which case the default
     *                    MIME type {@link ContentType#TEXT_PLAIN} is assumed.
     * @throws IllegalArgumentException    if the string parameter is null
     * @throws UnsupportedCharsetException Thrown when the named charset is not available in
     *                                     this instance of the Java virtual machine
     */
    public ExposedStringEntity(final String string, final ContentType contentType) throws UnsupportedCharsetException {
        super(string, contentType);
    }

    /**
     * Creates a StringEntity with the specified content and charset. The MIME type defaults
     * to "text/plain".
     *
     * @param string  content to be used. Not {@code null}.
     * @param charset character set to be used. May be {@code null}, in which case the default
     *                is {@link org.apache.http.protocol.HTTP#DEF_CONTENT_CHARSET} is assumed
     * @throws IllegalArgumentException    if the string parameter is null
     * @throws UnsupportedCharsetException Thrown when the named charset is not available in
     *                                     this instance of the Java virtual machine
     */
    public ExposedStringEntity(final String string, final String charset) throws UnsupportedCharsetException {
        super(string, charset);
    }

    /**
     * Creates a StringEntity with the specified content and charset. The MIME type defaults
     * to "text/plain".
     *
     * @param string  content to be used. Not {@code null}.
     * @param charset character set to be used. May be {@code null}, in which case the default
     *                is {@link org.apache.http.protocol.HTTP#DEF_CONTENT_CHARSET} is assumed
     * @throws IllegalArgumentException if the string parameter is null
     */
    public ExposedStringEntity(final String string, final Charset charset) {
        super(string, charset);
    }

    /**
     * Creates a StringEntity with the specified content. The content type defaults to
     * {@link ContentType#TEXT_PLAIN}.
     *
     * @param string content to be used. Not {@code null}.
     * @throws IllegalArgumentException     if the string parameter is null
     * @throws UnsupportedEncodingException if the default HTTP charset is not supported.
     */
    public ExposedStringEntity(final String string) throws UnsupportedEncodingException {
        super(string);
    }

    @Override
    public ByteBuffer getBackingBuffer() {
        return ByteBuffer.wrap(super.content);
    }
}
